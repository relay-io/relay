use crate::errors::{Error, Result};
use crate::migrations::{run_migrations, Migration};
use crate::num::PositiveI32;
use chrono::{DateTime, TimeZone, Utc};
use deadpool_postgres::{
    ClientWrapper, GenericClient, Hook, HookError, Manager, ManagerConfig, Pool, PoolError,
    RecyclingMethod,
};
use metrics::{counter, increment_counter};
use pg_interval::Interval;
use rustls::client::{ServerCertVerified, ServerCertVerifier, WebPkiVerifier};
use rustls::{Certificate, OwnedTrustAnchor, RootCertStore, ServerName};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::io;
use std::io::ErrorKind;
use std::sync::Arc;
use std::time::SystemTime;
use std::{str::FromStr, time::Duration};
use tokio_postgres::error::SqlState;
use tokio_postgres::types::{BorrowToSql, Json, ToSql};
use tokio_postgres::{Config as PostgresConfig, Row};
use tokio_stream::{Stream, StreamExt};
use tracing::{debug, warn};
use uuid::Uuid;

const MIGRATIONS: [Migration; 1] = [Migration::new(
    "1697429987001_initialize.sql",
    include_str!("../migrations/1697429987001_initialize.sql"),
)];

/// This is a custom enqueue mode that determines the behaviour of the enqueue function.
pub enum EnqueueMode {
    /// This ensures the Job is unique by Job ID and will return an error id any Job already exists.
    Unique,
    /// This will silently do nothing if the Job that already exists.
    Ignore,
    /// This will replace the existing Job with the new Job changing the job to be immediately no longer in-flight.
    Replace,
}

// Is a structure used to enqueue a new Job.
#[derive(Deserialize)]
pub struct NewJob<'a> {
    /// The unique Job ID which is also CAN be used to ensure the Job is a singleton.
    pub id: &'a str,

    /// Is used to differentiate different job types that can be picked up by job runners.
    pub queue: &'a str,

    /// Denotes the duration, in seconds, after a Job has started processing or since the last
    /// heartbeat request occurred before considering the Job failed and being put back into the
    /// queue.
    pub timeout: PositiveI32,

    /// Determines how many times the Job can be retried, due to timeouts, before being considered
    /// permanently failed. Infinite retries are supported by using a negative number eg. -1
    pub max_retries: Option<PositiveI32>,

    /// The raw JSON payload that the job runner will receive.
    #[serde(borrow)]
    pub payload: &'a RawValue,

    /// The raw JSON payload that the job runner will receive.
    #[serde(borrow)]
    pub state: Option<&'a RawValue>,

    /// With this you can optionally schedule/set a Job to be run only at a specific time in the
    /// future. This option should mainly be used for one-time jobs and scheduled jobs that have
    /// the option of being self-perpetuated in combination with the reschedule endpoint.
    pub run_at: Option<DateTime<Utc>>,
}

/// Job defines all information about a Job.
#[derive(Serialize)]
pub struct Job {
    /// The unique Job ID which is also CAN be used to ensure the Job is a singleton.
    pub id: String,

    /// Is used to differentiate different job types that can be picked up by job runners.
    pub queue: String,

    /// Denotes the duration, in seconds, after a Job has started processing or since the last
    /// heartbeat request occurred before considering the Job failed and being put back into the
    /// queue.
    pub timeout: PositiveI32,

    /// Determines how many times the Job can be retried, due to timeouts, before being considered
    /// permanently failed. Infinite retries are supported by using a negative number eg. -1
    pub max_retries: Option<PositiveI32>,

    /// Specifies how many more times the Job can be retried.
    pub retries_remaining: Option<PositiveI32>,

    /// The raw payload that the `Job` requires to run.
    pub payload: Box<RawValue>,

    /// The raw `Job` state stored during enqueue, reschedule or heartbeat while in-flight..
    pub state: Option<Box<RawValue>>,

    /// Is the current Jobs unique `run_id`. When there is a value here it signifies that the job is
    /// currently in-flight being processed.
    pub run_id: Option<Uuid>,

    /// Indicates the time that a `Job` is eligible to be run.
    pub run_at: DateTime<Utc>,

    /// This indicates the last time the `Job` was updated either through enqueue, reschedule or
    /// heartbeat.
    pub updated_at: DateTime<Utc>,

    /// This indicates the time the `Job` was originally created. this value does now change when a
    /// Job is rescheduled.
    pub created_at: DateTime<Utc>,
}

/// Postgres backing store
pub struct PgStore {
    pool: Pool,
}

impl PgStore {
    /// Creates a new backing store with default settings for Postgres.
    ///
    /// # Errors
    ///
    /// Will return `Err` if connecting the server or running migrations fails.
    #[inline]
    pub async fn default(uri: &str) -> std::result::Result<Self, anyhow::Error> {
        Self::new(uri, 10).await
    }

    /// Creates a new backing store with advanced options.
    ///
    /// # Errors
    ///
    /// Will return `Err` if connecting the server or running migrations fails.
    #[inline]
    pub async fn new(
        uri: &str,
        max_connections: usize,
    ) -> std::result::Result<Self, anyhow::Error> {
        // Attempt to parse out the ssl mode before config parsing
        // library currently does not support verify-ca nor verify-full and will error.
        let mut uri = uri.to_string();
        let mut accept_invalid_certs = true;
        let mut accept_invalid_hostnames = true;

        if uri.contains("sslmode=verify-ca") {
            accept_invalid_certs = false;
            // so the Config doesn't fail to parse.
            uri = uri.replace("sslmode=verify-ca", "sslmode=required");
        } else if uri.contains("sslmode=verify-full") {
            accept_invalid_certs = false;
            accept_invalid_hostnames = false;
            // so the Config doesn't fail to parse.
            uri = uri.replace("sslmode=verify-full", "sslmode=required");
        }

        let mut pg_config = PostgresConfig::from_str(&uri)?;
        if pg_config.get_connect_timeout().is_none() {
            pg_config.connect_timeout(Duration::from_secs(5));
        }
        if pg_config.get_application_name().is_none() {
            pg_config.application_name("relay");
        }

        let tls_config_defaults = rustls::ClientConfig::builder().with_safe_defaults();

        let tls_config = if accept_invalid_certs {
            tls_config_defaults
                .with_custom_certificate_verifier(Arc::new(AcceptAllTlsVerifier))
                .with_no_client_auth()
        } else {
            let mut cert_store = RootCertStore::empty();
            cert_store.add_trust_anchors(webpki_roots::TLS_SERVER_ROOTS.0.iter().map(|ta| {
                OwnedTrustAnchor::from_subject_spki_name_constraints(
                    ta.subject,
                    ta.spki,
                    ta.name_constraints,
                )
            }));

            if accept_invalid_hostnames {
                let verifier = WebPkiVerifier::new(cert_store, None);
                tls_config_defaults
                    .with_custom_certificate_verifier(Arc::new(NoHostnameTlsVerifier { verifier }))
                    .with_no_client_auth()
            } else {
                tls_config_defaults
                    .with_root_certificates(cert_store)
                    .with_no_client_auth()
            }
        };

        let tls = tokio_postgres_rustls::MakeRustlsConnect::new(tls_config);

        let mgr = Manager::from_config(
            pg_config,
            tls,
            ManagerConfig {
                recycling_method: RecyclingMethod::Fast,
            },
        );
        let pool = Pool::builder(mgr)
            .max_size(max_connections)
            .post_create(Hook::async_fn(|client: &mut ClientWrapper, _| {
                Box::pin(async move {
                    client
                        .simple_query("SET default_transaction_isolation TO 'read committed'")
                        .await
                        .map_err(|e| HookError::Backend(e))?;
                    Ok(())
                })
            }))
            .build()?;

        Self::new_with_pool(pool).await
    }

    /// Creates a new backing store with preconfigured pool
    ///
    /// # Errors
    ///
    /// Will return `Err` if connecting the server or running migrations fails.
    #[inline]
    pub async fn new_with_pool(pool: Pool) -> std::result::Result<Self, anyhow::Error> {
        {
            let mut client = pool.get().await?;
            run_migrations("_relay_rs_migrations", &mut client, &MIGRATIONS).await?;
        }
        Ok(Self { pool })
    }

    /// Creates a batch of Jobs to be processed in a single write transaction.
    ///
    /// NOTES: If the number of jobs passed is '1' then those will return a `JobExists` error
    ///        identifying the job as already existing.
    ///        If there are more than one jobs this function will not return an error for conflicts
    ///        in Job ID, but rather silently drop the record using an `ON CONFLICT DO NOTHING`.
    ///        If you need to have a Conflict error returned pass a single Job instead.
    ///
    /// # Errors
    ///
    /// Will return `Err` if there is any communication issues with the backend Postgres DB.
    #[tracing::instrument(name = "pg_enqueue", level = "debug", skip_all, fields(jobs = jobs.len()))]
    async fn enqueue<'a>(&self, mode: EnqueueMode, jobs: &[NewJob<'a>]) -> Result<()> {
        let mut client = self.pool.get().await?;
        let transaction = client.transaction().await?;
        let stmt = match mode {
            EnqueueMode::Unique => {
                transaction
                    .prepare_cached(
                        r#"INSERT INTO jobs (
                          id,
                          queue,
                          timeout,
                          max_retries,
                          retries_remaining,
                          data,
                          state,
                          run_at,
                          updated_at,
                          created_at
                        )
                        VALUES ($1, $2, $3, $4, $4, $5, $6, $7, $8, $8)"#,
                    )
                    .await?
            }
            EnqueueMode::Ignore => {
                transaction
                    .prepare_cached(
                        r#"INSERT INTO jobs (
                          id,
                          queue,
                          timeout,
                          max_retries,
                          retries_remaining,
                          data,
                          state,
                          run_at,
                          updated_at,
                          created_at
                        )
                        VALUES ($1, $2, $3, $4, $4, $5, $6, $7, $8, $8)
                        ON CONFLICT DO NOTHING"#,
                    )
                    .await?
            }
            EnqueueMode::Replace => {
                transaction
                    .prepare_cached(
                        r#"INSERT INTO jobs (
                          id,
                          queue,
                          timeout,
                          max_retries,
                          retries_remaining,
                          data,
                          state,
                          run_at,
                          updated_at,
                          created_at
                        )
                        VALUES ($1, $2, $3, $4, $4, $5, $6, $7, $8, $8)
                        ON CONFLICT UPDATE SET
                        timeout = EXCLUDED.timeout,
                        max_retries = EXCLUDED.max_retries,
                        retries_remaining = EXCLUDED.max_retries,
                        data = EXCLUDED.data,
                        state = EXCLUDED.state,
                        in_flight = false,
                        run_id = NULL,
                        run_at = EXCLUDED.run_at,
                        updated_at = EXCLUDED.updated_at"#,
                    )
                    .await?
            }
        };
        let mut counts = HashMap::new();

        for job in jobs {
            let now = Utc::now().naive_utc();
            let run_at = if let Some(run_at) = job.run_at {
                run_at.naive_utc()
            } else {
                now
            };

            transaction
                .execute(
                    &stmt,
                    &[
                        &job.id,
                        &job.queue,
                        &Interval::from_duration(chrono::Duration::seconds(i64::from(
                            job.timeout.get(),
                        ))),
                        &job.max_retries.as_ref().map(|r| Some(r.get())),
                        &Json(&job.payload),
                        &job.state.map(|state| Some(Json(state))),
                        &run_at,
                        &now,
                    ],
                )
                .await
                .map_err(|e| {
                    if let Some(&SqlState::UNIQUE_VIOLATION) = e.code() {
                        Error::JobExists {
                            job_id: job.id.to_string(),
                            queue: job.queue.to_string(),
                        }
                    } else {
                        e.into()
                    }
                })?;

            match counts.entry(job.queue) {
                Entry::Occupied(mut o) => *o.get_mut() += 1,
                Entry::Vacant(v) => {
                    v.insert(1);
                }
            };
        }

        transaction.commit().await?;

        for (queue, count) in counts {
            counter!("enqueued", count, "queue" => queue.to_string());
        }
        debug!("enqueued jobs");
        Ok(())
    }

    // async fn get(&self, queue: &str, job_id: &str) -> Result<Option<Job>> {
    //     let mut client = self.pool.get().await?;
    //     let stmt = client
    //         .prepare_cached(
    //             r#"
    //            SELECT id,
    //                   queue,
    //                   timeout,
    //                   max_retries,
    //                   retries_remaining,
    //                   data,
    //                   state,
    //                   run_id,
    //                   run_at,
    //                   updated_at,
    //                   created_at
    //            FROM jobs
    //            WHERE
    //                 queue=$1 AND
    //                 id=$2
    //         "#,
    //         )
    //         .await?;
    //     let row = client.query_opt(&stmt, &[&queue, &job_id]).await?;
    //
    //     let job = row.as_ref().map(row_to_job);
    //
    //     increment_counter!("get", "queue" => queue.to_owned());
    //     debug!("got job");
    //     Ok(job)
    // }
}

// impl From<&Row> for Job {
//     fn from(row: &Row) -> Self {
//         Job {
//             id: row.get(0),
//             queue: row.get(1),
//             timeout: unsafe {
//                 NonZeroU32::new_unchecked(interval_seconds(row.get::<usize, Interval>(2)) as u32)
//             },
//             max_retries: row
//                 .get::<usize, Option<i32>>(3)
//                 .map(|r| Some(unsafe { NonZeroU32::new_unchecked(r as u32) })),
//             retries_remaining: 0,
//             payload: Box::new(()),
//             state: None,
//             run_id: None,
//             run_at: Default::default(),
//             updated_at: Default::default(),
//             created_at: Default::default(),
//         }
//     }
// }

// #[inline]
// fn row_to_job(row: &Row) -> NewJob {
//     NewJob {
//         id: row.get(0),
//         queue: row.get(1),
//         timeout: interval_seconds(row.get::<usize, Interval>(2)),
//         max_retries: row.get(3),
//         payload: row.get::<usize, Json<&RawValue>>(4).0,
//         state: row
//             .get::<usize, Option<Json<&RawValue>>>(5)
//             .map(|state| match state {
//                 Json(state) => state,
//             }),
//         run_at: Some(Utc.from_utc_datetime(&row.get(6))),
//     }
// }

fn interval_seconds(interval: Interval) -> i32 {
    let month_secs = interval.months * 30 * 24 * 60 * 60;
    let day_secs = interval.days * 24 * 60 * 60;
    let micro_secs = (interval.microseconds / 1_000_000) as i32;

    month_secs + day_secs + micro_secs
}

impl From<tokio_postgres::Error> for Error {
    fn from(e: tokio_postgres::Error) -> Self {
        Error::Backend {
            message: e.to_string(),
            is_retryable: is_retryable(e),
        }
    }
}

impl From<PoolError> for Error {
    fn from(e: PoolError) -> Self {
        Error::Backend {
            message: e.to_string(),
            is_retryable: match e {
                PoolError::Timeout(_) => true,
                PoolError::Backend(e) => is_retryable(e),
                PoolError::PostCreateHook(e) => match e {
                    HookError::Backend(e) => is_retryable(e),
                    _ => false,
                },
                PoolError::Closed | PoolError::NoRuntimeSpecified => false,
            },
        }
    }
}

#[inline]
fn is_retryable(e: tokio_postgres::Error) -> bool {
    match e.code() {
        Some(
            &(SqlState::IO_ERROR
            | SqlState::TOO_MANY_CONNECTIONS
            | SqlState::LOCK_NOT_AVAILABLE
            | SqlState::QUERY_CANCELED
            | SqlState::SYSTEM_ERROR),
        ) => true,
        Some(_) => false,
        None => {
            if let Some(e) = e
                .into_source()
                .as_ref()
                .and_then(|e| e.downcast_ref::<io::Error>())
            {
                matches!(
                    e.kind(),
                    ErrorKind::ConnectionReset
                        | ErrorKind::ConnectionAborted
                        | ErrorKind::NotConnected
                        | ErrorKind::WouldBlock
                        | ErrorKind::TimedOut
                        | ErrorKind::WriteZero
                        | ErrorKind::Interrupted
                        | ErrorKind::UnexpectedEof
                )
            } else {
                false
            }
        }
    }
}

struct AcceptAllTlsVerifier;

impl ServerCertVerifier for AcceptAllTlsVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &Certificate,
        _intermediates: &[Certificate],
        _server_name: &ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp_response: &[u8],
        _now: SystemTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }
}

pub struct NoHostnameTlsVerifier {
    verifier: WebPkiVerifier,
}

impl ServerCertVerifier for NoHostnameTlsVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &Certificate,
        intermediates: &[Certificate],
        server_name: &ServerName,
        scts: &mut dyn Iterator<Item = &[u8]>,
        ocsp_response: &[u8],
        now: SystemTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        match self.verifier.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            scts,
            ocsp_response,
            now,
        ) {
            Err(rustls::Error::InvalidCertificate(cert_error))
                if cert_error == rustls::CertificateError::NotValidForName =>
            {
                Ok(ServerCertVerified::assertion())
            }
            res => res,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_enqueue_conflict() -> anyhow::Result<()> {
        let db_url = std::env::var("DATABASE_URL")?;
        let store = PgStore::default(&db_url).await?;
        let job_id = Uuid::new_v4().to_string();
        let queue = Uuid::new_v4().to_string();
        let job1 = NewJob {
            id: &job_id,
            queue: &queue,
            timeout: PositiveI32::new(30).unwrap(),
            max_retries: Some(PositiveI32::new(3).unwrap()),
            payload: &RawValue::from_string("{}".to_string())?,
            state: None,
            run_at: None,
        };
        let job2 = NewJob {
            id: &job_id,
            queue: &queue,
            timeout: PositiveI32::new(30).unwrap(),
            max_retries: Some(PositiveI32::new(3).unwrap()),
            payload: &RawValue::from_string("{}".to_string())?,
            state: None,
            run_at: None,
        };
        store.enqueue(EnqueueMode::Unique, &[job1]).await?;

        let result = store.enqueue(EnqueueMode::Unique, &[job2]).await;
        assert_eq!(Err(Error::JobExists { job_id, queue }), result);
        Ok(())
    }
}
