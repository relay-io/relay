use crate::errors::{Error, Result};
use crate::migrations::{run_migrations, Migration};
use chrono::{DateTime, TimeZone, Utc};
use deadpool_postgres::{
    ClientWrapper, GenericClient, Hook, HookError, Manager, ManagerConfig, Pool, PoolError,
    RecyclingMethod, Transaction,
};
use metrics::{counter, histogram, increment_counter};
use pg_interval::Interval;
use relay_core::job::EnqueueMode;
use relay_core::num::{GtZeroI64, PositiveI32};
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
use tokio_postgres::{Config as PostgresConfig, Row, Statement};
use tokio_stream::{Stream, StreamExt};
use tracing::{debug, warn};
use uuid::Uuid;

const MIGRATIONS: [Migration; 1] = [Migration::new(
    "1697429987001_initialize.sql",
    include_str!("../migrations/1697429987001_initialize.sql"),
)];

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
                        .map_err(HookError::Backend)?;
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

    // TODO: Update doc comments for existing before continuing.

    /// Creates a batch of Jobs to be processed in a single write transaction following the rules
    /// indicated by provided `EnqueueMode`.
    ///
    /// # Errors
    ///
    /// Will return `Err` if there is any communication issues with the backend Postgres DB.
    #[tracing::instrument(name = "pg_enqueue", level = "debug", skip_all, fields(mode, jobs = jobs.len()))]
    async fn enqueue<'a>(&self, mode: EnqueueMode, jobs: &[NewJob<'a>]) -> Result<()> {
        let mut client = self.pool.get().await?;
        let transaction = client.transaction().await?;
        let stmt = enqueue_stmt(mode, &transaction).await?;
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

    /// Returns, if available, the Job and associated metadata from the database using the provided
    /// queue and id.
    ///
    /// # Errors
    ///
    /// Will return `Err` if there is any communication issues with the backend Postgres DB.
    async fn get(&self, queue: &str, job_id: &str) -> Result<Option<Job>> {
        let client = self.pool.get().await?;
        let stmt = client
            .prepare_cached(
                r#"
               SELECT id,
                      queue,
                      timeout,
                      max_retries,
                      retries_remaining,
                      data,
                      state,
                      run_id,
                      run_at,
                      updated_at,
                      created_at
               FROM jobs
               WHERE
                    queue=$1 AND
                    id=$2
            "#,
            )
            .await?;

        let row = client.query_opt(&stmt, &[&queue, &job_id]).await?;
        let job = row.as_ref().map(|r| r.into());

        increment_counter!("get", "queue" => queue.to_owned());
        debug!("got job");
        Ok(job)
    }

    /// Checks and returns if a Job exists in the database with the provided queue and id.
    ///
    /// # Errors
    ///
    /// Will return `Err` if there is any communication issues with the backend Postgres DB.
    #[tracing::instrument(name = "pg_exists", level = "debug", skip_all, fields(job_id=%job_id, queue=%queue))]
    async fn exists(&self, queue: &str, job_id: &str) -> Result<bool> {
        let client = self.pool.get().await?;
        let stmt = client
            .prepare_cached(
                r#"
                    SELECT EXISTS (
                        SELECT 1 FROM jobs WHERE
                            queue=$1 AND
                            id=$2
                    )
                "#,
            )
            .await?;

        let exists: bool = client.query_one(&stmt, &[&queue, &job_id]).await?.get(0);

        increment_counter!("exists", "queue" => queue.to_owned());
        debug!("exists check job");
        Ok(exists)
    }

    /// Fetches the next available Job(s) to be executed order by `run_at`.
    ///
    /// # Errors
    ///
    /// Will return `Err` if there is any communication issues with the backend Postgres DB.
    #[tracing::instrument(name = "pg_next", level = "debug", skip_all, fields(num_jobs=num_jobs.get(), queue=%queue))]
    async fn next(&self, queue: &str, num_jobs: GtZeroI64) -> Result<Option<Vec<Job>>> {
        let client = self.pool.get().await?;

        // MUST USE CTE WITH `FOR UPDATE SKIP LOCKED LIMIT` otherwise the Postgres Query Planner
        // CAN optimize the query which will cause MORE updates than the LIMIT specifies within
        // a nested loop.
        // See here for details:
        // https://github.com/feikesteenbergen/demos/blob/19522f66ffb6eb358fe2d532d9bdeae38d4e2a0b/bugs/update_from_correlated.adoc
        let stmt = client
            .prepare_cached(
                r#"
               WITH subquery AS (
                   SELECT
                        id,
                        queue
                   FROM jobs
                   WHERE
                        queue=$1 AND
                        in_flight=false AND
                        run_at <= NOW()
                   ORDER BY run_at ASC
                   FOR UPDATE SKIP LOCKED
                   LIMIT $2
               )
               UPDATE jobs j
               SET in_flight=true,
                   run_id=uuid_generate_v4(),
                   updated_at=NOW(),
                   expires_at=NOW()+timeout
               FROM subquery
               WHERE
                   j.queue=subquery.queue AND
                   j.id=subquery.id
               RETURNING j.id,
                         j.queue,
                         j.timeout,
                         j.max_retries,
                         j.retries_remaining,
                         j.data,
                         j.state,
                         j.run_id,
                         j.run_at,
                         j.updated_at,
                         j.created_at
            "#,
            )
            .await?;

        let limit = num_jobs.get();
        let params: Vec<&(dyn ToSql + Sync)> = vec![&queue, &limit];
        let stream = client.query_raw(&stmt, params).await?;
        tokio::pin!(stream);

        // on purpose NOT using num_jobs as the capacity to avoid the potential attack vector of
        // someone exhausting all memory by sending a large number even if there aren't that many
        // records in the database.
        let mut jobs: Vec<Job> = if let Some(size) = stream.size_hint().1 {
            Vec::with_capacity(size)
        } else {
            Vec::new()
        };

        while let Some(row) = stream.next().await {
            jobs.push((&row?).into());
        }

        if jobs.is_empty() {
            debug!("fetched no jobs");
            Ok(None)
        } else {
            for job in &jobs {
                // using updated_at because this handles:
                // - enqueue -> processing
                // - reschedule -> processing
                // - reaped -> processing
                // This is a possible indicator not enough consumers/processors on the calling side
                // and jobs are backed up for processing.
                if let Ok(d) = (Utc::now() - job.updated_at).to_std() {
                    histogram!("latency", d, "queue" => job.queue.clone(), "type" => "to_processing");
                }
            }
            counter!("fetched", jobs.len() as u64, "queue" => queue.to_owned());
            debug!(fetched_jobs = jobs.len(), "fetched next job(s)");
            Ok(Some(jobs))
        }
    }

    /// Deletes the job from the database given the `queue` and `id`.
    ///
    /// If the `Job` already does not exist, this will complete without error.
    ///
    /// # Errors
    ///
    /// Will return `Err` if there is any communication issues with the backend Postgres DB.
    #[tracing::instrument(name = "pg_delete", level = "debug", skip_all, fields(job_id=%job_id, queue=%queue))]
    async fn delete(&self, queue: &str, job_id: &str) -> Result<()> {
        let client = self.pool.get().await?;
        let stmt = client
            .prepare_cached(
                r#"
                DELETE FROM jobs
                WHERE
                    queue=$1 AND
                    id=$2
                RETURNING run_at
            "#,
            )
            .await?;
        let row = client.query_opt(&stmt, &[&queue, &job_id]).await?;

        if let Some(row) = row {
            let run_at = Utc.from_utc_datetime(&row.get(0));

            increment_counter!("deleted", "queue" => queue.to_owned());

            if let Ok(d) = (Utc::now() - run_at).to_std() {
                histogram!("duration", d, "queue" => queue.to_owned(), "type" => "deleted");
            }
            debug!("deleted job");
        }
        Ok(())
    }

    /// Completes a `Job` by deleting it from the database given the `queue` and `id` and current
    /// `run_id`. This different from deletion in that it is used to indicate a `Job` has been
    /// completed and not just removed.
    ///
    /// If the `Job` already does not exist, this will complete without error.
    ///
    /// # Errors
    ///
    /// Will return `Err` if there is any communication issues with the backend Postgres DB.
    #[tracing::instrument(name = "pg_complete", level = "debug", skip_all, fields(job_id=%job_id, queue=%queue))]
    async fn complete(&self, queue: &str, job_id: &str, run_id: &Uuid) -> Result<()> {
        let client = self.pool.get().await?;
        let stmt = client
            .prepare_cached(
                r#"
                DELETE FROM jobs
                WHERE
                    queue=$1 AND
                    id=$2 AND
                    run_id=$3
                RETURNING run_at
            "#,
            )
            .await?;
        let row = client.query_opt(&stmt, &[&queue, &job_id, &run_id]).await?;

        if let Some(row) = row {
            let run_at = Utc.from_utc_datetime(&row.get(0));

            increment_counter!("completed", "queue" => queue.to_owned());

            if let Ok(d) = (Utc::now() - run_at).to_std() {
                histogram!("duration", d, "queue" => queue.to_owned(), "type" => "completed");
            }
            debug!("completed job");
        }
        Ok(())
    }

    /// Reschedules the an existing in-flight Job to be run again with the provided new information.
    ///
    /// The Jobs queue, id and run_id must match an existing in-flight Job. This is primarily used
    /// to schedule a new/the next run of a singleton `Job`. This provides the ability for
    /// self-perpetuating scheduled jobs in an atomic manner.
    ///
    /// Reschedule also allows you to change the `Job`'s `queue` and `id` during the reschedule.
    /// This is allowed to facilitate advancing a `Job` through a distributed pipeline/state
    /// machine atomically if that is more appropriate than advancing using the `Job`'s state alone.
    ///
    /// If rescheduling and changing the `Job`'s `queue` and `id` the mode will be used to
    /// determine the behaviour if a conflicting record already exists, just like when enqueuing
    /// jobs. The only difference is if `Ignore` is used and no update happens then the current
    /// `Job` will be completed automatically.
    ///
    /// If the `Job` no longer exists or is not in-flight, this will return without error.
    ///
    /// # Errors
    ///
    /// Will return `Err` if there is any communication issues with the backend Postgres DB.
    #[tracing::instrument(name = "pg_reschedule", level = "debug", skip_all, fields(job_id=%job.id, queue=%job.queue))]
    async fn reschedule<'a>(
        &self,
        mode: EnqueueMode,
        queue: &str,
        job_id: &str,
        run_id: &Uuid,
        job: &NewJob<'a>,
    ) -> Result<()> {
        let now = Utc::now().naive_utc();
        let run_at = if let Some(run_at) = job.run_at {
            run_at.naive_utc()
        } else {
            now
        };

        let mut client = self.pool.get().await?;

        let previous_run_at: Option<DateTime<Utc>> = if queue == job.queue && job_id == job.id {
            let stmt = client
                .prepare_cached(
                    r#"
                UPDATE jobs
                SET
                    timeout = $3,
                    max_retries = $4,
                    retries_remaining = $4,
                    data = $5,
                    state = $6,
                    updated_at = $7,
                    run_at = $8,
                    in_flight = false,
                    run_id = NULL
                WHERE
                    queue=$1 AND
                    id=$2 AND
                    in_flight=true AND
                    run_id=$9
                RETURNING (SELECT run_at FROM jobs WHERE queue=$1 AND id=$2 AND in_flight=true)
                "#,
                )
                .await?;
            client
                .query_opt(
                    &stmt,
                    &[
                        &job.queue,
                        &job.id,
                        &Interval::from_duration(chrono::Duration::seconds(i64::from(
                            job.timeout.get(),
                        ))),
                        &job.max_retries.as_ref().map(|r| Some(r.get())),
                        &Json(&job.payload),
                        &job.state.map(|state| Some(Json(state))),
                        &now,
                        &run_at,
                        &run_id,
                    ],
                )
                .await?
                .map(|row| Utc.from_utc_datetime(&row.get(0)))
        } else {
            let transaction = client.transaction().await?;
            let delete_stmt = transaction
                .prepare_cached(
                    r#"
                DELETE FROM jobs
                WHERE
                    queue=$1 AND
                    id=$2 AND
                    in_flight=true AND
                    run_id=$3
                RETURNING run_at
            "#,
                )
                .await?;

            let previous_run_at = transaction
                .query_opt(&delete_stmt, &[&queue, &job_id, &run_id])
                .await?
                .map(|row| Utc.from_utc_datetime(&row.get(0)));
            if previous_run_at.is_some() {
                let stmt = enqueue_stmt(mode, &transaction).await?;

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
            }
            transaction.commit().await?;
            previous_run_at
        };

        if let Some(run_at) = previous_run_at {
            increment_counter!("rescheduled", "queue" => job.queue.to_string());

            if let Ok(d) = (Utc::now() - run_at).to_std() {
                histogram!("duration", d, "queue" => job.queue.to_string(), "type" => "rescheduled");
            }
            debug!("rescheduled job");
        }
        Ok(())
    }

    // /// Updates the existing in-flight job by incrementing it's `updated_at` and optionally
    // /// setting state at the same time.
    // ///
    // /// # Errors
    // ///
    // /// Will return `Err` if there is any communication issues with the backend Postgres DB or the
    // /// Job attempting to be updated cannot be found.
    // #[tracing::instrument(name = "pg_heartbeat", level = "debug", skip_all, fields(job_id=%job_id, queue=%queue))]
    // async fn heartbeat(&self, queue: &str, job_id: &str, state: Option<Vec<u8>>) -> Result<()> {
    //     let client = self.pool.get().await?;
    //
    //     let stmt = client
    //         .prepare_cached(
    //             r#"
    //            UPDATE jobs
    //            SET state=$3,
    //                updated_at=NOW(),
    //                expires_at=NOW()+timeout
    //            WHERE
    //                queue=$1 AND
    //                id=$2 AND
    //                in_flight=true
    //            RETURNING (SELECT run_at FROM jobs WHERE queue=$1 AND id=$2 AND in_flight=true)
    //         "#,
    //         )
    //         .await
    //         .map_err(|e| Error::Backend {
    //             message: e.to_string(),
    //             is_retryable: is_retryable(e),
    //         })?;
    //
    //     let row = client
    //         .query_opt(
    //             &stmt,
    //             &[&queue, &job_id, &state.map(|state| Some(Json(state)))],
    //         )
    //         .await
    //         .map_err(|e| Error::Backend {
    //             message: e.to_string(),
    //             is_retryable: is_retryable(e),
    //         })?;
    //
    //     if let Some(row) = row {
    //         let run_at = Utc.from_utc_datetime(&row.get(0));
    //
    //         increment_counter!("heartbeat", "queue" => queue.to_owned());
    //
    //         if let Ok(d) = (Utc::now() - run_at).to_std() {
    //             histogram!("duration", d, "queue" => queue.to_owned(), "type" => "running");
    //         }
    //         debug!("heartbeat job");
    //         Ok(())
    //     } else {
    //         debug!("job not found");
    //         Err(Error::JobNotFound {
    //             job_id: job_id.to_string(),
    //             queue: queue.to_string(),
    //         })
    //     }
    // }
}

#[inline]
async fn enqueue_stmt<'a>(mode: EnqueueMode, transaction: &Transaction<'a>) -> Result<Statement> {
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
                        ON CONFLICT (queue, id) DO UPDATE SET
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
    Ok(stmt)
}

impl From<&Row> for Job {
    fn from(row: &Row) -> Self {
        Job {
            id: row.get(0),
            queue: row.get(1),
            timeout: PositiveI32::new(interval_seconds(row.get::<usize, Interval>(2)))
                .unwrap_or_else(|| {
                    warn!("invalid timeout value, defaulting to 30s");
                    PositiveI32::new(30).unwrap()
                }),
            max_retries: row.get::<usize, Option<i32>>(3).map(|i| {
                PositiveI32::new(i).unwrap_or_else(|| {
                    warn!("invalid max_retries value, defaulting to 0");
                    PositiveI32::new(0).unwrap()
                })
            }),
            retries_remaining: row.get::<usize, Option<i32>>(4).map(|i| {
                PositiveI32::new(i).unwrap_or_else(|| {
                    warn!("invalid max_retries value, defaulting to 0");
                    PositiveI32::new(0).unwrap()
                })
            }),
            payload: row.get::<usize, Json<Box<RawValue>>>(5).0,
            state: row
                .get::<usize, Option<Json<Box<RawValue>>>>(6)
                .map(|state| match state {
                    Json(state) => state,
                }),
            run_id: row.get(7),
            run_at: Utc.from_utc_datetime(&row.get(8)),
            updated_at: Utc.from_utc_datetime(&row.get(9)),
            created_at: Utc.from_utc_datetime(&row.get(10)),
        }
    }
}

const fn interval_seconds(interval: Interval) -> i32 {
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
    use chrono::DurationRound;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_reschedule_replace_pk_change() -> anyhow::Result<()> {
        let db_url = std::env::var("DATABASE_URL")?;
        let store = PgStore::default(&db_url).await?;
        let job_id1 = Uuid::new_v4().to_string();
        let job_id2 = Uuid::new_v4().to_string();
        let queue1 = Uuid::new_v4().to_string();
        let queue2 = Uuid::new_v4().to_string();
        let payload1 = &RawValue::from_string("{}".to_string())?;
        let payload2 = &RawValue::from_string(r#"{"key": "value"}"#.to_string())?;
        let job1 = NewJob {
            id: &job_id1,
            queue: &queue1,
            timeout: PositiveI32::new(30).unwrap(),
            max_retries: Some(PositiveI32::new(3).unwrap()),
            payload: payload1,
            state: None,
            run_at: None,
        };
        let job2 = NewJob {
            id: &job_id2,
            queue: &queue2,
            timeout: PositiveI32::new(30).unwrap(),
            max_retries: Some(PositiveI32::new(3).unwrap()),
            payload: payload2,
            state: None,
            run_at: None,
        };
        let reschedule = NewJob {
            id: &job_id1,
            queue: &queue1,
            timeout: PositiveI32::new(30).unwrap(),
            max_retries: Some(PositiveI32::new(3).unwrap()),
            payload: payload1,
            state: None,
            run_at: None,
        };

        store.enqueue(EnqueueMode::Unique, &[job1]).await?;
        assert!(store.exists(&queue1, &job_id1).await?);

        store.enqueue(EnqueueMode::Unique, &[job2]).await?;
        assert!(store.exists(&queue2, &job_id2).await?);

        let next = store.next(&queue2, GtZeroI64::new(1).unwrap()).await?;
        assert!(next.is_some());
        assert_eq!(1, *&next.as_ref().unwrap().len());
        let next = &next.unwrap()[0];

        let now = Utc::now().duration_trunc(chrono::Duration::milliseconds(100))?;
        let result = store
            .reschedule(
                EnqueueMode::Unique,
                &next.queue,
                &next.id,
                &next.run_id.unwrap(),
                &reschedule,
            )
            .await;
        assert!(matches!(
            result.unwrap_err(),
            Error::JobExists {
                queue: queue1,
                job_id: job_id1,
            }
        ));
        assert!(store.exists(&reschedule.queue, &reschedule.id).await?);

        // test job was deleted even though existing job existed
        store
            .reschedule(
                EnqueueMode::Replace,
                &queue2,
                &job_id2,
                &next.run_id.unwrap(),
                &reschedule,
            )
            .await?;
        assert!(!store.exists(&queue2, &job_id2).await?);
        assert!(store.exists(&queue1, &job_id1).await?);

        let rescheduled_job = store.get(&queue1, &job_id1).await?;
        assert!(rescheduled_job.is_some());
        let rescheduled_job = rescheduled_job.unwrap();
        assert_eq!(reschedule.id, rescheduled_job.id);
        assert_eq!(reschedule.queue, rescheduled_job.queue);
        assert_eq!(reschedule.timeout, rescheduled_job.timeout);
        assert_eq!(reschedule.max_retries, rescheduled_job.max_retries);
        assert_eq!(
            reschedule.payload.to_string(),
            rescheduled_job.payload.to_string()
        );
        assert!(rescheduled_job.state.is_none());
        assert!(rescheduled_job.run_id.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_reschedule_ignore_pk_change() -> anyhow::Result<()> {
        let db_url = std::env::var("DATABASE_URL")?;
        let store = PgStore::default(&db_url).await?;
        let job_id1 = Uuid::new_v4().to_string();
        let job_id2 = Uuid::new_v4().to_string();
        let queue1 = Uuid::new_v4().to_string();
        let queue2 = Uuid::new_v4().to_string();
        let payload1 = &RawValue::from_string("{}".to_string())?;
        let payload2 = &RawValue::from_string(r#"{"key": "value"}"#.to_string())?;
        let job1 = NewJob {
            id: &job_id1,
            queue: &queue1,
            timeout: PositiveI32::new(30).unwrap(),
            max_retries: Some(PositiveI32::new(3).unwrap()),
            payload: payload1,
            state: None,
            run_at: None,
        };
        let job2 = NewJob {
            id: &job_id2,
            queue: &queue2,
            timeout: PositiveI32::new(30).unwrap(),
            max_retries: Some(PositiveI32::new(3).unwrap()),
            payload: payload2,
            state: None,
            run_at: None,
        };
        let reschedule = NewJob {
            id: &job_id1,
            queue: &queue1,
            timeout: PositiveI32::new(30).unwrap(),
            max_retries: Some(PositiveI32::new(3).unwrap()),
            payload: payload1,
            state: None,
            run_at: None,
        };

        store.enqueue(EnqueueMode::Unique, &[job1]).await?;
        assert!(store.exists(&queue1, &job_id1).await?);

        store.enqueue(EnqueueMode::Unique, &[job2]).await?;
        assert!(store.exists(&queue2, &job_id2).await?);

        let next = store.next(&queue2, GtZeroI64::new(1).unwrap()).await?;
        assert!(next.is_some());
        assert_eq!(1, *&next.as_ref().unwrap().len());
        let next = &next.unwrap()[0];

        let now = Utc::now().duration_trunc(chrono::Duration::milliseconds(100))?;
        let result = store
            .reschedule(
                EnqueueMode::Unique,
                &next.queue,
                &next.id,
                &next.run_id.unwrap(),
                &reschedule,
            )
            .await;
        assert!(matches!(
            result.unwrap_err(),
            Error::JobExists {
                queue: queue1,
                job_id: job_id1,
            }
        ));
        assert!(store.exists(&reschedule.queue, &reschedule.id).await?);

        // test job was deleted even though existing job existed
        store
            .reschedule(
                EnqueueMode::Ignore,
                &queue2,
                &job_id2,
                &next.run_id.unwrap(),
                &reschedule,
            )
            .await?;
        assert!(!store.exists(&queue2, &job_id2).await?);
        assert!(store.exists(&queue1, &job_id1).await?);
        Ok(())
    }

    #[tokio::test]
    async fn test_reschedule_unique_pk_change_exists() -> anyhow::Result<()> {
        let db_url = std::env::var("DATABASE_URL")?;
        let store = PgStore::default(&db_url).await?;
        let job_id1 = Uuid::new_v4().to_string();
        let job_id2 = Uuid::new_v4().to_string();
        let queue1 = Uuid::new_v4().to_string();
        let queue2 = Uuid::new_v4().to_string();
        let payload1 = &RawValue::from_string("{}".to_string())?;
        let payload2 = &RawValue::from_string(r#"{"key": "value"}"#.to_string())?;
        let job1 = NewJob {
            id: &job_id1,
            queue: &queue1,
            timeout: PositiveI32::new(30).unwrap(),
            max_retries: Some(PositiveI32::new(3).unwrap()),
            payload: payload1,
            state: None,
            run_at: None,
        };
        let job2 = NewJob {
            id: &job_id2,
            queue: &queue2,
            timeout: PositiveI32::new(30).unwrap(),
            max_retries: Some(PositiveI32::new(3).unwrap()),
            payload: payload2,
            state: None,
            run_at: None,
        };
        let reschedule = NewJob {
            id: &job_id1,
            queue: &queue1,
            timeout: PositiveI32::new(30).unwrap(),
            max_retries: Some(PositiveI32::new(3).unwrap()),
            payload: payload1,
            state: None,
            run_at: None,
        };

        store.enqueue(EnqueueMode::Unique, &[job1]).await?;
        assert!(store.exists(&queue1, &job_id1).await?);

        store.enqueue(EnqueueMode::Unique, &[job2]).await?;
        assert!(store.exists(&queue2, &job_id2).await?);

        let next = store.next(&queue2, GtZeroI64::new(1).unwrap()).await?;
        assert!(next.is_some());
        assert_eq!(1, *&next.as_ref().unwrap().len());
        let next = &next.unwrap()[0];

        let now = Utc::now().duration_trunc(chrono::Duration::milliseconds(100))?;
        let result = store
            .reschedule(
                EnqueueMode::Unique,
                &queue2,
                &job_id2,
                &next.run_id.unwrap(),
                &reschedule,
            )
            .await;
        assert!(matches!(
            result.unwrap_err(),
            Error::JobExists {
                queue: queue1,
                job_id: job_id1,
            }
        ));
        Ok(())
    }

    #[tokio::test]
    async fn test_reschedule_no_pk_change() -> anyhow::Result<()> {
        let db_url = std::env::var("DATABASE_URL")?;
        let store = PgStore::default(&db_url).await?;
        let job_id = Uuid::new_v4().to_string();
        let queue = Uuid::new_v4().to_string();
        let payload = &RawValue::from_string("{}".to_string())?;
        let payload2 = &RawValue::from_string(r#"{"key": "value"}"#.to_string())?;
        let job = NewJob {
            id: &job_id,
            queue: &queue,
            timeout: PositiveI32::new(30).unwrap(),
            max_retries: Some(PositiveI32::new(3).unwrap()),
            payload,
            state: None,
            run_at: None,
        };

        store.enqueue(EnqueueMode::Unique, &[job]).await?;
        assert!(store.exists(&queue, &job_id).await?);

        let next = store.next(&queue, GtZeroI64::new(1).unwrap()).await?;
        assert!(next.is_some());
        assert_eq!(1, *&next.as_ref().unwrap().len());
        let next = &next.unwrap()[0];

        let now = Utc::now().duration_trunc(chrono::Duration::milliseconds(100))?;
        let mut reschedule = NewJob {
            id: &job_id,
            queue: &queue,
            timeout: PositiveI32::new(31).unwrap(),
            max_retries: Some(PositiveI32::new(4).unwrap()),
            payload: payload2,
            state: None,
            run_at: Some(now.clone()),
        };
        store
            .reschedule(
                EnqueueMode::Unique,
                &reschedule.queue,
                &reschedule.id,
                &next.run_id.unwrap(),
                &reschedule,
            )
            .await?;

        let result = store.get(&reschedule.queue, &reschedule.id).await?;
        assert!(result.is_some());
        let result = result.unwrap();
        assert_eq!(reschedule.id, result.id);
        assert_eq!(reschedule.queue, result.queue);
        assert_eq!(PositiveI32::new(31).unwrap(), result.timeout);
        assert_eq!(Some(PositiveI32::new(4).unwrap()), result.max_retries);
        assert_eq!(Some(PositiveI32::new(4).unwrap()), result.retries_remaining);
        assert_eq!(payload2.to_string(), result.payload.to_string());
        assert!(result.state.is_none());
        assert!(result.run_id.is_none());
        assert_eq!(now, result.run_at);

        // tests that can't reschedule a job no longer in flight anymore
        reschedule.payload = payload;
        store
            .reschedule(
                EnqueueMode::Unique,
                &reschedule.queue,
                &reschedule.id,
                &next.run_id.unwrap(),
                &reschedule,
            )
            .await?;
        let result = store.get(&reschedule.queue, &reschedule.id).await?;
        assert!(result.is_some());
        let result = result.unwrap();
        assert_eq!(payload2.to_string(), result.payload.to_string());

        store.delete(&queue, &job_id).await?;
        assert!(!store.exists(&queue, &job_id).await?);

        // ensures no error rescheduling when no Job exists
        store
            .reschedule(
                EnqueueMode::Unique,
                &reschedule.queue,
                &reschedule.id,
                &next.run_id.unwrap(),
                &reschedule,
            )
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_enqueue_do_nothing() -> anyhow::Result<()> {
        let db_url = std::env::var("DATABASE_URL")?;
        let store = PgStore::default(&db_url).await?;
        let job_id = Uuid::new_v4().to_string();
        let queue = Uuid::new_v4().to_string();
        let payload = &RawValue::from_string("{}".to_string())?;
        let payload2 = &RawValue::from_string(r#"{"key": "value"}"#.to_string())?;
        let job1 = NewJob {
            id: &job_id,
            queue: &queue,
            timeout: PositiveI32::new(30).unwrap(),
            max_retries: Some(PositiveI32::new(3).unwrap()),
            payload,
            state: None,
            run_at: None,
        };
        let job2 = NewJob {
            id: &job_id,
            queue: &queue,
            timeout: PositiveI32::new(31).unwrap(),
            max_retries: Some(PositiveI32::new(4).unwrap()),
            payload: payload2,
            state: None,
            run_at: None,
        };
        store.enqueue(EnqueueMode::Ignore, &[job1]).await?;
        assert!(store.exists(&queue, &job_id).await?);

        let next = store.next(&queue, GtZeroI64::new(1).unwrap()).await?;
        assert!(next.is_some());
        assert_eq!(1, *&next.as_ref().unwrap().len());
        let next = &next.unwrap()[0];

        store.enqueue(EnqueueMode::Ignore, &[job2]).await?;
        assert!(store.exists(&queue, &job_id).await?);

        let j = store.get(&queue, &job_id).await?;
        assert!(j.is_some());
        let j = j.unwrap();
        assert_eq!(job_id, j.id);
        assert_eq!(queue, j.queue);
        assert_eq!(PositiveI32::new(30).unwrap(), j.timeout);
        assert_eq!(Some(PositiveI32::new(3).unwrap()), j.max_retries);
        assert_eq!(Some(PositiveI32::new(3).unwrap()), j.retries_remaining);
        assert_eq!(payload.to_string(), j.payload.to_string());
        assert!(j.state.is_none());
        assert!(j.run_id.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn test_enqueue_replace() -> anyhow::Result<()> {
        let db_url = std::env::var("DATABASE_URL")?;
        let store = PgStore::default(&db_url).await?;
        let job_id = Uuid::new_v4().to_string();
        let queue = Uuid::new_v4().to_string();
        let payload = &RawValue::from_string("{}".to_string())?;
        let payload2 = &RawValue::from_string(r#"{"key": "value"}"#.to_string())?;
        let job1 = NewJob {
            id: &job_id,
            queue: &queue,
            timeout: PositiveI32::new(30).unwrap(),
            max_retries: Some(PositiveI32::new(3).unwrap()),
            payload,
            state: None,
            run_at: None,
        };
        let job2 = NewJob {
            id: &job_id,
            queue: &queue,
            timeout: PositiveI32::new(31).unwrap(),
            max_retries: Some(PositiveI32::new(4).unwrap()),
            payload: payload2,
            state: None,
            run_at: None,
        };
        store.enqueue(EnqueueMode::Replace, &[job1]).await?;
        assert!(store.exists(&queue, &job_id).await?);

        let next = store.next(&queue, GtZeroI64::new(1).unwrap()).await?;
        assert!(next.is_some());
        assert_eq!(1, *&next.as_ref().unwrap().len());
        let next = &next.unwrap()[0];

        store.enqueue(EnqueueMode::Replace, &[job2]).await?;
        assert!(store.exists(&queue, &job_id).await?);

        let j = store.get(&queue, &job_id).await?;
        assert!(j.is_some());
        let j = j.unwrap();
        assert_eq!(job_id, j.id);
        assert_eq!(queue, j.queue);
        assert_eq!(PositiveI32::new(31).unwrap(), j.timeout);
        assert_eq!(Some(PositiveI32::new(4).unwrap()), j.max_retries);
        assert_eq!(Some(PositiveI32::new(4).unwrap()), j.retries_remaining);
        assert_eq!(payload2.to_string(), j.payload.to_string());
        assert!(j.state.is_none());
        assert!(j.run_id.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_enqueue_already_exist() -> anyhow::Result<()> {
        let db_url = std::env::var("DATABASE_URL")?;
        let store = PgStore::default(&db_url).await?;
        let job_id = Uuid::new_v4().to_string();
        let queue = Uuid::new_v4().to_string();
        let payload = &RawValue::from_string("{}".to_string())?;
        let job1 = NewJob {
            id: &job_id,
            queue: &queue,
            timeout: PositiveI32::new(30).unwrap(),
            max_retries: Some(PositiveI32::new(3).unwrap()),
            payload,
            state: None,
            run_at: None,
        };
        let job2 = NewJob {
            id: &job_id,
            queue: &queue,
            timeout: PositiveI32::new(31).unwrap(),
            max_retries: Some(PositiveI32::new(4).unwrap()),
            payload,
            state: None,
            run_at: None,
        };
        store.enqueue(EnqueueMode::Unique, &[job1]).await?;
        assert!(store.exists(&queue, &job_id).await?);

        let result = store.enqueue(EnqueueMode::Unique, &[job2]).await;
        assert_eq!(
            Err(Error::JobExists {
                job_id: job_id.clone(),
                queue: queue.clone()
            }),
            result
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_lifecycle() -> anyhow::Result<()> {
        let db_url = std::env::var("DATABASE_URL")?;
        let store = PgStore::default(&db_url).await?;
        let job_id = Uuid::new_v4().to_string();
        let queue = Uuid::new_v4().to_string();
        let payload = &RawValue::from_string("{}".to_string())?;
        let job1 = NewJob {
            id: &job_id,
            queue: &queue,
            timeout: PositiveI32::new(30).unwrap(),
            max_retries: Some(PositiveI32::new(3).unwrap()),
            payload,
            state: None,
            run_at: None,
        };
        let job2 = NewJob {
            id: &job_id,
            queue: &queue,
            timeout: PositiveI32::new(30).unwrap(),
            max_retries: Some(PositiveI32::new(3).unwrap()),
            payload,
            state: None,
            run_at: None,
        };
        store.enqueue(EnqueueMode::Unique, &[job1]).await?;
        assert!(store.exists(&queue, &job_id).await?);

        let result = store.get(&queue, &job_id).await?;
        assert!(result.is_some());
        let result = result.unwrap();
        assert_eq!(job_id, result.id);
        assert_eq!(queue, result.queue);
        assert_eq!(PositiveI32::new(30).unwrap(), result.timeout);
        assert_eq!(Some(PositiveI32::new(3).unwrap()), result.max_retries);
        assert_eq!(Some(PositiveI32::new(3).unwrap()), result.retries_remaining);
        assert_eq!(payload.to_string(), result.payload.to_string());
        assert!(result.state.is_none());
        assert!(result.run_id.is_none());

        let result = store.enqueue(EnqueueMode::Unique, &[job2]).await;
        assert_eq!(
            Err(Error::JobExists {
                job_id: job_id.clone(),
                queue: queue.clone()
            }),
            result
        );

        let result = store.next(&queue, GtZeroI64::new(1).unwrap()).await?;
        assert!(result.is_some());
        assert_eq!(1, *&result.as_ref().unwrap().len());
        let result = &result.as_ref().unwrap()[0];
        assert_eq!(job_id, result.id);
        assert_eq!(queue, result.queue);
        assert_eq!(PositiveI32::new(30).unwrap(), result.timeout);
        assert_eq!(Some(PositiveI32::new(3).unwrap()), result.max_retries);
        assert_eq!(Some(PositiveI32::new(3).unwrap()), result.retries_remaining);
        assert_eq!(payload.to_string(), result.payload.to_string());
        assert!(result.state.is_none());
        assert!(result.run_id.is_some());

        let result = store
            .complete(&result.queue, &result.id, &result.run_id.unwrap())
            .await;
        assert!(result.is_ok());
        assert!(!store.exists(&queue, &job_id).await?);
        Ok(())
    }

    #[tokio::test]
    async fn test_delete() -> anyhow::Result<()> {
        let db_url = std::env::var("DATABASE_URL")?;
        let store = PgStore::default(&db_url).await?;
        let job_id = Uuid::new_v4().to_string();
        let queue = Uuid::new_v4().to_string();
        let payload = &RawValue::from_string("{}".to_string())?;
        let job = NewJob {
            id: &job_id,
            queue: &queue,
            timeout: PositiveI32::new(30).unwrap(),
            max_retries: Some(PositiveI32::new(3).unwrap()),
            payload,
            state: None,
            run_at: None,
        };
        store.enqueue(EnqueueMode::Unique, &[job]).await?;
        assert!(store.exists(&queue, &job_id).await?);

        let next = store.next(&queue, GtZeroI64::new(1).unwrap()).await?;
        assert!(next.is_some());
        let jobs = next.unwrap();
        let next = jobs.get(0).unwrap();

        let result = store.delete(&next.queue, &next.id).await;
        assert!(result.is_ok());
        assert!(!store.exists(&next.queue, &next.id).await?);

        let result = store.delete(&next.queue, &next.id).await;
        assert!(result.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn test_complete() -> anyhow::Result<()> {
        let db_url = std::env::var("DATABASE_URL")?;
        let store = PgStore::default(&db_url).await?;
        let job_id = Uuid::new_v4().to_string();
        let queue = Uuid::new_v4().to_string();
        let payload = &RawValue::from_string("{}".to_string())?;
        let job = NewJob {
            id: &job_id,
            queue: &queue,
            timeout: PositiveI32::new(30).unwrap(),
            max_retries: Some(PositiveI32::new(3).unwrap()),
            payload,
            state: None,
            run_at: None,
        };
        store.enqueue(EnqueueMode::Unique, &[job]).await?;

        let existing = store.get(&queue, &job_id).await?;
        assert!(existing.is_some());
        let existing = existing.unwrap();
        assert!(existing.run_id.is_none());

        let next = store.next(&queue, GtZeroI64::new(1).unwrap()).await?;
        assert!(next.is_some());
        let jobs = next.unwrap();
        let next = jobs.get(0).unwrap();

        let result = store
            .complete(&queue, &next.id, &next.run_id.unwrap())
            .await;
        assert!(result.is_ok());
        assert!(!store.exists(&next.queue, &next.id).await?);

        // doesn't exist anymore, should return ok still
        let result = store
            .complete(&next.queue, &next.id, &next.run_id.unwrap())
            .await;
        assert!(result.is_ok());
        Ok(())
    }
}
