use axum::body::{Body, BoxBody};
use axum::extract::{Path, Query, State};
use axum::http::{Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, head, patch, post, put};
use axum::{Json, Router};
use metrics::increment_counter;
use relay_core::job::{EnqueueMode, New, OldV1};
use relay_core::num::{GtZeroI64, PositiveI16, PositiveI32};
use relay_postgres::{Error as PostgresError, PgStore};
use serde::Deserialize;
use serde_json::value::RawValue;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tower_http::trace::TraceLayer;
use tracing::{info, span, Level, Span};
use uuid::Uuid;

/// The internal HTTP server representation for Jobs.
pub struct Server;

#[derive(Deserialize)]
struct EnqueueQueryInfo {
    mode: Option<String>,
}

impl EnqueueQueryInfo {
    fn enqueue_mode(&self) -> EnqueueMode {
        match self.mode.as_deref() {
            Some("ignore") => EnqueueMode::Ignore,
            Some("replace") => EnqueueMode::Replace,
            _ => EnqueueMode::Unique,
        }
    }
}

#[tracing::instrument(name = "http_enqueue_v2", level = "debug", skip_all)]
async fn enqueue_v2(
    State(state): State<Arc<PgStore>>,
    params: Query<EnqueueQueryInfo>,
    jobs: Json<Vec<New<Box<RawValue>, Box<RawValue>>>>,
) -> Response {
    increment_counter!("http_request", "endpoint" => "enqueue", "version" => "v2");

    if let Err(e) = state.enqueue(params.0.enqueue_mode(), jobs.0.iter()).await {
        increment_counter!("errors", "endpoint" => "enqueue", "type" => e.error_type());
        match e {
            PostgresError::Backend { .. } => {
                if e.is_retryable() {
                    (StatusCode::TOO_MANY_REQUESTS, e.to_string()).into_response()
                } else {
                    (StatusCode::UNPROCESSABLE_ENTITY, e.to_string()).into_response()
                }
            }
            PostgresError::JobExists { .. } => {
                (StatusCode::CONFLICT, e.to_string()).into_response()
            }
            PostgresError::JobNotFound { .. } => {
                (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
            }
        }
    } else {
        StatusCode::ACCEPTED.into_response()
    }
}

#[tracing::instrument(name = "http_enqueue_v1", level = "debug", skip_all)]
async fn enqueue_v1(
    State(state): State<Arc<PgStore>>,
    jobs: Json<Vec<OldV1<Box<RawValue>, Box<RawValue>>>>,
) -> Response {
    increment_counter!("http_request", "endpoint" => "enqueue", "version" => "v1");

    let mode = if jobs.len() == 1 {
        EnqueueMode::Unique
    } else {
        EnqueueMode::Ignore
    };
    let jobs = jobs
        .0
        .into_iter()
        .map(|j| New {
            id: j.id,
            queue: j.queue,
            timeout: PositiveI32::new(j.timeout).unwrap_or_else(|| PositiveI32::new(0).unwrap()),
            max_retries: if j.max_retries < 0 {
                None
            } else if j.max_retries > i32::from(i16::MAX) {
                Some(PositiveI16::new(i16::MAX).unwrap())
            } else if j.max_retries < i32::from(i16::MIN) {
                None
            } else {
                Some(
                    PositiveI16::new(i16::try_from(j.max_retries).unwrap())
                        .unwrap_or_else(|| PositiveI16::new(0).unwrap()),
                )
            },
            payload: j.payload,
            state: j.state,
            run_at: j.run_at,
        })
        .collect::<Vec<_>>();

    if let Err(e) = state.enqueue(mode, jobs.iter()).await {
        increment_counter!("errors", "endpoint" => "enqueue", "type" => e.error_type(), "version" => "v2");
        match e {
            PostgresError::Backend { .. } => {
                if e.is_retryable() {
                    (StatusCode::TOO_MANY_REQUESTS, e.to_string()).into_response()
                } else {
                    (StatusCode::UNPROCESSABLE_ENTITY, e.to_string()).into_response()
                }
            }
            PostgresError::JobExists { .. } => {
                (StatusCode::CONFLICT, e.to_string()).into_response()
            }
            PostgresError::JobNotFound { .. } => {
                (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
            }
        }
    } else {
        StatusCode::ACCEPTED.into_response()
    }
}

#[tracing::instrument(name = "http_get_v2", level = "debug", skip_all)]
async fn get_job(
    State(state): State<Arc<PgStore>>,
    Path((queue, id)): Path<(String, String)>,
) -> Response {
    increment_counter!("http_request", "endpoint" => "get", "queue" => queue.clone(), "version" => "v2");

    match state.get(&queue, &id).await {
        Ok(job) => {
            if let Some(job) = job {
                Json(job).into_response()
            } else {
                StatusCode::NOT_FOUND.into_response()
            }
        }
        Err(e) => {
            increment_counter!("errors", "endpoint" => "get", "type" => e.error_type(), "queue" => e.queue(), "version" => "v2");
            match e {
                PostgresError::Backend { .. } => {
                    if e.is_retryable() {
                        (StatusCode::TOO_MANY_REQUESTS, e.to_string()).into_response()
                    } else {
                        (StatusCode::UNPROCESSABLE_ENTITY, e.to_string()).into_response()
                    }
                }
                _ => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
            }
        }
    }
}

#[tracing::instrument(name = "http_exists_v2", level = "debug", skip_all)]
async fn exists(
    State(state): State<Arc<PgStore>>,
    Path((queue, id)): Path<(String, String)>,
) -> Response {
    increment_counter!("http_request", "endpoint" => "exists", "queue" => queue.clone(), "version" => "v2");

    match state.exists(&queue, &id).await {
        Ok(exists) => {
            if exists {
                StatusCode::OK.into_response()
            } else {
                StatusCode::NOT_FOUND.into_response()
            }
        }
        Err(e) => {
            increment_counter!("errors", "endpoint" => "exists", "type" => e.error_type(), "queue" => e.queue(), "version" => "v2");
            match e {
                PostgresError::Backend { .. } => {
                    if e.is_retryable() {
                        (StatusCode::TOO_MANY_REQUESTS, e.to_string()).into_response()
                    } else {
                        (StatusCode::UNPROCESSABLE_ENTITY, e.to_string()).into_response()
                    }
                }
                _ => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
            }
        }
    }
}

#[tracing::instrument(name = "http_heartbeat_v2", level = "debug", skip_all)]
async fn heartbeat(
    State(state): State<Arc<PgStore>>,
    Path((queue, id, run_id)): Path<(String, String, Uuid)>,
    job_state: Option<Json<Box<RawValue>>>,
) -> Response {
    increment_counter!("http_request", "endpoint" => "heartbeat", "queue" => queue.clone(), "version" => "v2");

    let job_state = match job_state {
        None => None,
        Some(job_state) => Some(job_state.0),
    };
    if let Err(e) = state
        .heartbeat(&queue, &id, &run_id, job_state.as_deref())
        .await
    {
        increment_counter!("errors", "endpoint" => "heartbeat", "type" => e.error_type(), "queue" => e.queue(), "version" => "v2");
        match e {
            PostgresError::JobNotFound { .. } => {
                (StatusCode::NOT_FOUND, e.to_string()).into_response()
            }
            PostgresError::Backend { .. } => {
                if e.is_retryable() {
                    (StatusCode::TOO_MANY_REQUESTS, e.to_string()).into_response()
                } else {
                    (StatusCode::UNPROCESSABLE_ENTITY, e.to_string()).into_response()
                }
            }
            PostgresError::JobExists { .. } => {
                (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
            }
        }
    } else {
        StatusCode::ACCEPTED.into_response()
    }
}

#[tracing::instrument(name = "http_re_enqueue_v2", level = "debug", skip_all)]
async fn re_enqueue(
    State(state): State<Arc<PgStore>>,
    Path((queue, id, run_id)): Path<(String, String, Uuid)>,
    params: Query<EnqueueQueryInfo>,
    jobs: Json<Vec<New<Box<RawValue>, Box<RawValue>>>>,
) -> Response {
    increment_counter!("http_request", "endpoint" => "re_enqueue", "version" => "v2");

    if let Err(e) = state
        .requeue(params.0.enqueue_mode(), &queue, &id, &run_id, jobs.0.iter())
        .await
    {
        increment_counter!("errors", "endpoint" => "re_enqueue", "type" => e.error_type(), "queue" => e.queue(), "version" => "v2");
        match e {
            PostgresError::JobExists { .. } => {
                (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
            }
            PostgresError::Backend { .. } => {
                if e.is_retryable() {
                    (StatusCode::TOO_MANY_REQUESTS, e.to_string()).into_response()
                } else {
                    (StatusCode::UNPROCESSABLE_ENTITY, e.to_string()).into_response()
                }
            }
            PostgresError::JobNotFound { .. } => {
                (StatusCode::NOT_FOUND, e.to_string()).into_response()
            }
        }
    } else {
        StatusCode::ACCEPTED.into_response()
    }
}

#[tracing::instrument(name = "http_delete_v2", level = "debug", skip_all)]
async fn delete_job(
    State(state): State<Arc<PgStore>>,
    Path((queue, id)): Path<(String, String)>,
) -> Response {
    increment_counter!("http_request", "endpoint" => "delete", "queue" => queue.clone(), "version" => "v2");

    if let Err(e) = state.delete(&queue, &id).await {
        increment_counter!("errors", "endpoint" => "delete", "type" => e.error_type(), "queue" => e.queue(), "version" => "v2");
        match e {
            PostgresError::JobNotFound { .. } => {
                (StatusCode::NOT_FOUND, e.to_string()).into_response()
            }
            PostgresError::Backend { .. } => {
                if e.is_retryable() {
                    (StatusCode::TOO_MANY_REQUESTS, e.to_string()).into_response()
                } else {
                    (StatusCode::UNPROCESSABLE_ENTITY, e.to_string()).into_response()
                }
            }
            PostgresError::JobExists { .. } => {
                (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
            }
        }
    } else {
        StatusCode::OK.into_response()
    }
}

#[tracing::instrument(name = "http_complete_v2", level = "debug", skip_all)]
async fn complete_job(
    State(state): State<Arc<PgStore>>,
    Path((queue, id, run_id)): Path<(String, String, Uuid)>,
) -> Response {
    increment_counter!("http_request", "endpoint" => "delete", "queue" => queue.clone(), "version" => "v2");

    if let Err(e) = state.complete(&queue, &id, &run_id).await {
        increment_counter!("errors", "endpoint" => "delete", "type" => e.error_type(), "queue" => e.queue(), "version" => "v2");
        match e {
            PostgresError::JobNotFound { .. } => {
                (StatusCode::NOT_FOUND, e.to_string()).into_response()
            }
            PostgresError::Backend { .. } => {
                if e.is_retryable() {
                    (StatusCode::TOO_MANY_REQUESTS, e.to_string()).into_response()
                } else {
                    (StatusCode::UNPROCESSABLE_ENTITY, e.to_string()).into_response()
                }
            }
            PostgresError::JobExists { .. } => {
                (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
            }
        }
    } else {
        StatusCode::OK.into_response()
    }
}

#[derive(Deserialize)]
struct NextQueryInfo {
    #[serde(default = "default_num_jobs")]
    num_jobs: GtZeroI64,
}

fn default_num_jobs() -> GtZeroI64 {
    GtZeroI64::new(1).unwrap()
}

#[tracing::instrument(name = "http_next_v2", level = "debug", skip_all)]
async fn next(
    State(state): State<Arc<PgStore>>,
    Path(queue): Path<String>,
    params: Query<NextQueryInfo>,
) -> Response {
    increment_counter!("http_request", "endpoint" => "next", "queue" => queue.clone(), "version" => "v2");

    match state.next(&queue, params.0.num_jobs).await {
        Err(e) => {
            increment_counter!("errors", "endpoint" => "next", "type" => e.error_type(), "queue" => e.queue(), "version" => "v2");
            if let PostgresError::Backend { .. } = e {
                if e.is_retryable() {
                    (StatusCode::TOO_MANY_REQUESTS, e.to_string()).into_response()
                } else {
                    (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
                }
            } else {
                (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
            }
        }
        Ok(job) => match job {
            None => StatusCode::NO_CONTENT.into_response(),
            Some(job) => (StatusCode::OK, Json(job)).into_response(),
        },
    }
}

#[allow(clippy::unused_async)]
async fn health() {}

impl Server {
    /// starts the HTTP server and waits for a shutdown signal before returning.
    ///
    /// # Errors
    ///
    /// Will return `Err` if the server fails to start.
    ///
    /// # Panics
    ///
    /// Will panic the reaper async thread fails, which can only happen if the timer and channel
    /// both die.
    #[inline]
    pub async fn run<F>(backend: Arc<PgStore>, addr: &str, shutdown: F) -> anyhow::Result<()>
    where
        F: Future<Output = ()>,
    {
        let app = Server::init_app(backend);

        axum::Server::bind(&addr.parse().unwrap())
            .serve(app.into_make_service())
            .with_graceful_shutdown(shutdown)
            .await
            .unwrap();
        Ok(())
    }

    pub(crate) fn init_app(backend: Arc<PgStore>) -> Router {
        Router::new()
            .route("/v1/queues/jobs", post(enqueue_v1))
            .route("/v2/queues/jobs", post(enqueue_v2))
            .route("/v2/queues/:queue/jobs/:id/run_id/:run_id", put(re_enqueue))
            .route("/v2/queues/:queue/jobs", get(next))
            .route("/v2/queues/:queue/jobs/:id", head(exists))
            .route("/v2/queues/:queue/jobs/:id", get(get_job))
            .route(
                "/v2/queues/:queue/jobs/:id/run_id/:run_id",
                patch(heartbeat),
            )
            .route("/v2/queues/:queue/jobs/:id", delete(delete_job))
            .route(
                "/v2/queues/:queue/jobs/:id/run_id/:run_id",
                delete(complete_job),
            )
            .route("/health", get(health))
            .layer(
                TraceLayer::new_for_http()
                    .make_span_with(|request: &Request<Body>| {
                        let dbg = span!(
                            Level::DEBUG,
                            "request",
                            id = %Uuid::new_v4().to_string(),
                        );
                        span!(
                            parent: &dbg,
                            Level::INFO,
                            "request",
                            method = %request.method(),
                            uri = %request.uri(),
                            version = ?request.version(),
                        )
                    })
                    .on_response(
                        |response: &Response<BoxBody>, latency: Duration, _span: &Span| {
                            info!(
                                target: "response",
                                status = response.status().as_u16(),
                                latency = ?latency,
                            );
                        },
                    ),
            )
            .with_state(backend)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::{anyhow, Context};
    use chrono::DurationRound;
    use chrono::Utc;
    use portpicker::pick_unused_port;
    use relay_client::http::client::{Builder as ClientBuilder, Client, Error as ClientError};
    use relay_core::num::PositiveI32;
    use relay_postgres::PgStore;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener};
    use tokio::task::JoinHandle;
    use uuid::Uuid;

    /// Generates a `SocketAddr` on the IP 0.0.0.0, using a random port.
    pub fn new_random_socket_addr() -> anyhow::Result<SocketAddr> {
        let ip_address = IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0));
        let port = pick_unused_port().ok_or_else(|| anyhow!("No free port was found"))?;
        let addr = SocketAddr::new(ip_address, port);
        Ok(addr)
    }

    async fn init_server() -> anyhow::Result<(JoinHandle<()>, Arc<Client>)> {
        let db_url = std::env::var("DATABASE_URL")?;
        let store = Arc::new(PgStore::default(&db_url).await?);
        let app = Server::init_app(store);

        let socket_address =
            new_random_socket_addr().expect("Cannot create socket address for use");
        let listener = TcpListener::bind(socket_address)
            .with_context(|| "Failed to create TCPListener for TestServer")?;
        let server_address = socket_address.to_string();
        let server = axum::Server::from_tcp(listener)
            .with_context(|| "Failed to create ::axum::Server for TestServer")?
            .serve(app.into_make_service());

        let server_thread = tokio::spawn(async move {
            server.await.expect("Expect server to start serving");
        });

        let url = format!("http://{server_address}");
        let client = ClientBuilder::new(&url).build();
        Ok((server_thread, Arc::new(client)))
    }

    #[tokio::test]
    async fn test_oneshot_job_v2() -> anyhow::Result<()> {
        let (_srv, client) = init_server().await?;
        let now = Utc::now()
            .duration_trunc(chrono::Duration::milliseconds(1))
            .unwrap();
        let job: New<(), i32> = New {
            id: Uuid::new_v4().to_string(),
            queue: Uuid::new_v4().to_string(),
            timeout: PositiveI32::new(30).unwrap(),
            max_retries: None,
            payload: (),
            state: None,
            run_at: Some(now),
        };
        let jobs = vec![job];

        client.enqueue(EnqueueMode::Unique, &jobs).await?;

        let exists = client
            .exists(&jobs.get(0).unwrap().queue, &jobs.get(0).unwrap().id)
            .await?;
        assert!(exists);

        let mut j = client
            .get::<(), i32>(&jobs.get(0).unwrap().queue, &jobs.get(0).unwrap().id)
            .await?;
        assert!(j.updated_at >= now);
        assert_eq!(&jobs.get(0).unwrap().id, j.id.as_str());
        assert_eq!(&jobs.get(0).unwrap().queue, j.queue.as_str());
        assert_eq!(&jobs.get(0).unwrap().timeout, &j.timeout);

        let mut jobs = client
            .poll::<(), i32>(&jobs.get(0).unwrap().queue, 10)
            .await?;
        assert_eq!(jobs.len(), 1);

        let j2 = jobs.pop().unwrap();
        j.updated_at = j2.updated_at;
        j.run_id = j2.run_id;
        assert_eq!(j2, j);
        assert!(j2.run_id.is_some());

        client
            .heartbeat(&j2.queue, &j2.id, &j2.run_id.unwrap(), Some(3))
            .await?;

        let j = client.get::<(), i32>(&j2.queue, &j2.id).await?;
        assert_eq!(j.state, Some(3));

        client
            .complete(&j2.queue, &j2.id, &j2.run_id.unwrap())
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_re_enqueue_v2() -> anyhow::Result<()> {
        let (_srv, client) = init_server().await?;
        let now = Utc::now()
            .duration_trunc(chrono::Duration::milliseconds(1))
            .unwrap();
        let job: New<(), i32> = New {
            id: Uuid::new_v4().to_string(),
            queue: Uuid::new_v4().to_string(),
            timeout: PositiveI32::new(30).unwrap(),
            max_retries: None,
            payload: (),
            state: None,
            run_at: Some(now),
        };
        let mut jobs = vec![job];

        client.enqueue(EnqueueMode::Unique, &jobs).await?;

        let mut j = client
            .get::<(), i32>(&jobs.get(0).unwrap().queue, &jobs.get(0).unwrap().id)
            .await?;
        assert!(j.updated_at >= now);
        assert_eq!(&jobs.get(0).unwrap().id, j.id.as_str());
        assert_eq!(&jobs.get(0).unwrap().queue, j.queue.as_str());
        assert_eq!(&jobs.get(0).unwrap().timeout, &j.timeout);

        let mut polled_jobs = client
            .poll::<(), i32>(&jobs.get(0).unwrap().queue, 10)
            .await?;
        assert_eq!(polled_jobs.len(), 1);

        let j2 = polled_jobs.pop().unwrap();
        j.updated_at = j2.updated_at;
        j.run_id = j2.run_id;
        assert_eq!(j2, j);
        assert!(j2.run_id.is_some());

        jobs.get_mut(0).unwrap().run_at = Some(
            Utc::now()
                .duration_trunc(chrono::Duration::milliseconds(1))
                .unwrap(),
        );
        client
            .requeue(
                EnqueueMode::Replace,
                &j2.queue,
                &j2.id,
                &j2.run_id.unwrap(),
                &jobs,
            )
            .await?;

        let mut j = client.get::<(), i32>(&j2.queue, &j2.id).await?;
        assert!(j.updated_at >= j2.updated_at);
        assert!(j.created_at >= j2.created_at);
        assert!(j.run_id.is_none());
        j.updated_at = j2.updated_at;
        j.created_at = j2.created_at;
        j.run_id = j2.run_id;
        j.run_at = j2.run_at;
        assert_eq!(j, j2);

        client.delete(&j2.queue, &j2.id).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_enqueue_modes_v2() -> anyhow::Result<()> {
        let (_srv, client) = init_server().await?;
        let job: New<(), i32> = New {
            id: Uuid::new_v4().to_string(),
            queue: Uuid::new_v4().to_string(),
            timeout: PositiveI32::new(30).unwrap(),
            max_retries: None,
            payload: (),
            state: None,
            run_at: None,
        };
        let mut jobs = vec![job];

        client.enqueue(EnqueueMode::Unique, &jobs).await?;
        let result = client.enqueue(EnqueueMode::Unique, &jobs).await;
        assert_eq!(result, Err(ClientError::JobExists));

        let polled_jobs = client
            .poll::<(), i32>(&jobs.get(0).unwrap().queue, 10)
            .await?;
        assert_eq!(polled_jobs.len(), 1);
        assert_eq!(polled_jobs.get(0).unwrap().state, None);

        let j2 = client
            .get::<(), i32>(&jobs.get(0).unwrap().queue, &jobs.get(0).unwrap().id)
            .await?;
        assert!(j2.run_id.is_some());
        assert!(j2.state.is_none());

        // test replacing the job and seeing the run_id + state change
        jobs.get_mut(0).unwrap().state = Some(3);
        client.enqueue(EnqueueMode::Replace, &jobs).await?; // should not error

        let j2 = client
            .get::<(), i32>(&jobs.get(0).unwrap().queue, &jobs.get(0).unwrap().id)
            .await?;
        assert!(j2.run_id.is_none());
        assert_eq!(j2.state, Some(3));

        // test ignoring, new state should not be written
        jobs.get_mut(0).unwrap().state = Some(4);
        client.enqueue(EnqueueMode::Ignore, &jobs).await?; // should not error

        let j2 = client
            .get::<(), i32>(&jobs.get(0).unwrap().queue, &jobs.get(0).unwrap().id)
            .await?;
        assert!(j2.run_id.is_none());
        assert_eq!(j2.state, Some(3));

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_v2() -> anyhow::Result<()> {
        let (_srv, client) = init_server().await?;
        let job: New<(), i32> = New {
            id: Uuid::new_v4().to_string(),
            queue: Uuid::new_v4().to_string(),
            timeout: PositiveI32::new(30).unwrap(),
            max_retries: None,
            payload: (),
            state: None,
            run_at: None,
        };
        let jobs = vec![job];

        client.enqueue(EnqueueMode::Unique, &jobs).await?;
        assert!(
            client
                .exists(&jobs.get(0).unwrap().queue, &jobs.get(0).unwrap().id)
                .await?
        );

        client
            .delete(&jobs.get(0).unwrap().queue, &jobs.get(0).unwrap().id)
            .await?;

        assert!(
            !client
                .exists(&jobs.get(0).unwrap().queue, &jobs.get(0).unwrap().id)
                .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_complete_v2() -> anyhow::Result<()> {
        let (_srv, client) = init_server().await?;
        let job: New<(), i32> = New {
            id: Uuid::new_v4().to_string(),
            queue: Uuid::new_v4().to_string(),
            timeout: PositiveI32::new(30).unwrap(),
            max_retries: None,
            payload: (),
            state: None,
            run_at: None,
        };
        let jobs = vec![job];

        client.enqueue(EnqueueMode::Unique, &jobs).await?;

        let polled_jobs = client
            .poll::<(), i32>(&jobs.get(0).unwrap().queue, 10)
            .await?;
        assert_eq!(polled_jobs.len(), 1);
        assert!(
            client
                .exists(&jobs.get(0).unwrap().queue, &jobs.get(0).unwrap().id)
                .await?
        );

        client
            .complete(
                &polled_jobs[0].queue,
                &polled_jobs[0].id,
                &polled_jobs[0].run_id.unwrap(),
            )
            .await?;
        assert!(
            !client
                .exists(&jobs.get(0).unwrap().queue, &jobs.get(0).unwrap().id)
                .await?
        );
        // calling again should not return error
        client
            .complete(
                &polled_jobs[0].queue,
                &polled_jobs[0].id,
                &polled_jobs[0].run_id.unwrap(),
            )
            .await?;
        // same for delete
        client
            .delete(&polled_jobs[0].queue, &polled_jobs[0].id)
            .await?;

        Ok(())
    }
}
