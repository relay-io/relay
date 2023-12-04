//! The HTTP server for the relay.
use axum::body::{Body, BoxBody};
use axum::extract::{Path, Query, State};
use axum::http::{Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, head, patch, post, put};
use axum::{Json, Router};
use metrics::increment_counter;
use relay_core::job::{EnqueueMode, New, OldV1};
use relay_core::num::GtZeroI64;
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
                    (StatusCode::SERVICE_UNAVAILABLE, e.to_string()).into_response()
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
async fn get_job_v2(
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
                        (StatusCode::SERVICE_UNAVAILABLE, e.to_string()).into_response()
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
async fn exists_v2(
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
                        (StatusCode::SERVICE_UNAVAILABLE, e.to_string()).into_response()
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
async fn heartbeat_v2(
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
                    (StatusCode::SERVICE_UNAVAILABLE, e.to_string()).into_response()
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

#[tracing::instrument(name = "http_requeue_v2", level = "debug", skip_all)]
async fn requeue(
    State(state): State<Arc<PgStore>>,
    Path((queue, id, run_id)): Path<(String, String, Uuid)>,
    params: Query<EnqueueQueryInfo>,
    jobs: Json<Vec<New<Box<RawValue>, Box<RawValue>>>>,
) -> Response {
    increment_counter!("http_request", "endpoint" => "requeue", "version" => "v2");

    if let Err(e) = state
        .requeue(params.0.enqueue_mode(), &queue, &id, &run_id, jobs.0.iter())
        .await
    {
        increment_counter!("errors", "endpoint" => "requeue", "type" => e.error_type(), "queue" => e.queue(), "version" => "v2");
        match e {
            PostgresError::JobExists { .. } => {
                (StatusCode::CONFLICT, e.to_string()).into_response()
            }
            PostgresError::Backend { .. } => {
                if e.is_retryable() {
                    (StatusCode::SERVICE_UNAVAILABLE, e.to_string()).into_response()
                } else {
                    (StatusCode::UNPROCESSABLE_ENTITY, e.to_string()).into_response()
                }
            }
            PostgresError::JobNotFound { .. } => {
                (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
            }
        }
    } else {
        StatusCode::ACCEPTED.into_response()
    }
}

#[tracing::instrument(name = "http_delete_v2", level = "debug", skip_all)]
async fn delete_job_v2(
    State(state): State<Arc<PgStore>>,
    Path((queue, id)): Path<(String, String)>,
) -> Response {
    increment_counter!("http_request", "endpoint" => "delete", "queue" => queue.clone(), "version" => "v2");

    if let Err(e) = state.delete(&queue, &id).await {
        increment_counter!("errors", "endpoint" => "delete", "type" => e.error_type(), "queue" => e.queue(), "version" => "v2");
        match e {
            PostgresError::Backend { .. } => {
                if e.is_retryable() {
                    (StatusCode::SERVICE_UNAVAILABLE, e.to_string()).into_response()
                } else {
                    (StatusCode::UNPROCESSABLE_ENTITY, e.to_string()).into_response()
                }
            }
            PostgresError::JobNotFound { .. } | PostgresError::JobExists { .. } => {
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
            PostgresError::Backend { .. } => {
                if e.is_retryable() {
                    (StatusCode::SERVICE_UNAVAILABLE, e.to_string()).into_response()
                } else {
                    (StatusCode::UNPROCESSABLE_ENTITY, e.to_string()).into_response()
                }
            }
            PostgresError::JobNotFound { .. } | PostgresError::JobExists { .. } => {
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

const fn default_num_jobs() -> GtZeroI64 {
    unsafe { GtZeroI64::new_unchecked(1) }
}

#[tracing::instrument(name = "http_next_v2", level = "debug", skip_all)]
async fn next_v2(
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
                    (StatusCode::SERVICE_UNAVAILABLE, e.to_string()).into_response()
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
    let jobs = jobs.0.into_iter().map(New::from).collect::<Vec<_>>();

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

#[tracing::instrument(name = "http_reschedule_v1", level = "debug", skip_all)]
async fn reschedule_v1(
    State(state): State<Arc<PgStore>>,
    job: Json<OldV1<Box<RawValue>, Box<RawValue>>>,
) -> Response {
    increment_counter!("http_request", "endpoint" => "reschedule", "queue" => job.0.queue.clone(), "version" => "v1");

    // need to get run_id for new requeue logic.
    let run_id = match state.get(&job.0.queue, &job.0.id).await {
        Ok(Some(job)) if job.run_id.is_some() => job.run_id.unwrap(),
        _ => return StatusCode::ACCEPTED.into_response(),
    };

    let job = New::from(job.0);

    if let Err(e) = state
        .requeue(
            EnqueueMode::Replace,
            &job.queue,
            &job.id,
            &run_id,
            [&job].into_iter(),
        )
        .await
    {
        increment_counter!("errors", "endpoint" => "enqueued", "type" => e.error_type(), "queue" => e.queue(), "version" => "v1");
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

#[tracing::instrument(name = "http_delete_v1", level = "debug", skip_all)]
async fn delete_job_v1(
    State(state): State<Arc<PgStore>>,
    Path((queue, id)): Path<(String, String)>,
) -> Response {
    increment_counter!("http_request", "endpoint" => "delete", "queue" => queue.clone(), "version" => "v1");

    if let Err(e) = state.delete(&queue, &id).await {
        increment_counter!("errors", "endpoint" => "delete", "type" => e.error_type(), "queue" => e.queue(), "version" => "v1");
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

#[tracing::instrument(name = "http_heartbeat_v1", level = "debug", skip_all)]
async fn heartbeat_v1(
    State(state): State<Arc<PgStore>>,
    Path((queue, id)): Path<(String, String)>,
    job_state: Option<Json<Box<RawValue>>>,
) -> Response {
    increment_counter!("http_request", "endpoint" => "heartbeat", "queue" => queue.clone(), "version" => "v1");

    // need to get run_id for new requeue logic.
    let run_id = match state.get(&queue, &id).await {
        Ok(Some(job)) if job.run_id.is_some() => job.run_id.unwrap(),
        _ => {
            return (
                StatusCode::NOT_FOUND,
                format!("job with id `{id}` not found in queue `{queue}`."),
            )
                .into_response()
        }
    };

    let job_state = match job_state {
        None => None,
        Some(job_state) => Some(job_state.0),
    };
    if let Err(e) = state
        .heartbeat(&queue, &id, &run_id, job_state.as_deref())
        .await
    {
        increment_counter!("errors", "endpoint" => "heartbeat", "type" => e.error_type(), "queue" => e.queue(), "version" => "v1");
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

#[tracing::instrument(name = "http_exists_v1", level = "debug", skip_all)]
async fn exists_v1(
    State(state): State<Arc<PgStore>>,
    Path((queue, id)): Path<(String, String)>,
) -> Response {
    increment_counter!("http_request", "endpoint" => "exists", "queue" => queue.clone(), "version" => "v1");

    match state.exists(&queue, &id).await {
        Ok(exists) => {
            if exists {
                StatusCode::OK.into_response()
            } else {
                StatusCode::NOT_FOUND.into_response()
            }
        }
        Err(e) => {
            increment_counter!("errors", "endpoint" => "exists", "type" => e.error_type(), "queue" => e.queue(), "version" => "v1");
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

#[tracing::instrument(name = "http_get_v1", level = "debug", skip_all)]
async fn get_job_v1(
    State(state): State<Arc<PgStore>>,
    Path((queue, id)): Path<(String, String)>,
) -> Response {
    increment_counter!("http_request", "endpoint" => "get", "queue" => queue.clone(), "version" => "v1");

    match state.get(&queue, &id).await {
        Ok(job) => {
            if let Some(job) = job {
                Json(OldV1::from(job)).into_response()
            } else {
                StatusCode::NOT_FOUND.into_response()
            }
        }
        Err(e) => {
            increment_counter!("errors", "endpoint" => "get", "type" => e.error_type(), "queue" => e.queue(), "version" => "v1");
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

#[tracing::instrument(name = "http_next_v1", level = "debug", skip_all)]
async fn next_v1(
    State(state): State<Arc<PgStore>>,
    Path(queue): Path<String>,
    params: Query<NextQueryInfo>,
) -> Response {
    increment_counter!("http_request", "endpoint" => "next", "queue" => queue.clone(), "version" => "v1");

    match state.next(&queue, params.0.num_jobs).await {
        Err(e) => {
            increment_counter!("errors", "endpoint" => "next", "type" => e.error_type(), "queue" => e.queue(), "version" => "v1");
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
            Some(jobs) => {
                let jobs = jobs.into_iter().map(OldV1::from).collect::<Vec<_>>();
                (StatusCode::OK, Json(jobs)).into_response()
            }
        },
    }
}

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
            .route("/v1/queues/jobs", put(reschedule_v1))
            .route("/v1/queues/:queue/jobs", get(next_v1))
            .route("/v1/queues/:queue/jobs/:id", head(exists_v1))
            .route("/v1/queues/:queue/jobs/:id", get(get_job_v1))
            .route("/v1/queues/:queue/jobs/:id", patch(heartbeat_v1))
            .route("/v1/queues/:queue/jobs/:id", delete(delete_job_v1))
            .route("/v2/queues/jobs", post(enqueue_v2))
            .route("/v2/queues/:queue/jobs/:id/run-id/:run_id", put(requeue))
            .route("/v2/queues/:queue/jobs", get(next_v2))
            .route("/v2/queues/:queue/jobs/:id", head(exists_v2))
            .route("/v2/queues/:queue/jobs/:id", get(get_job_v2))
            .route(
                "/v2/queues/:queue/jobs/:id/run-id/:run_id",
                patch(heartbeat_v2),
            )
            .route("/v2/queues/:queue/jobs/:id", delete(delete_job_v2))
            .route(
                "/v2/queues/:queue/jobs/:id/run-id/:run_id",
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
    use async_trait::async_trait;
    use chrono::DurationRound;
    use chrono::Utc;
    use portpicker::pick_unused_port;
    use relay_client::http::client::{
        Builder as ClientBuilder, Client, Error as ClientError, JobHelper, Runner,
    };
    use relay_core::job::Existing;
    use relay_core::num::PositiveI32;
    use relay_postgres::PgStore;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener};
    use std::ops::Add;
    use std::sync::Mutex;
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
        let (st, base_url) = init_server_only().await?;
        let client = ClientBuilder::new(&base_url).build();
        Ok((st, Arc::new(client)))
    }

    async fn init_server_only() -> anyhow::Result<(JoinHandle<()>, String)> {
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
        Ok((server_thread, url))
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
            .exists(&jobs.first().unwrap().queue, &jobs.first().unwrap().id)
            .await?;
        assert!(exists);

        let mut j = client
            .get::<(), i32>(&jobs.first().unwrap().queue, &jobs.first().unwrap().id)
            .await?
            .unwrap();
        assert!(j.updated_at >= now);
        assert_eq!(&jobs.first().unwrap().id, j.id.as_str());
        assert_eq!(&jobs.first().unwrap().queue, j.queue.as_str());
        assert_eq!(&jobs.first().unwrap().timeout, &j.timeout);

        let mut jobs = client
            .poll::<(), i32>(&jobs.first().unwrap().queue, 10)
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

        let j = client.get::<(), i32>(&j2.queue, &j2.id).await?.unwrap();
        assert_eq!(j.state, Some(3));

        client
            .complete(&j2.queue, &j2.id, &j2.run_id.unwrap())
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_requeue_v2() -> anyhow::Result<()> {
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
            .get::<(), i32>(&jobs.first().unwrap().queue, &jobs.first().unwrap().id)
            .await?
            .unwrap();
        assert!(j.updated_at >= now);
        assert_eq!(&jobs.first().unwrap().id, j.id.as_str());
        assert_eq!(&jobs.first().unwrap().queue, j.queue.as_str());
        assert_eq!(&jobs.first().unwrap().timeout, &j.timeout);

        let mut polled_jobs = client
            .poll::<(), i32>(&jobs.first().unwrap().queue, 10)
            .await?;
        assert_eq!(polled_jobs.len(), 1);

        let j2 = polled_jobs.pop().unwrap();
        j.updated_at = j2.updated_at;
        j.run_id = j2.run_id;
        assert_eq!(j2, j);
        assert!(j2.run_id.is_some());

        jobs.first_mut().unwrap().run_at = Some(
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

        let mut j = client.get::<(), i32>(&j2.queue, &j2.id).await?.unwrap();
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
            .poll::<(), i32>(&jobs.first().unwrap().queue, 10)
            .await?;
        assert_eq!(polled_jobs.len(), 1);
        assert_eq!(polled_jobs.first().unwrap().state, None);

        let j2 = client
            .get::<(), i32>(&jobs.first().unwrap().queue, &jobs.first().unwrap().id)
            .await?
            .unwrap();
        assert!(j2.run_id.is_some());
        assert!(j2.state.is_none());

        // test replacing the job and seeing the run_id + state change
        jobs.first_mut().unwrap().state = Some(3);
        client.enqueue(EnqueueMode::Replace, &jobs).await?; // should not error

        let j2 = client
            .get::<(), i32>(&jobs.first().unwrap().queue, &jobs.first().unwrap().id)
            .await?
            .unwrap();
        assert!(j2.run_id.is_none());
        assert_eq!(j2.state, Some(3));

        // test ignoring, new state should not be written
        jobs.first_mut().unwrap().state = Some(4);
        client.enqueue(EnqueueMode::Ignore, &jobs).await?; // should not error

        let j2 = client
            .get::<(), i32>(&jobs.first().unwrap().queue, &jobs.first().unwrap().id)
            .await?
            .unwrap();
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
                .exists(&jobs.first().unwrap().queue, &jobs.first().unwrap().id)
                .await?
        );

        client
            .delete(&jobs.first().unwrap().queue, &jobs.first().unwrap().id)
            .await?;

        assert!(
            !client
                .exists(&jobs.first().unwrap().queue, &jobs.first().unwrap().id)
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
            .poll::<(), i32>(&jobs.first().unwrap().queue, 10)
            .await?;
        assert_eq!(polled_jobs.len(), 1);
        assert!(
            client
                .exists(&jobs.first().unwrap().queue, &jobs.first().unwrap().id)
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
                .exists(&jobs.first().unwrap().queue, &jobs.first().unwrap().id)
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

    #[tokio::test]
    async fn test_oneshot_job_v1() -> anyhow::Result<()> {
        let (_srv, base_url) = init_server_only().await?;

        let client = reqwest::ClientBuilder::default()
            .timeout(Duration::from_secs(60))
            .connect_timeout(Duration::from_secs(10))
            .tcp_keepalive(Duration::from_secs(60))
            .http2_keep_alive_timeout(Duration::from_secs(60))
            .http2_keep_alive_interval(Duration::from_secs(5))
            .http2_keep_alive_while_idle(true)
            .pool_max_idle_per_host(512)
            .build()
            .expect("valid default HTTP Client configuration");

        let now = Utc::now()
            .duration_trunc(chrono::Duration::milliseconds(1))
            .unwrap();
        let job: OldV1<(), i32> = OldV1 {
            id: Uuid::new_v4().to_string(),
            queue: Uuid::new_v4().to_string(),
            timeout: 30,
            max_retries: -1,
            payload: (),
            state: None,
            updated_at: None,
            run_at: Some(now),
        };
        let jobs = vec![job];

        let url = format!("{base_url}/v1/queues/jobs");
        let resp = client.post(&url).json(&jobs).send().await?;
        assert_eq!(resp.status(), StatusCode::ACCEPTED);

        let url = format!(
            "{base_url}/v1/queues/{}/jobs/{}",
            &jobs.first().unwrap().queue,
            &jobs.first().unwrap().id
        );
        let resp = client.head(&url).send().await?;
        assert_eq!(resp.status(), StatusCode::OK);

        let url = format!(
            "{base_url}/v1/queues/{}/jobs/{}",
            &jobs.first().unwrap().queue,
            &jobs.first().unwrap().id
        );
        let mut j: OldV1<(), i32> = client.get(&url).send().await?.json().await?;
        assert!(j.updated_at.unwrap() >= now);
        assert_eq!(&jobs.first().unwrap().id, j.id.as_str());
        assert_eq!(&jobs.first().unwrap().queue, j.queue.as_str());
        assert_eq!(&jobs.first().unwrap().timeout, &j.timeout);

        let url = format!("{base_url}/v1/queues/{}/jobs?num_jobs=10", &j.queue);
        let mut jobs: Vec<OldV1<(), i32>> = client.get(&url).send().await?.json().await?;
        assert_eq!(jobs.len(), 1);

        let j2 = jobs.pop().unwrap();
        j.updated_at = j2.updated_at;
        assert_eq!(j2, j);

        let url = format!("{base_url}/v1/queues/{}/jobs/{}", &j2.queue, &j2.id);
        client.patch(&url).json(&Some(3)).send().await?;

        let url = format!("{base_url}/v1/queues/{}/jobs/{}", &j2.queue, &j2.id);
        let j: OldV1<(), i32> = client.get(&url).send().await?.json().await?;
        assert_eq!(j.state, Some(3));

        let url = format!("{base_url}/v1/queues/{}/jobs/{}", &j2.queue, &j2.id);
        client.delete(&url).send().await?;

        let url = format!("{base_url}/v1/queues/{}/jobs/{}", &j2.queue, &j2.id);
        let resp = client.head(&url).send().await?;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        Ok(())
    }

    #[tokio::test]
    async fn test_reschedule_job_v1() -> anyhow::Result<()> {
        let (_srv, base_url) = init_server_only().await?;

        let client = reqwest::ClientBuilder::default()
            .timeout(Duration::from_secs(60))
            .connect_timeout(Duration::from_secs(10))
            .tcp_keepalive(Duration::from_secs(60))
            .http2_keep_alive_timeout(Duration::from_secs(60))
            .http2_keep_alive_interval(Duration::from_secs(5))
            .http2_keep_alive_while_idle(true)
            .pool_max_idle_per_host(512)
            .build()
            .expect("valid default HTTP Client configuration");

        let now = Utc::now()
            .duration_trunc(chrono::Duration::milliseconds(1))
            .unwrap();
        let job: OldV1<(), i32> = OldV1 {
            id: Uuid::new_v4().to_string(),
            queue: Uuid::new_v4().to_string(),
            timeout: 30,
            max_retries: -1,
            payload: (),
            state: None,
            updated_at: None,
            run_at: Some(now),
        };
        let mut jobs = vec![job];

        let url = format!("{base_url}/v1/queues/jobs");
        let resp = client.post(&url).json(&jobs).send().await?;
        assert_eq!(resp.status(), StatusCode::ACCEPTED);

        let url = format!(
            "{base_url}/v1/queues/{}/jobs/{}",
            &jobs.first().unwrap().queue,
            &jobs.first().unwrap().id
        );
        let resp = client.head(&url).send().await?;
        assert_eq!(resp.status(), StatusCode::OK);

        let url = format!(
            "{base_url}/v1/queues/{}/jobs/{}",
            &jobs.first().unwrap().queue,
            &jobs.first().unwrap().id
        );
        let mut j: OldV1<(), i32> = client.get(&url).send().await?.json().await?;
        assert!(j.updated_at.unwrap() >= now);
        assert_eq!(&jobs.first().unwrap().id, j.id.as_str());
        assert_eq!(&jobs.first().unwrap().queue, j.queue.as_str());
        assert_eq!(&jobs.first().unwrap().timeout, &j.timeout);

        let url = format!("{base_url}/v1/queues/{}/jobs?num_jobs=10", &j.queue);
        let mut polled_jobs: Vec<OldV1<(), i32>> = client.get(&url).send().await?.json().await?;
        assert_eq!(polled_jobs.len(), 1);

        let j2 = polled_jobs.pop().unwrap();
        j.updated_at = j2.updated_at;
        assert_eq!(j2, j);

        let now = Utc::now()
            .add(chrono::Duration::days(1))
            .duration_trunc(chrono::Duration::milliseconds(1))
            .unwrap();
        jobs.first_mut().unwrap().state = Some(4);
        jobs.first_mut().unwrap().run_at = Some(now);
        let url = format!("{base_url}/v1/queues/jobs");
        client.put(&url).json(&jobs.first().unwrap()).send().await?;

        let url = format!("{base_url}/v1/queues/{}/jobs/{}", &j2.queue, &j2.id);
        let j: OldV1<(), i32> = client.get(&url).send().await?.json().await?;
        assert_eq!(j.state, Some(4));
        assert_eq!(j.run_at.unwrap(), now);

        let url = format!("{base_url}/v1/queues/{}/jobs/{}", &j2.queue, &j2.id);
        client.delete(&url).send().await?;

        let url = format!("{base_url}/v1/queues/{}/jobs/{}", &j2.queue, &j2.id);
        let resp = client.head(&url).send().await?;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        Ok(())
    }

    #[tokio::test]
    async fn test_enqueue_modes_job_v1() -> anyhow::Result<()> {
        let (_srv, base_url) = init_server_only().await?;

        let client = reqwest::ClientBuilder::default()
            .timeout(Duration::from_secs(60))
            .connect_timeout(Duration::from_secs(10))
            .tcp_keepalive(Duration::from_secs(60))
            .http2_keep_alive_timeout(Duration::from_secs(60))
            .http2_keep_alive_interval(Duration::from_secs(5))
            .http2_keep_alive_while_idle(true)
            .pool_max_idle_per_host(512)
            .build()
            .expect("valid default HTTP Client configuration");

        let now = Utc::now()
            .duration_trunc(chrono::Duration::milliseconds(1))
            .unwrap();
        let job: OldV1<(), i32> = OldV1 {
            id: Uuid::new_v4().to_string(),
            queue: Uuid::new_v4().to_string(),
            timeout: 30,
            max_retries: -1,
            payload: (),
            state: None,
            updated_at: None,
            run_at: Some(now),
        };
        let mut jobs = vec![job];

        let url = format!("{base_url}/v1/queues/jobs");
        let resp = client.post(&url).json(&jobs).send().await?;
        assert_eq!(resp.status(), StatusCode::ACCEPTED);

        let resp = client.post(&url).json(&jobs).send().await?;
        assert_eq!(resp.status(), StatusCode::CONFLICT);

        // test sending more than one and will be ignored
        let job = jobs.pop().unwrap();
        let jobs = vec![job.clone(), job.clone()];

        let resp = client.post(&url).json(&jobs).send().await?;
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        Ok(())
    }

    #[tokio::test]
    async fn test_poller() -> anyhow::Result<()> {
        let (_srv, client) = init_server().await?;
        let now = Utc::now()
            .duration_trunc(chrono::Duration::milliseconds(1))
            .unwrap();
        let queue = Uuid::new_v4().to_string();
        let j: New<i32, i32> = New {
            id: Uuid::new_v4().to_string(),
            queue: queue.clone(),
            timeout: PositiveI32::new(30).unwrap(),
            max_retries: None,
            payload: 3,
            state: None,
            run_at: Some(now),
        };
        let j2: New<i32, i32> = New {
            id: Uuid::new_v4().to_string(),
            queue: queue.clone(),
            timeout: PositiveI32::new(30).unwrap(),
            max_retries: None,
            payload: 4,
            state: None,
            run_at: Some(now),
        };
        let mut jobs = vec![j, j2];

        client.enqueue(EnqueueMode::Unique, &jobs).await?;

        struct mockRunner {
            helpers: Arc<Mutex<Vec<JobHelper<i32, i32>>>>,
            done: tokio::sync::mpsc::Sender<()>,
        }

        #[async_trait]
        impl Runner<i32, i32> for mockRunner {
            async fn run(&self, helper: JobHelper<i32, i32>) {
                let mut l = 0;
                {
                    let mut lock = self.helpers.lock().unwrap();
                    lock.push(helper);
                    l = lock.len();
                }
                if l == 2 {
                    self.done.send(()).await.unwrap();
                }
            }
        }

        let helpers = Arc::new(Mutex::new(Vec::new()));
        let (tx, mut rx) = tokio::sync::mpsc::channel::<()>(1);
        let mr = mockRunner {
            helpers: helpers.clone(),
            done: tx,
        };
        let p = client.poller(&queue, mr).num_workers(1).build().unwrap();
        let r = p
            .start(async move {
                let _ = rx.recv().await;
            })
            .await;
        assert!(r.is_ok());

        let helpers = helpers.lock().unwrap();
        assert_eq!(helpers.len(), 2);

        let jh1 = helpers.first().unwrap();
        let jh2 = helpers.last().unwrap();
        assert_eq!(jh1.job().queue, queue);
        assert_eq!(jh1.job().id, jobs.first().unwrap().id);
        assert_eq!(jh1.job().payload, jobs.first().unwrap().payload);
        assert_eq!(jh2.job().queue, queue);
        assert_eq!(jh2.job().id, jobs.last().unwrap().id);
        assert_eq!(jh2.job().payload, jobs.last().unwrap().payload);

        jh2.complete().await?;
        assert!(!jh2.exists().await?);
        jh1.heartbeat(Some(5)).await?;

        let maybe_ej: Option<Existing<i32, i32>> = jh1
            .inner_client()
            .get(&jh1.job().queue, &jh1.job().id)
            .await?;
        assert!(maybe_ej.is_some());

        let existing = maybe_ej.unwrap();
        assert_eq!(existing.state, Some(5));
        assert_eq!(existing.run_id, jh1.job().run_id);

        jobs.first_mut().unwrap().state = Some(6);
        jobs.truncate(1);

        jh1.requeue(EnqueueMode::Unique, &jobs).await?;

        let maybe_ej: Option<Existing<i32, i32>> = jh1
            .inner_client()
            .get(&jh1.job().queue, &jh1.job().id)
            .await?;
        assert!(maybe_ej.is_some());

        let existing = maybe_ej.unwrap();
        assert_eq!(existing.state, Some(6));
        assert!(existing.run_id.is_none());

        jh1.delete().await?;
        assert!(!jh1.exists().await?);
        Ok(())
    }
}
