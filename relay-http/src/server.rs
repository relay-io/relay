use axum::body::{Body, BoxBody};
use axum::extract::{Path, Query, State};
use axum::http::{Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, head, patch, post, put};
use axum::{Json, Router};
use metrics::increment_counter;
use relay_core::job::{EnqueueMode, NewJob};
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

#[tracing::instrument(name = "http_enqueue", level = "debug", skip_all)]
async fn enqueue(
    State(state): State<Arc<PgStore>>,
    params: Query<EnqueueQueryInfo>,
    jobs: Json<Vec<NewJob>>,
) -> Response {
    increment_counter!("http_request", "endpoint" => "enqueue");

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

#[tracing::instrument(name = "http_get", level = "debug", skip_all)]
async fn get_job(
    State(state): State<Arc<PgStore>>,
    Path((queue, id)): Path<(String, String)>,
) -> Response {
    increment_counter!("http_request", "endpoint" => "get", "queue" => queue.clone());

    match state.get(&queue, &id).await {
        Ok(job) => {
            if let Some(job) = job {
                Json(job).into_response()
            } else {
                StatusCode::NOT_FOUND.into_response()
            }
        }
        Err(e) => {
            increment_counter!("errors", "endpoint" => "get", "type" => e.error_type(), "queue" => e.queue());
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

#[tracing::instrument(name = "http_exists", level = "debug", skip_all)]
async fn exists(
    State(state): State<Arc<PgStore>>,
    Path((queue, id)): Path<(String, String)>,
) -> Response {
    increment_counter!("http_request", "endpoint" => "exists", "queue" => queue.clone());

    match state.exists(&queue, &id).await {
        Ok(exists) => {
            if exists {
                StatusCode::OK.into_response()
            } else {
                StatusCode::NOT_FOUND.into_response()
            }
        }
        Err(e) => {
            increment_counter!("errors", "endpoint" => "exists", "type" => e.error_type(), "queue" => e.queue());
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

#[tracing::instrument(name = "http_heartbeat", level = "debug", skip_all)]
async fn heartbeat(
    State(state): State<Arc<PgStore>>,
    Path((queue, id, run_id)): Path<(String, String, Uuid)>,
    job_state: Option<Json<Box<RawValue>>>,
) -> Response {
    increment_counter!("http_request", "endpoint" => "heartbeat", "queue" => queue.clone());

    let job_state = match job_state {
        None => None,
        Some(job_state) => Some(job_state.0),
    };
    if let Err(e) = state
        .heartbeat(&queue, &id, &run_id, job_state.as_deref())
        .await
    {
        increment_counter!("errors", "endpoint" => "heartbeat", "type" => e.error_type(), "queue" => e.queue());
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

#[tracing::instrument(name = "http_reschedule", level = "debug", skip_all)]
async fn reschedule(
    State(state): State<Arc<PgStore>>,
    Path((queue, id, run_id)): Path<(String, String, Uuid)>,
    params: Query<EnqueueQueryInfo>,
    job: Json<NewJob>,
) -> Response {
    increment_counter!("http_request", "endpoint" => "reschedule", "queue" => job.0.queue.clone());

    if let Err(e) = state
        .reschedule(params.0.enqueue_mode(), &queue, &id, &run_id, &job)
        .await
    {
        increment_counter!("errors", "endpoint" => "enqueued", "type" => e.error_type(), "queue" => e.queue());
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

#[tracing::instrument(name = "http_delete", level = "debug", skip_all)]
async fn delete_job(
    State(state): State<Arc<PgStore>>,
    Path((queue, id)): Path<(String, String)>,
) -> Response {
    increment_counter!("http_request", "endpoint" => "delete", "queue" => queue.clone());

    if let Err(e) = state.delete(&queue, &id).await {
        increment_counter!("errors", "endpoint" => "delete", "type" => e.error_type(), "queue" => e.queue());
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

#[tracing::instrument(name = "http_complete", level = "debug", skip_all)]
async fn complete_job(
    State(state): State<Arc<PgStore>>,
    Path((queue, id, run_id)): Path<(String, String, Uuid)>,
) -> Response {
    increment_counter!("http_request", "endpoint" => "delete", "queue" => queue.clone());

    if let Err(e) = state.complete(&queue, &id, &run_id).await {
        increment_counter!("errors", "endpoint" => "delete", "type" => e.error_type(), "queue" => e.queue());
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

#[tracing::instrument(name = "http_next", level = "debug", skip_all)]
async fn next(
    State(state): State<Arc<PgStore>>,
    Path(queue): Path<String>,
    params: Query<NextQueryInfo>,
) -> Response {
    increment_counter!("http_request", "endpoint" => "next", "queue" => queue.clone());

    match state.next(&queue, params.0.num_jobs).await {
        Err(e) => {
            increment_counter!("errors", "endpoint" => "next", "type" => e.error_type(), "queue" => e.queue());
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
            .route("/v1/queues/jobs", post(enqueue))
            .route("/v1/queues/:queue/jobs/:id/run_id/:run_id", put(reschedule))
            .route("/v1/queues/:queue/jobs", get(next))
            .route("/v1/queues/:queue/jobs/:id", head(exists))
            .route("/v1/queues/:queue/jobs/:id", get(get_job))
            .route(
                "/v1/queues/:queue/jobs/:id/run_id/:run_id",
                patch(heartbeat),
            )
            .route("/v1/queues/:queue/jobs/:id", delete(delete_job))
            .route(
                "/v1/queues/:queue/jobs/:id/run_id/:run_id",
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

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::client::{Builder as ClientBuilder, Client};
//     use anyhow::{anyhow, Context};
//     use chrono::DurationRound;
//     use chrono::Utc;
//     use portpicker::pick_unused_port;
//     use relay_postgres::PgStore;
//     use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener};
//     use tokio::task::JoinHandle;
//     use uuid::Uuid;
//
//     /// Generates a `SocketAddr` on the IP 0.0.0.0, using a random port.
//     pub fn new_random_socket_addr() -> anyhow::Result<SocketAddr> {
//         let ip_address = IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0));
//         let port = pick_unused_port().ok_or_else(|| anyhow!("No free port was found"))?;
//         let addr = SocketAddr::new(ip_address, port);
//         Ok(addr)
//     }
//
//     async fn init_server() -> anyhow::Result<(JoinHandle<()>, Arc<Client>)> {
//         let db_url = std::env::var("DATABASE_URL")?;
//         let store = Arc::new(PgStore::default(&db_url).await?);
//         let app = Server::init_app(store);
//
//         let socket_address =
//             new_random_socket_addr().expect("Cannot create socket address for use");
//         let listener = TcpListener::bind(socket_address)
//             .with_context(|| "Failed to create TCPListener for TestServer")?;
//         let server_address = socket_address.to_string();
//         let server = axum::Server::from_tcp(listener)
//             .with_context(|| "Failed to create ::axum::Server for TestServer")?
//             .serve(app.into_make_service());
//
//         let server_thread = tokio::spawn(async move {
//             server.await.expect("Expect server to start serving");
//         });
//
//         let url = format!("http://{server_address}");
//         let client = ClientBuilder::new(&url).build();
//         Ok((server_thread, Arc::new(client)))
//     }
// }
