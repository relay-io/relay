use axum::body::{Body, BoxBody};
use axum::extract::{Path, Query, State};
use axum::http::{Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, head, patch, post, put};
use axum::{Json, Router};
use metrics::increment_counter;
use relay_core::job::EnqueueMode;
use relay_postgres::{Job, NewJob, PgStore};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tower_http::trace::TraceLayer;
use tracing::{info, span, Level, Span};
use uuid::Uuid;

/// The internal HTTP server representation for Jobs.
pub struct Server;

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
        // TODO: Can in-flight be replaced with the run_id, where NULL = in-flight = false, else true
        //
        // POST /v1/queues/jobs - accept optional query param for mode of operation?
        //
        // GET  /v1/queues/:queue/jobs/:id
        // HEAD /v1/queues/:queue/jobs/:id
        // DELETE /v1/queues/:queue/jobs/:id - delete
        // DELETE /v1/queues/:queue/jobs/:id/run_id/:run_id - complete
        // PUT  /v1/queues/:queue/jobs/:id/run_id/:run_id - Reschedule + mode
        // PATCH /v1/queues/:queue/jobs/:id/run_id/:run_id - updates state + updated_at + expires_at only
        Router::new()
            // .route("/v1/queues/jobs", post(enqueue))
            // .route("/v1/queues/jobs", put(reschedule))
            // .route("/v1/queues/:queue/jobs", get(next))
            // .route("/v1/queues/:queue/jobs/:id", head(exists))
            // .route("/v1/queues/:queue/jobs/:id", get(get_job))
            // .route("/v1/queues/:queue/jobs/:id", patch(heartbeat))
            // .route("/v1/queues/:queue/jobs/:id", delete(delete_job))
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
