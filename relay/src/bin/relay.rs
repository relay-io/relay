//! Relay is a simple, no-nonsense, stateless job runner.
//!
//! A Job runner is like a Relay race, you hand the baton off in turn to be handed off again and so on until the end of the race.
//! The race itself doesn't record the times, video itself or announce the outcomes; but rather those are done externally.
//!
//! To that end Relay is designed to be simple, reliable and easy to use.
//! It on purpose does not:
//! - Track `Job` history.
//! - Keep logs.
//! - Have any notion of success or fail.
//! - Retain Job run information after it completes.
//!
//! Relay embraces the unix philosophy of doing one thing and doing it well leaving the above as optional features to be handled
//! by the callers and or clients.

#[allow(unused_imports)]
use anyhow::Context;
use clap::Parser;
use std::env;
use std::io::IsTerminal;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use relay_postgres::PgStore;
#[cfg(unix)]
use tokio::signal::unix::{signal, SignalKind};

/// Relay command line options.
#[derive(Debug, Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"), author = env!("CARGO_PKG_AUTHORS"), about = env!("CARGO_PKG_DESCRIPTION"))]
pub struct Opts {
    /// HTTP Port to bind to, default 8080.
    #[cfg(feature = "frontend-http")]
    #[clap(long, default_value = "8080", env = "HTTP_PORT")]
    pub http_port: String,

    /// Metrics Port to bind to, default 5001.
    #[cfg(feature = "metrics-prometheus")]
    #[clap(long, default_value = "5001", env = "METRICS_PORT")]
    pub metrics_port: String,

    /// DATABASE URL to connect to.
    #[cfg(feature = "backend-postgres")]
    #[clap(
        long,
        default_value = "postgres://username:pass@localhost:5432/relay?sslmode=disable",
        env = "DATABASE_URL"
    )]
    pub database_url: String,

    /// Maximum allowed database connections, default 10.
    #[cfg(feature = "backend-postgres")]
    #[clap(long, default_value = "10", env = "DATABASE_MAX_CONNECTIONS")]
    pub database_max_connections: usize,

    /// This time interval, in seconds, between runs checking for retries and failed jobs.
    #[clap(long, default_value = "5", env = "REAP_INTERVAL")]
    pub reap_interval: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    match env::var("RUST_LOG") {
        Err(_) => env::set_var("RUST_LOG", "info"),
        Ok(v) => {
            if v.trim() == "" {
                env::set_var("RUST_LOG", "info");
            }
        }
    };

    // install global collector configured based on RUST_LOG env var.
    let (non_blocking, _guard) = tracing_appender::non_blocking(std::io::stdout());
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_ansi(std::io::stdout().is_terminal())
        .with_writer(non_blocking)
        .init();

    let opts: Opts = Opts::parse();

    #[cfg(feature = "metrics-prometheus")]
    metrics_exporter_prometheus::PrometheusBuilder::new()
        .with_http_listener(
            format!("0.0.0.0:{}", &opts.metrics_port)
                .parse::<std::net::SocketAddr>()
                .context("invalid prometheus address")?,
        )
        .idle_timeout(
            metrics_util::MetricKindMask::COUNTER | metrics_util::MetricKindMask::HISTOGRAM,
            Some(Duration::from_secs(30)),
        )
        .add_global_label("app", "relay_rs")
        .install()
        .context("failed to install Prometheus recorder")?;

    #[cfg(feature = "backend-postgres")]
    let backend = Arc::new(init_backend(&opts).await?);
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let reap_be = backend.clone();
    let reaper = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(opts.reap_interval));
        interval.reset();
        tokio::pin!(shutdown_rx);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(e) = reap_be.reap(opts.reap_interval).await {
                        error!("error occurred reaping jobs. {}", e.to_string());
                    }
                    interval.reset();
                },
                _ = (&mut shutdown_rx) => break
            }
        }
    });

    #[cfg(feature = "frontend-http")]
    relay_http::server::Server::run(
        backend,
        &format!("0.0.0.0:{}", opts.http_port),
        shutdown_signal(),
    )
    .await?;

    drop(shutdown_tx);

    reaper.await?;
    info!("Reaper shutdown");
    info!("Application gracefully shutdown");
    Ok(())
}

#[cfg(feature = "backend-postgres")]
async fn init_backend(opts: &Opts) -> anyhow::Result<PgStore> {
    relay_postgres::PgStore::new(&opts.database_url, opts.database_max_connections).await
}

/// Tokio signal handler that will wait for a user to press CTRL+C.
/// We use this in our hyper `Server` method `with_graceful_shutdown`.
#[cfg(unix)]
async fn shutdown_signal() {
    let mut interrupt = signal(SignalKind::interrupt()).expect("Expect shutdown signal");
    let mut terminate = signal(SignalKind::terminate()).expect("Expect shutdown signal");
    let mut hangup = signal(SignalKind::hangup()).expect("Expect shutdown signal");
    let mut quit = signal(SignalKind::quit()).expect("Expect shutdown signal");

    tokio::select! {
        _ = interrupt.recv() => info!("Received SIGINT"),
        _ = terminate.recv() => info!("Received SIGTERM"),
        _ = hangup.recv() => info!("Received SIGHUP"),
        _ = quit.recv() => info!("Received SIGQUIT"),
    }
    info!("received shutdown signal");
}

/// Tokio signal handler that will wait for a user to press CTRL+C.
/// We use this in our hyper `Server` method `with_graceful_shutdown`.
#[cfg(windows)]
async fn shutdown_signal() {
    tokio::signal::ctrl_c().await.expect("Shutdown signal");
    println!("received shutdown signal");
}
