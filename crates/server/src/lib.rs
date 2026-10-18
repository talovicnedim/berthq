//! Berthq's HTTP server: the JSON API under `/v1`, live stats over server-sent events,
//! Prometheus metrics and the dashboard, all on one port.

mod auth;
mod dashboard;
mod error;
mod json;
mod metrics;
mod opening;
mod origin;
mod routes;
mod serve;

use std::path::PathBuf;
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use berthq_core::Broker;

pub use auth::ApiKeys;
pub use serve::{RustlsConfig, ServerHandle, serve, tls_config};

pub struct ServerOptions {
    pub host: String,
    pub port: u16,
    pub data_dir: PathBuf,
    /// Publishing stops while the disk has fewer free bytes than this; 0 turns that off.
    pub min_free_disk: u64,
    /// How long shutdown waits for leased messages to be acked.
    pub shutdown_timeout: Duration,
    /// Without keys, the API is open; only allowed on a loopback address or with `no_auth`.
    pub api_keys: ApiKeys,
    /// The user says the network is private: allow an open API on any address.
    pub no_auth: bool,
    /// PEM certificate chain and private key; serves HTTPS when set.
    pub tls: Option<(PathBuf, PathBuf)>,
}

/// `loopback`: the server listens on a loopback address, so an open API accepts only loopback
/// host names.
pub fn router(broker: Broker, keys: ApiKeys, loopback: bool) -> Router {
    let open = keys.is_empty();
    // The dashboard's files and the health check stay public; the dashboard asks for a key.
    let mut protected = Router::new()
        .nest("/v1", routes::router())
        .route("/metrics", get(metrics::metrics))
        .layer(axum::middleware::from_fn_with_state(
            keys,
            auth::require_key,
        ));
    if open {
        protected = protected.layer(axum::middleware::from_fn_with_state(
            loopback,
            origin::require_same_origin,
        ));
    }
    Router::new()
        .merge(protected)
        .route("/healthz", get(healthz))
        .fallback(dashboard::serve)
        .with_state(broker)
}

/// 503 once storage has failed, so a supervisor such as the Kubernetes liveness probe restarts
/// Berthq.
async fn healthz(State(broker): State<Broker>) -> Response {
    match broker.storage_failure() {
        Some(failure) => (StatusCode::SERVICE_UNAVAILABLE, failure).into_response(),
        None => "ok".into_response(),
    }
}

/// Runs the broker until SIGINT or SIGTERM, then shuts down gracefully.
pub async fn run(options: ServerOptions) -> Result<(), String> {
    // HTTPS uses rustls with ring; an error means a provider is already set.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let address = serve::resolve(&options.host, options.port)?;
    serve::check_open(address, options.api_keys.is_empty(), options.no_auth)?;
    let tls = match options.tls.clone() {
        Some((cert, key)) => Some(tls_config(cert, key).await?),
        None => None,
    };
    let listener = std::net::TcpListener::bind(address)
        .map_err(|err| format!("failed to listen on {address}: {err}"))?;
    let broker =
        Broker::open(&options.data_dir, options.min_free_disk).map_err(|err| err.to_string())?;
    let loopback = address.ip().is_loopback();
    let app = router(broker.clone(), options.api_keys.clone(), loopback);
    let handle = ServerHandle::new();
    let server = serve(listener, app, tls, handle.clone())?;
    let scheme = if options.tls.is_some() {
        "https"
    } else {
        "http"
    };
    tracing::info!(
        "berthq {} listening on {scheme}://{address} (data in {})",
        env!("CARGO_PKG_VERSION"),
        options.data_dir.display()
    );

    serve::shutdown_signal().await;
    tracing::info!("shutting down");
    // The API keeps serving while the broker drains, so consumers can still ack.
    broker.shutdown(options.shutdown_timeout).await;
    handle.graceful_shutdown(Some(Duration::from_secs(5)));
    match server.await {
        Ok(Ok(())) => {
            tracing::info!("stopped");
            Ok(())
        }
        Ok(Err(err)) => Err(format!("server error: {err}")),
        Err(err) => Err(format!("server task failed: {err}")),
    }
}
