//! Listening: HTTP/1.1 and HTTP/2 on one port, optional TLS, and stopping on a signal.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use axum::Router;
use axum_server::Server;
pub use axum_server::tls_rustls::RustlsConfig;
use hyper_util::rt::TokioTimer;
use tokio::task::JoinHandle;

use crate::opening::OpeningAcceptor;

/// A client gets this long to send a request head, timed from when Berthq starts waiting for
/// one, so it is also how long an idle HTTP/1.1 connection stays open. An HTTP/2 client quiet
/// for this long gets a ping and must answer it in time. Bodies and answers (uploads,
/// long-polls, `/v1/events`) have no limit.
const CLIENT_TIMEOUT: Duration = Duration::from_secs(30);

/// Stops a server started with [`serve`].
pub type ServerHandle = axum_server::Handle<SocketAddr>;

/// Loads a PEM certificate chain and private key for [`serve`].
pub async fn tls_config(cert: PathBuf, key: PathBuf) -> Result<RustlsConfig, String> {
    RustlsConfig::from_pem_file(cert, key)
        .await
        .map_err(|err| format!("failed to load the tls certificate or key: {err}"))
}

/// Serves `app` on a bound listener until `handle` shuts it down, and returns the server task.
/// The listener already accepts connections, so there is nothing to wait for. HTTP/1.1 and
/// HTTP/2 share the port: without TLS a client picks HTTP/2 with prior knowledge, with TLS
/// through ALPN.
pub fn serve(
    listener: std::net::TcpListener,
    app: Router,
    tls: Option<RustlsConfig>,
    handle: ServerHandle,
) -> Result<JoinHandle<std::io::Result<()>>, String> {
    let failed = |err: std::io::Error| format!("failed to use the listener: {err}");
    listener.set_nonblocking(true).map_err(failed)?;
    let service = app.into_make_service();
    Ok(match tls {
        Some(config) => {
            let server = axum_server::from_tcp_rustls(listener, config).map_err(failed)?;
            tokio::spawn(with_timeouts(server).handle(handle).serve(service))
        }
        None => {
            let server = axum_server::from_tcp(listener).map_err(failed)?;
            tokio::spawn(with_timeouts(server).handle(handle).serve(service))
        }
    })
}

/// Drops clients that hold a connection without sending a request, so they can't use up the
/// open files.
fn with_timeouts<A>(server: Server<SocketAddr, A>) -> Server<SocketAddr, OpeningAcceptor<A>> {
    let mut server = server.map(|inner| OpeningAcceptor {
        inner,
        timeout: CLIENT_TIMEOUT,
    });
    let builder = server.http_builder();
    builder
        .http1()
        .timer(TokioTimer::new())
        .header_read_timeout(CLIENT_TIMEOUT);
    builder
        .http2()
        .timer(TokioTimer::new())
        .keep_alive_interval(CLIENT_TIMEOUT)
        .keep_alive_timeout(CLIENT_TIMEOUT);
    server
}

/// The address to listen on; host names like `localhost` are resolved.
pub(crate) fn resolve(host: &str, port: u16) -> Result<SocketAddr, String> {
    if let Ok(address) = format!("{host}:{port}").parse() {
        return Ok(address);
    }
    std::net::ToSocketAddrs::to_socket_addrs(&(host, port))
        .ok()
        .and_then(|mut addresses| addresses.next())
        .ok_or_else(|| format!("invalid host {host}"))
}

/// Refuses an open API on a network address unless the user said the network is private.
pub(crate) fn check_open(address: SocketAddr, no_keys: bool, no_auth: bool) -> Result<(), String> {
    if !no_keys || address.ip().is_loopback() {
        return Ok(());
    }
    if no_auth {
        tracing::warn!(
            "listening on {address} without api keys (--no-auth): anyone who can reach this \
             port can read and delete messages"
        );
        return Ok(());
    }
    Err(format!(
        "refusing to listen on {address} without api keys: anyone who can reach it could read \
         and delete messages. Set --api-key (or BERTHQ_API_KEYS), or pass --no-auth (or \
         BERTHQ_NO_AUTH=true) if this network is private"
    ))
}

pub(crate) async fn shutdown_signal() {
    let interrupt = async {
        if let Err(err) = tokio::signal::ctrl_c().await {
            tracing::error!("failed to listen for ctrl-c: {err}");
            std::future::pending::<()>().await;
        }
    };
    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(err) => {
                tracing::error!("failed to listen for SIGTERM: {err}");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = interrupt => {}
        () = terminate => {}
    }
}

#[cfg(test)]
#[path = "serve_tests.rs"]
mod tests;
