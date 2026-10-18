//! HTTP/1.1 and HTTP/2 on the same port: cleartext with prior knowledge, and ALPN over TLS.
//!
//! `fixtures/` holds a test-only CA and a `localhost` certificate signed by it, valid until 2126.
//! The keys are P-256 with a named curve: ring rejects keys that spell out the curve parameters.
//! The private key protects nothing real.

// Helpers outside `#[test]` functions aren't covered by clippy's allow-unwrap-in-tests.
#![allow(clippy::unwrap_used)]

use std::net::SocketAddr;
use std::path::PathBuf;

use berthq_core::Broker;
use berthq_server::{ApiKeys, ServerHandle, ServerOptions, router, run, serve, tls_config};
use reqwest::Version;

struct Running {
    address: SocketAddr,
    broker: Broker,
    handle: ServerHandle,
    dir: PathBuf,
}

async fn start(name: &str, tls: bool) -> Running {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let dir = std::env::temp_dir().join(format!("berthq-h2-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let broker = Broker::open(&dir, 0).unwrap();
    let app = router(broker.clone(), ApiKeys::default(), true);
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let tls = match tls {
        true => Some(
            tls_config(fixture("cert.pem"), fixture("key.pem"))
                .await
                .unwrap(),
        ),
        false => None,
    };
    let handle = ServerHandle::new();
    serve(listener, app, tls, handle.clone()).unwrap();
    Running {
        address,
        broker,
        handle,
        dir,
    }
}

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

impl Running {
    async fn stop(self) {
        self.handle.shutdown();
        self.broker.shutdown(std::time::Duration::ZERO).await;
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

#[tokio::test]
async fn cleartext_speaks_http1_and_http2_with_prior_knowledge() {
    let server = start("cleartext", false).await;
    let url = format!("http://{}/healthz", server.address);

    let http1 = reqwest::Client::new().get(&url).send().await.unwrap();
    assert_eq!(http1.version(), Version::HTTP_11);

    let client = reqwest::Client::builder()
        .http2_prior_knowledge()
        .build()
        .unwrap();
    let http2 = client.get(&url).send().await.unwrap();
    assert_eq!(http2.version(), Version::HTTP_2);
    assert_eq!(http2.text().await.unwrap(), "ok");

    let created = client
        .post(format!("http://{}/v1/queues", server.address))
        .body(r#"{"name":"jobs"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), reqwest::StatusCode::CREATED);
    server.stop().await;
}

#[tokio::test]
async fn tls_picks_http2_through_alpn() {
    let server = start("tls", true).await;
    let ca = std::fs::read(fixture("ca.pem")).unwrap();
    let client = reqwest::Client::builder()
        .tls_certs_only([reqwest::Certificate::from_pem(&ca).unwrap()])
        .resolve("localhost", server.address)
        .build()
        .unwrap();
    let url = format!("https://localhost:{}/healthz", server.address.port());
    let response = client.get(&url).send().await.unwrap();
    assert_eq!(response.version(), Version::HTTP_2);
    assert_eq!(response.text().await.unwrap(), "ok");

    let http1_only = reqwest::Client::builder()
        .tls_certs_only([reqwest::Certificate::from_pem(&ca).unwrap()])
        .resolve("localhost", server.address)
        .http1_only()
        .build()
        .unwrap();
    let response = http1_only.get(&url).send().await.unwrap();
    assert_eq!(response.version(), Version::HTTP_11);
    server.stop().await;
}

#[tokio::test]
async fn a_key_that_cannot_be_read_stops_the_start_instead_of_hanging() {
    let dir = std::env::temp_dir().join(format!("berthq-h2-badkey-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let key = dir.join("key.pem");
    std::fs::write(
        &key,
        "-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n",
    )
    .unwrap();
    let options = ServerOptions {
        host: "127.0.0.1".to_string(),
        port: 0,
        data_dir: dir.join("data"),
        min_free_disk: 0,
        shutdown_timeout: std::time::Duration::ZERO,
        api_keys: ApiKeys::default(),
        no_auth: false,
        tls: Some((fixture("cert.pem"), key)),
    };
    let started = tokio::time::timeout(std::time::Duration::from_secs(10), run(options)).await;
    let err = started.expect("run must not hang").unwrap_err();
    assert!(err.contains("tls certificate or key"), "{err}");
    let _ = std::fs::remove_dir_all(&dir);
}
