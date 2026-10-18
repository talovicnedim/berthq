//! HTTP behavior that clients rely on: status codes, the JSON error envelope, auth, consumers,
//! a full disk and backups.

// Helpers outside `#[test]` functions aren't covered by clippy's allow-unwrap-in-tests.
#![allow(clippy::unwrap_used)]

use std::path::{Path, PathBuf};
use std::time::Duration;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use berthq_core::{Broker, NewMessage};
use berthq_server::{ApiKeys, router};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;

mod auth;
mod consumers;
mod limits;
mod origin;
mod responses;
mod storage;

struct App {
    router: axum::Router,
    broker: Broker,
    dir: PathBuf,
}

async fn app(name: &str, keys: &[&str]) -> App {
    app_with_disk(name, keys, 0).await
}

/// A `min_free_disk` above any real free space makes the disk count as full.
async fn app_with_disk(name: &str, keys: &[&str], min_free_disk: u64) -> App {
    start(name, keys, min_free_disk, true)
}

/// An open API on a network address (`--no-auth`).
async fn app_on_network(name: &str) -> App {
    start(name, &[], 0, false)
}

fn start(name: &str, keys: &[&str], min_free_disk: u64, loopback: bool) -> App {
    let dir = std::env::temp_dir().join(format!("berthq-api-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let broker = Broker::open(&dir, min_free_disk).unwrap();
    let keys: Vec<String> = keys.iter().map(|k| k.to_string()).collect();
    let router = router(broker.clone(), ApiKeys::parse(&keys).unwrap(), loopback);
    App {
        router,
        broker,
        dir,
    }
}

/// A request: method, path, optional JSON body, optional headers.
struct Call<'a> {
    method: Method,
    uri: &'a str,
    body: Option<Body>,
    headers: Vec<(&'a str, String)>,
}

impl<'a> Call<'a> {
    fn new(method: Method, uri: &'a str) -> Self {
        Self {
            method,
            uri,
            body: None,
            headers: Vec::new(),
        }
    }

    fn json(mut self, body: Value) -> Self {
        self.body = Some(Body::from(body.to_string()));
        self
    }

    /// A request body sent exactly as given, for formatting and bytes `json!` can't produce.
    fn raw(mut self, body: impl Into<Body>) -> Self {
        self.body = Some(body.into());
        self
    }

    fn header(mut self, name: &'a str, value: &str) -> Self {
        self.headers.push((name, value.to_string()));
        self
    }

    fn key(self, key: &str) -> Self {
        self.header("authorization", &format!("Bearer {key}"))
    }
}

impl App {
    async fn send(&self, call: Call<'_>) -> (StatusCode, Vec<u8>) {
        let mut request = Request::builder().method(call.method).uri(call.uri);
        for (name, value) in call.headers {
            request = request.header(name, value);
        }
        let body = call.body.unwrap_or_else(Body::empty);
        let response = self
            .router
            .clone()
            .oneshot(request.body(body).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (status, bytes.to_vec())
    }

    /// Only the status, without reading the body (server-sent events never end).
    async fn status(&self, call: Call<'_>) -> StatusCode {
        let mut request = Request::builder().method(call.method).uri(call.uri);
        for (name, value) in call.headers {
            request = request.header(name, value);
        }
        let body = call.body.unwrap_or_else(Body::empty);
        let response = self
            .router
            .clone()
            .oneshot(request.body(body).unwrap())
            .await;
        response.unwrap().status()
    }

    async fn call(&self, call: Call<'_>) -> (StatusCode, Value) {
        let (status, bytes) = self.send(call).await;
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    async fn queue(&self, name: &str) {
        let (status, _) = self
            .call(Call::new(Method::POST, "/v1/queues").json(json!({ "name": name })))
            .await;
        assert_eq!(status, StatusCode::CREATED);
    }

    /// A few MB of messages, so a backup takes a moment to copy.
    async fn fill(&self, queue: &str) {
        let body = serde_json::value::to_raw_value(&"x".repeat(10_000)).unwrap();
        let messages = (0..500)
            .map(|_| NewMessage {
                body: body.clone(),
                priority: None,
                delay_ms: None,
                deliver_at: None,
                dedup_id: None,
            })
            .collect();
        self.broker.publish_batch(queue, messages).await.unwrap();
    }

    async fn stop(self) {
        self.broker.shutdown(Duration::ZERO).await;
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn backup_files(dir: &Path) -> usize {
    std::fs::read_dir(dir)
        .unwrap()
        .filter(|entry| {
            entry
                .as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("backup-")
        })
        .count()
}

/// Whether `done` turns true within 10 seconds.
async fn eventually(done: impl Fn() -> bool) -> bool {
    let wait = async {
        while !done() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(10), wait)
        .await
        .is_ok()
}
