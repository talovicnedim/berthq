//! Rust client for [Berthq](https://github.com/talovicnedim/berthq).
//!
//! ```no_run
//! # async fn example() -> Result<(), berthq_client::Error> {
//! use berthq_client::{Client, ConsumeOptions, NewMessage};
//!
//! let client = Client::new("http://127.0.0.1:7331")?;
//! let message = NewMessage::new(serde_json::json!({ "user": 123 }));
//! client.publish("emails", &message).await?;
//!
//! let options = ConsumeOptions { wait_ms: Some(20_000), ..ConsumeOptions::default() };
//! for delivery in client.consume("emails", &options).await?.messages {
//!     // ... send the email ...
//!     if let Some(lease) = delivery.lease {
//!         client.ack("emails", &[lease]).await?;
//!     }
//! }
//! # Ok(())
//! # }
//! ```
//!
//! Method names match the HTTP API and the broker's core: `queues_create`, `publish`,
//! `consume`, `ack`, `consumers_replay`, `requeue`, …
//!
//! For speed, publish through a [`Publisher`] (`client.publisher(queue, …)`), which sends
//! messages in batches, and work with [`Client::process`], which acks with its next consume.

mod acks;
mod backup;
mod consumers;
mod delivery;
mod messages;
mod process;
mod publisher;
mod queues;

use std::time::Duration;

use reqwest::{Method, RequestBuilder, StatusCode};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

pub use consumers::{
    ConsumerInfo, ConsumerMessage, ConsumerMessageState, ConsumerStats, WorkerInfo,
};
pub use delivery::{ConsumeOptions, Consumed, Delivery, NackOptions, Settled};
pub use messages::{
    DeadLetterReason, MessageDeadLetter, MessageInfo, MessageState, NewMessage, Published, Requeued,
};
pub use process::ProcessOptions;
pub use publisher::{Pending, Publisher, PublisherOptions};
pub use queues::{
    Counters, DiskStats, Overview, QueueDetails, QueueInfo, QueueSettings, QueueSettingsPatch,
    QueueStats, StatsPoint,
};

#[derive(Debug, Clone, thiserror::Error)]
pub enum Error {
    /// The broker answered with an error, e.g. `notFound` or `queueFull`.
    #[error("{message}")]
    Api {
        status: u16,
        kind: String,
        message: String,
    },
    /// No connection to the broker: nothing listens there, the host is unknown, or TLS failed.
    #[error("{0}")]
    Unreachable(String),
    /// Any other failure outside the broker: a timeout, an unreadable response, a local file.
    #[error("{0}")]
    Http(String),
}

impl Error {
    /// The API's error kind (`invalid`, `unauthorized`, `forbidden`, `notFound`, `conflict`,
    /// `queueFull`, `diskFull`, `unavailable`, `storage`), or `http` for transport errors.
    pub fn kind(&self) -> &str {
        match self {
            Error::Api { kind, .. } => kind,
            Error::Unreachable(_) | Error::Http(_) => "http",
        }
    }

    /// True when trying again later can help: the broker is shutting down (`unavailable`) or
    /// could not be reached. Worker loops wait and try again.
    pub fn is_temporary(&self) -> bool {
        matches!(self, Error::Unreachable(_) | Error::Http(_)) || self.kind() == "unavailable"
    }
}

pub type Result<T> = std::result::Result<T, Error>;

/// How long a request may take; long-polling consumes get their wait on top.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub struct Client {
    base: String,
    http: reqwest::Client,
    api_key: Option<String>,
}

impl Client {
    /// A client for the broker at `url`, e.g. `http://127.0.0.1:7331`.
    pub fn new(url: &str) -> Result<Client> {
        // reqwest is built without a TLS provider so ring is the only one; an error means the
        // application already installed one.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .build()
            .map_err(|err| Error::Http(err.to_string()))?;
        Ok(Client {
            base: url.trim_end_matches('/').to_string(),
            http,
            api_key: None,
        })
    }

    /// Sends `key` with every request (`Authorization: Bearer <key>`).
    pub fn with_api_key(mut self, key: &str) -> Client {
        self.api_key = Some(key.to_string());
        self
    }

    pub fn url(&self) -> &str {
        &self.base
    }

    async fn send<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        body: Option<&(impl Serialize + ?Sized)>,
    ) -> Result<T> {
        let mut request = self.request(method, path).timeout(REQUEST_TIMEOUT);
        if let Some(body) = body {
            request = request.json(body);
        }
        self.execute(request).await
    }

    /// POSTs a body that is already JSON, so it isn't encoded twice.
    async fn post_json<T: DeserializeOwned>(&self, path: &str, body: Vec<u8>) -> Result<T> {
        let request = self
            .request(Method::POST, path)
            .timeout(REQUEST_TIMEOUT)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body);
        self.execute(request).await
    }

    async fn send_empty(&self, method: Method, path: &str) -> Result<()> {
        let request = self.request(method, path).timeout(REQUEST_TIMEOUT);
        let response = request.send().await.map_err(|err| self.transport(&err))?;
        if response.status().is_success() {
            return Ok(());
        }
        Err(api_error(response).await)
    }

    fn request(&self, method: Method, path: &str) -> RequestBuilder {
        let request = self.http.request(method, format!("{}{path}", self.base));
        match &self.api_key {
            Some(key) => request.bearer_auth(key),
            None => request,
        }
    }

    async fn execute<T: DeserializeOwned>(&self, request: RequestBuilder) -> Result<T> {
        let response = request.send().await.map_err(|err| self.transport(&err))?;
        if !response.status().is_success() {
            return Err(api_error(response).await);
        }
        response
            .json()
            .await
            .map_err(|err| Error::Http(format!("invalid response from the broker: {err}")))
    }

    fn transport(&self, err: &reqwest::Error) -> Error {
        if err.is_connect() {
            // The innermost cause says why: refused, a bad certificate, DNS, …
            let mut cause: &dyn std::error::Error = err;
            while let Some(source) = cause.source() {
                cause = source;
            }
            return Error::Unreachable(format!(
                "could not connect to the broker at {} ({cause})",
                self.base
            ));
        }
        if err.is_timeout() {
            return Error::Http(format!(
                "the broker at {} did not answer in time",
                self.base
            ));
        }
        Error::Http(err.to_string())
    }
}

async fn api_error(response: reqwest::Response) -> Error {
    let status = response.status();
    let body: Value = response.json().await.unwrap_or(Value::Null);
    let kind = body["error"]["kind"].as_str().unwrap_or("http").to_string();
    let message = body["error"]["message"]
        .as_str()
        .map(str::to_string)
        .unwrap_or_else(|| default_message(status));
    Error::Api {
        status: status.as_u16(),
        kind,
        message,
    }
}

fn default_message(status: StatusCode) -> String {
    format!("the broker answered {status}")
}

fn field<T: DeserializeOwned>(mut response: Value, name: &str) -> Result<T> {
    serde_json::from_value(response[name].take())
        .map_err(|err| Error::Http(format!("invalid response from the broker: {err}")))
}

fn queue_path(queue: &str, rest: &str) -> String {
    format!("/v1/queues/{}{rest}", encode(queue))
}

/// Percent-encodes a path segment or query value. Names and ids only use safe characters, but
/// user input shouldn't be able to change the path.
fn encode(segment: &str) -> String {
    segment
        .bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}
