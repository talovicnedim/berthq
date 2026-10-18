//! Getting messages for a consumer and settling them: consume, ack, nack, extend.

use std::time::Duration;

use reqwest::Method;
use serde::{Deserialize, Serialize};

use crate::{Client, REQUEST_TIMEOUT, Result, queue_path};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumeOptions {
    /// Whose position to use; `default` when missing. Created if it doesn't exist.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consumer: Option<String>,
    /// Where a new consumer starts: `start`, `end`, an RFC 3339 time or a message id.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from: Option<String>,
    /// Most messages to return, 1-100 (default 1).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<u32>,
    /// Long-poll: wait up to this long for a message (default 0, at most 60 s).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wait_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub visibility_timeout_ms: Option<u64>,
    /// No lease: the position moves now and nothing is retried.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub auto_ack: bool,
    /// Name shown in the dashboard.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker: Option<String>,
    /// Leases to ack before new messages are handed out (up to 1000): one request instead of
    /// a separate `ack`.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub ack: Vec<String>,
}

/// What `consume` returns: new messages, and what happened to the `ack` leases.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Consumed {
    pub messages: Vec<Delivery>,
    /// How many `ack` leases were settled.
    pub settled: u32,
    /// `ack` leases that no longer exist (already settled, expired, or replayed).
    #[serde(default)]
    pub unknown: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Delivery {
    /// The same on every redelivery and replay: use it as an idempotency key.
    pub id: String,
    /// Pass to `ack`, `nack` or `extend`; `None` with `auto_ack`.
    pub lease: Option<String>,
    pub queue: String,
    pub consumer: String,
    pub body: serde_json::Value,
    pub priority: u8,
    /// 1 on the first delivery to this consumer.
    pub attempt: u32,
    pub published_at: String,
    pub lease_expires_at: Option<String>,
    /// Length of this lease (0 with `auto_ack`).
    pub visibility_timeout_ms: u64,
    pub dedup_id: Option<String>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NackOptions {
    pub leases: Vec<String>,
    /// Retry after this long instead of the queue's backoff.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delay_ms: Option<u64>,
    /// Dead-letter right away.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub dead_letter: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Settled {
    pub settled: u32,
    /// Leases that no longer exist (already settled, expired and redelivered, or replayed).
    pub unknown: Vec<String>,
}

impl Client {
    /// Gets messages for a consumer, after acking `options.ack`. With `wait_ms`, waits for one
    /// to arrive (long-polling); the acks are saved before the wait.
    pub async fn consume(&self, queue: &str, options: &ConsumeOptions) -> Result<Consumed> {
        // The request may legitimately take as long as the wait, plus time to answer.
        let timeout = Duration::from_millis(options.wait_ms.unwrap_or(0)) + REQUEST_TIMEOUT;
        let request = self
            .request(Method::POST, &queue_path(queue, "/consume"))
            .timeout(timeout)
            .json(options);
        self.execute(request).await
    }

    pub async fn ack(&self, queue: &str, leases: &[String]) -> Result<Settled> {
        let body = Leases {
            leases,
            visibility_timeout_ms: None,
        };
        self.send(Method::POST, &queue_path(queue, "/ack"), Some(&body))
            .await
    }

    pub async fn nack(&self, queue: &str, options: &NackOptions) -> Result<Settled> {
        self.send(Method::POST, &queue_path(queue, "/nack"), Some(options))
            .await
    }

    /// Restarts the lease clock of messages that take long to process.
    pub async fn extend(
        &self,
        queue: &str,
        leases: &[String],
        visibility_timeout_ms: Option<u64>,
    ) -> Result<Settled> {
        let body = Leases {
            leases,
            visibility_timeout_ms,
        };
        self.send(Method::POST, &queue_path(queue, "/extend"), Some(&body))
            .await
    }
}

/// The body of `ack` and `extend`.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Leases<'a> {
    leases: &'a [String],
    #[serde(skip_serializing_if = "Option::is_none")]
    visibility_timeout_ms: Option<u64>,
}
