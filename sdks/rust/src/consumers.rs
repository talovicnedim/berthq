//! Consumers: names with a saved position in a queue, and their workers.

use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{Client, Result, encode, field, queue_path};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerInfo {
    pub queue: String,
    pub name: String,
    pub created_at: String,
    pub paused: bool,
    pub stats: ConsumerStats,
    /// No worker for an hour while it still holds messages.
    pub idle: bool,
    pub last_seen: Option<String>,
    pub workers: Vec<WorkerInfo>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerStats {
    pub waiting: u64,
    pub processing: u64,
    pub retrying: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerInfo {
    pub name: String,
    pub last_seen: String,
    pub waiting: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ConsumerMessageState {
    Waiting,
    Processing,
    Retrying,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerMessage {
    pub id: String,
    pub queue: String,
    pub consumer: String,
    pub state: ConsumerMessageState,
    pub priority: u8,
    pub attempts: u32,
    pub published_at: String,
    pub last_error: Option<String>,
    pub worker: Option<String>,
    pub lease_expires_at: Option<String>,
    pub retry_at: Option<String>,
    pub size: u32,
}

impl Client {
    pub async fn consumers_list(&self, queue: &str) -> Result<Vec<ConsumerInfo>> {
        let response: Value = self
            .send(Method::GET, &queue_path(queue, "/consumers"), None::<&()>)
            .await?;
        field(response, "consumers")
    }

    pub async fn consumers_get(&self, queue: &str, consumer: &str) -> Result<ConsumerInfo> {
        self.send(
            Method::GET,
            &consumer_path(queue, consumer, ""),
            None::<&()>,
        )
        .await
    }

    /// Creates a consumer starting at `from` (`start`, `end`, an RFC 3339 time or a message
    /// id; `start` when `None`).
    pub async fn consumers_create(
        &self,
        queue: &str,
        consumer: &str,
        from: Option<&str>,
    ) -> Result<ConsumerInfo> {
        let body = json!({ "name": consumer, "from": from });
        self.send(Method::POST, &queue_path(queue, "/consumers"), Some(&body))
            .await
    }

    pub async fn consumers_remove(&self, queue: &str, consumer: &str) -> Result<()> {
        self.send_empty(Method::DELETE, &consumer_path(queue, consumer, ""))
            .await
    }

    /// Moves a consumer's position to `from`; what it was processing and retrying is dropped.
    pub async fn consumers_replay(
        &self,
        queue: &str,
        consumer: &str,
        from: &str,
    ) -> Result<ConsumerInfo> {
        let body = json!({ "from": from });
        let path = consumer_path(queue, consumer, "/replay");
        self.send(Method::POST, &path, Some(&body)).await
    }

    pub async fn consumers_pause(&self, queue: &str, consumer: &str) -> Result<ConsumerInfo> {
        let path = consumer_path(queue, consumer, "/pause");
        self.send(Method::POST, &path, None::<&()>).await
    }

    pub async fn consumers_resume(&self, queue: &str, consumer: &str) -> Result<ConsumerInfo> {
        let path = consumer_path(queue, consumer, "/resume");
        self.send(Method::POST, &path, None::<&()>).await
    }

    /// What a consumer is doing: messages waiting for it, processing (default) or retrying.
    pub async fn consumers_messages(
        &self,
        queue: &str,
        consumer: &str,
        state: Option<ConsumerMessageState>,
        limit: Option<u32>,
    ) -> Result<Vec<ConsumerMessage>> {
        let mut query = Vec::new();
        if let Some(state) = state {
            let state = match state {
                ConsumerMessageState::Waiting => "waiting",
                ConsumerMessageState::Processing => "processing",
                ConsumerMessageState::Retrying => "retrying",
            };
            query.push(format!("state={state}"));
        }
        if let Some(limit) = limit {
            query.push(format!("limit={limit}"));
        }
        let path = format!(
            "{}?{}",
            consumer_path(queue, consumer, "/messages"),
            query.join("&")
        );
        let response: Value = self.send(Method::GET, &path, None::<&()>).await?;
        field(response, "messages")
    }
}

fn consumer_path(queue: &str, consumer: &str, rest: &str) -> String {
    queue_path(queue, &format!("/consumers/{}{rest}", encode(consumer)))
}
