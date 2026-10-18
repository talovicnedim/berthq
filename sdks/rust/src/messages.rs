//! Publishing, browsing and deleting messages, and requeueing dead letters.

use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{Client, Result, encode, field, queue_path};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NewMessage {
    /// Any JSON value. Plain text is a JSON string.
    pub body: serde_json::Value,
    /// 0-9, higher is delivered first.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delay_ms: Option<u64>,
    /// RFC 3339 time to deliver at; overrides `delay_ms`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deliver_at: Option<String>,
    /// Publishing the same id again within an hour stores nothing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dedup_id: Option<String>,
}

impl NewMessage {
    pub fn new(body: impl Into<serde_json::Value>) -> Self {
        Self {
            body: body.into(),
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Published {
    pub id: String,
    /// True when the `dedup_id` was already seen and nothing was stored.
    pub duplicate: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MessageState {
    Visible,
    Delayed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum DeadLetterReason {
    MaxAttempts,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageDeadLetter {
    pub queue: String,
    pub consumer: String,
    /// The message's id in `queue`; requeueing gives it back under this id.
    pub message_id: String,
    pub reason: DeadLetterReason,
    pub at: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageInfo {
    pub id: String,
    pub queue: String,
    pub state: MessageState,
    pub priority: u8,
    pub published_at: String,
    pub deliver_at: Option<String>,
    pub dedup_id: Option<String>,
    pub size: u32,
    pub attempts: u32,
    pub last_error: Option<String>,
    pub dead_letter: Option<MessageDeadLetter>,
    /// Only present when fetching a single message.
    pub body: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Requeued {
    pub requeued: u32,
    pub skipped: u32,
}

#[derive(Serialize)]
struct BatchRequest<'a> {
    messages: &'a [NewMessage],
}

#[derive(Deserialize)]
struct BatchAnswer {
    messages: Vec<Published>,
}

impl Client {
    pub async fn publish(&self, queue: &str, message: &NewMessage) -> Result<Published> {
        self.send(Method::POST, &queue_path(queue, "/messages"), Some(message))
            .await
    }

    /// Publishes up to 1000 messages atomically.
    pub async fn publish_batch(
        &self,
        queue: &str,
        messages: &[NewMessage],
    ) -> Result<Vec<Published>> {
        let body = BatchRequest { messages };
        let answer: BatchAnswer = self
            .send(Method::POST, &queue_path(queue, "/batch"), Some(&body))
            .await?;
        Ok(answer.messages)
    }

    /// Publishes messages that are already encoded as JSON, as one atomic batch.
    pub(crate) async fn publish_encoded(
        &self,
        queue: &str,
        messages: &[&[u8]],
    ) -> Result<Vec<Published>> {
        let size: usize = messages.iter().map(|message| message.len() + 1).sum();
        let mut body = Vec::with_capacity(size + 16);
        body.extend_from_slice(b"{\"messages\":[");
        for (index, message) in messages.iter().enumerate() {
            if index > 0 {
                body.push(b',');
            }
            body.extend_from_slice(message);
        }
        body.extend_from_slice(b"]}");
        let answer: BatchAnswer = self.post_json(&queue_path(queue, "/batch"), body).await?;
        Ok(answer.messages)
    }

    /// Stored messages, newest first, without consuming them. `before` (a message id) pages to
    /// older ones.
    pub async fn messages_list(
        &self,
        queue: &str,
        state: Option<MessageState>,
        limit: Option<u32>,
        before: Option<&str>,
    ) -> Result<Vec<MessageInfo>> {
        let mut query = Vec::new();
        if let Some(state) = state {
            let state = match state {
                MessageState::Visible => "visible",
                MessageState::Delayed => "delayed",
            };
            query.push(format!("state={state}"));
        }
        if let Some(limit) = limit {
            query.push(format!("limit={limit}"));
        }
        if let Some(before) = before {
            query.push(format!("before={}", encode(before)));
        }
        let path = format!("{}?{}", queue_path(queue, "/messages"), query.join("&"));
        let response: Value = self.send(Method::GET, &path, None::<&()>).await?;
        field(response, "messages")
    }

    /// One message including its body.
    pub async fn messages_get(&self, queue: &str, id: &str) -> Result<MessageInfo> {
        let path = queue_path(queue, &format!("/messages/{}", encode(id)));
        self.send(Method::GET, &path, None::<&()>).await
    }

    /// Deletes a message for every consumer.
    pub async fn messages_remove(&self, queue: &str, id: &str) -> Result<()> {
        let path = queue_path(queue, &format!("/messages/{}", encode(id)));
        self.send_empty(Method::DELETE, &path).await
    }

    /// Gives dead letters back to the consumer that failed them: the given ids, or all of them.
    pub async fn requeue(&self, queue: &str, ids: Option<&[String]>) -> Result<Requeued> {
        let body = match ids {
            Some(ids) => json!({ "ids": ids }),
            None => json!({}),
        };
        self.send(Method::POST, &queue_path(queue, "/requeue"), Some(&body))
            .await
    }
}
