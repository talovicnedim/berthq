//! Creating, inspecting, changing and removing queues, and broker-wide stats.

use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::consumers::ConsumerInfo;
use crate::{Client, Error, Result, field, queue_path};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueSettings {
    /// How long messages stay after they become visible, for replay. They also stay until every
    /// consumer has acked them.
    pub retention_ms: u64,
    pub visibility_timeout_ms: u64,
    /// Deliveries before a message is dead-lettered; 0 retries forever.
    pub max_attempts: u32,
    /// First retry delay, doubled per attempt up to 5 minutes.
    pub retry_delay_ms: u64,
    /// Most messages consumers still have to process; publishing to a full queue fails.
    pub max_messages: Option<u32>,
    pub paused: bool,
}

/// Settings to set; `None` keeps the current value. `max_messages` of 0 removes the limit.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueSettingsPatch {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retention_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub visibility_timeout_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_attempts: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_delay_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_messages: Option<u32>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Counters {
    pub published: u64,
    pub delivered: u64,
    pub acked: u64,
    pub retried: u64,
    pub dead_lettered: u64,
    /// Ran out of attempts in a dead-letter queue, which has no dead-letter queue of its own.
    pub dropped: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueStats {
    /// Everything stored, including messages kept only for replay and delayed ones.
    pub messages: u64,
    /// Messages at least one consumer still has to process.
    pub backlog: u64,
    pub delayed: u64,
    pub processing: u64,
    pub bytes: u64,
    pub consumers: u64,
    pub workers: u64,
    pub counters: Counters,
    pub publish_rate: u64,
    pub deliver_rate: u64,
    pub ack_rate: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueInfo {
    pub name: String,
    pub created_at: String,
    pub settings: QueueSettings,
    pub stats: QueueStats,
    pub dead_letter_queue: Option<String>,
    pub dead_letter_of: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueDetails {
    #[serde(flatten)]
    pub info: QueueInfo,
    pub consumers: Vec<ConsumerInfo>,
    pub history: Vec<StatsPoint>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StatsPoint {
    pub at: String,
    pub published: u64,
    pub delivered: u64,
    pub acked: u64,
    pub failed: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Overview {
    pub version: String,
    pub started_at: String,
    pub queues: u64,
    pub messages: u64,
    pub backlog: u64,
    pub processing: u64,
    /// Messages in dead-letter queues.
    pub failed: u64,
    pub consumers: u64,
    pub workers: u64,
    pub publish_rate: u64,
    pub deliver_rate: u64,
    pub ack_rate: u64,
    pub counters: Counters,
    pub history: Vec<StatsPoint>,
    pub disk: DiskStats,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiskStats {
    /// Bytes free on the disk that holds the broker's data.
    pub free: u64,
    /// Publishing stops below this many free bytes; 0 means the check is off.
    pub min_free: u64,
    /// True while publishes are refused with `diskFull`.
    pub full: bool,
}

impl Client {
    pub async fn queues_list(&self) -> Result<Vec<QueueInfo>> {
        let response: Value = self.send(Method::GET, "/v1/queues", None::<&()>).await?;
        field(response, "queues")
    }

    pub async fn queues_create(
        &self,
        name: &str,
        settings: &QueueSettingsPatch,
    ) -> Result<QueueInfo> {
        let mut body =
            serde_json::to_value(settings).map_err(|err| Error::Http(err.to_string()))?;
        if let Value::Object(map) = &mut body {
            map.insert("name".to_string(), json!(name));
        }
        self.send(Method::POST, "/v1/queues", Some(&body)).await
    }

    pub async fn queues_get(&self, queue: &str) -> Result<QueueDetails> {
        self.send(Method::GET, &queue_path(queue, ""), None::<&()>)
            .await
    }

    pub async fn queues_update(
        &self,
        queue: &str,
        settings: &QueueSettingsPatch,
    ) -> Result<QueueInfo> {
        self.send(Method::PATCH, &queue_path(queue, ""), Some(settings))
            .await
    }

    pub async fn queues_remove(&self, queue: &str) -> Result<()> {
        self.send_empty(Method::DELETE, &queue_path(queue, ""))
            .await
    }

    /// Deletes every message, including ones kept for replay; returns how many.
    pub async fn queues_purge(&self, queue: &str) -> Result<u64> {
        let response: Value = self
            .send(Method::POST, &queue_path(queue, "/purge"), None::<&()>)
            .await?;
        field(response, "purged")
    }

    pub async fn queues_pause(&self, queue: &str) -> Result<QueueInfo> {
        self.send(Method::POST, &queue_path(queue, "/pause"), None::<&()>)
            .await
    }

    pub async fn queues_resume(&self, queue: &str) -> Result<QueueInfo> {
        self.send(Method::POST, &queue_path(queue, "/resume"), None::<&()>)
            .await
    }

    /// Broker-wide totals, rates and the last minute of throughput.
    pub async fn stats(&self) -> Result<Overview> {
        self.send(Method::GET, "/v1/stats", None::<&()>).await
    }
}
