//! What the API shows about a consumer, built from memory.

use serde::Serialize;

use crate::error::Result;
use crate::memory::{Memory, QueueMemory, WORKER_IDLE_MS, Worker};
use crate::records::{ConsumerRecord, LANES};
use crate::time;

/// A consumer that holds messages is idle after this long without a worker.
const IDLE_AFTER_MS: u64 = 60 * 60 * 1000;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerInfo {
    pub queue: String,
    pub name: String,
    pub created_at: String,
    pub paused: bool,
    pub stats: ConsumerStats,
    /// No worker for an hour while it still holds messages: it keeps them from being deleted.
    pub idle: bool,
    pub last_seen: Option<String>,
    /// Workers seen in the last minute.
    pub workers: Vec<WorkerInfo>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerStats {
    /// Messages after its position it hasn't received yet.
    pub waiting: u64,
    pub processing: u64,
    /// Messages it received before that wait for their next attempt.
    pub retrying: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerInfo {
    pub name: String,
    pub last_seen: String,
    /// Long-polling right now.
    pub waiting: bool,
}

impl Memory {
    pub(crate) fn consumer_info(&self, queue: &str, name: &str, now: u64) -> Result<ConsumerInfo> {
        let queue_memory = self.queue(queue)?;
        let record = &self.consumer(queue, name)?.record;
        let stats = ConsumerStats {
            waiting: waiting(queue_memory, record),
            processing: record.processing,
            retrying: record.retrying(),
        };
        let workers: Vec<(&str, &Worker)> = queue_memory
            .workers_of(name)
            .filter(|(_, w)| w.waiting > 0 || now.saturating_sub(w.last_seen) < WORKER_IDLE_MS)
            .collect();
        let last_seen = workers
            .iter()
            .map(|(_, worker)| worker.last_seen)
            .chain(record.last_seen)
            .max();
        let polling = workers.iter().any(|(_, worker)| worker.waiting > 0);
        let holds = stats.waiting + stats.processing + stats.retrying > 0;
        let quiet_for = now.saturating_sub(last_seen.unwrap_or(record.created_at));
        Ok(ConsumerInfo {
            queue: queue.to_string(),
            name: name.to_string(),
            created_at: time::format(record.created_at),
            paused: record.paused,
            stats,
            idle: holds && !polling && quiet_for >= IDLE_AFTER_MS,
            last_seen: last_seen.map(time::format),
            workers: workers
                .into_iter()
                .map(|(name, worker)| WorkerInfo {
                    name: name.to_string(),
                    last_seen: time::format(worker.last_seen),
                    waiting: worker.waiting > 0,
                })
                .collect(),
        })
    }
}

/// Messages after the position, per lane, not counting ones retention already deleted.
pub(crate) fn waiting(queue: &QueueMemory, record: &ConsumerRecord) -> u64 {
    (0..LANES)
        .map(|lane| {
            let start = record.positions[lane].max(queue.tails[lane].saturating_sub(1));
            queue.counts.heads[lane].saturating_sub(start)
        })
        .sum()
}
