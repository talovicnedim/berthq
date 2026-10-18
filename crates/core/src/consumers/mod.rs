//! Consumers: names with a saved position in a queue. Every consumer gets every message, at its
//! own pace; its workers share them.

mod info;
mod lifecycle;
#[cfg(test)]
mod tests;
mod view;

use serde::Deserialize;

use crate::broker::Broker;
use crate::error::{Error, Result};
use crate::positions::Start;
use crate::records::LANES;
use crate::time;

pub use info::{ConsumerInfo, ConsumerStats, WorkerInfo};
pub use view::{ConsumerMessage, ConsumerMessageState};

/// Most consumers one queue may have.
pub const MAX_CONSUMERS: usize = 100;
/// Pending messages dropped per transaction when a consumer is replayed or removed.
const DROP_CHUNK: usize = 10_000;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NewConsumer {
    pub name: String,
    /// Where it starts: `start` (default), `end`, an RFC 3339 time or a message id.
    pub from: Option<String>,
}

impl Broker {
    pub async fn consumers_create(&self, queue: &str, new: NewConsumer) -> Result<ConsumerInfo> {
        let from = Start::parse(new.from.as_deref())?;
        let queue = queue.to_string();
        self.store()
            .run(move |tx| {
                if tx.mem.queue(&queue)?.consumers.contains_key(&new.name) {
                    return Err(Error::Conflict(format!(
                        "consumer {} of queue {queue} already exists",
                        new.name
                    )));
                }
                tx.create_consumer(&queue, &new.name, from)?;
                tx.mem.consumer_info(&queue, &new.name, tx.now)
            })
            .await
    }

    pub fn consumers_list(&self, queue: &str) -> Result<Vec<ConsumerInfo>> {
        let memory = self.memory();
        let now = time::now_ms();
        let names: Vec<&String> = memory.queue(queue)?.consumers.keys().collect();
        names
            .into_iter()
            .map(|name| memory.consumer_info(queue, name, now))
            .collect()
    }

    pub fn consumers_get(&self, queue: &str, name: &str) -> Result<ConsumerInfo> {
        self.memory().consumer_info(queue, name, time::now_ms())
    }

    /// Removes a consumer with its position, leases and retries.
    pub async fn consumers_remove(&self, queue: &str, name: &str) -> Result<()> {
        self.memory().consumer(queue, name)?;
        self.drop_all_pending(queue, name).await?;
        let (queue, name) = (queue.to_string(), name.to_string());
        self.store()
            .run(move |tx| tx.remove_consumer(&queue, &name))
            .await
    }

    /// Moves a consumer's position. What it was processing and retrying is dropped, except
    /// requeued dead letters it won't get again from the log.
    pub async fn consumers_replay(
        &self,
        queue: &str,
        name: &str,
        from: Option<&str>,
    ) -> Result<ConsumerInfo> {
        let from = Start::parse(from)?;
        // Found before anything is dropped, so a bad message id drops nothing. Plain sequences
        // stay right even when that message is deleted while the replay runs.
        let positions = {
            let (queue, name) = (queue.to_string(), name.to_string());
            self.store()
                .run(move |tx| {
                    tx.mem.consumer(&queue, &name)?;
                    tx.positions_for(&queue, from)
                })
                .await?
        };
        self.replay_all_pending(queue, name, positions).await?;
        let (queue, name) = (queue.to_string(), name.to_string());
        self.store()
            .run(move |tx| {
                tx.move_consumer(&queue, &name, positions)?;
                tx.mem.consumer_info(&queue, &name, tx.now)
            })
            .await
    }

    pub async fn consumers_pause(&self, queue: &str, name: &str) -> Result<ConsumerInfo> {
        self.change_consumer(queue, name, |tx, queue, name| {
            tx.mem.consumer_mut(queue, name)?.record.paused = true;
            Ok(())
        })
        .await
    }

    pub async fn consumers_resume(&self, queue: &str, name: &str) -> Result<ConsumerInfo> {
        self.change_consumer(queue, name, |tx, queue, name| {
            tx.mem.consumer_mut(queue, name)?.record.paused = false;
            tx.wake.insert(queue.to_string());
            Ok(())
        })
        .await
    }

    async fn drop_all_pending(&self, queue: &str, name: &str) -> Result<()> {
        loop {
            let (queue, name) = (queue.to_string(), name.to_string());
            let dropped = self
                .store()
                .run(move |tx| tx.drop_pending(&queue, &name, DROP_CHUNK))
                .await?;
            if dropped < DROP_CHUNK {
                return Ok(());
            }
        }
    }

    async fn replay_all_pending(
        &self,
        queue: &str,
        name: &str,
        positions: [u64; LANES],
    ) -> Result<()> {
        let mut after = None;
        loop {
            let (queue, name) = (queue.to_string(), name.to_string());
            after = self
                .store()
                .run(move |tx| tx.replay_pending(&queue, &name, &positions, after, DROP_CHUNK))
                .await?;
            if after.is_none() {
                return Ok(());
            }
        }
    }

    async fn change_consumer(
        &self,
        queue: &str,
        name: &str,
        change: impl FnOnce(&mut crate::tx::Tx, &str, &str) -> Result<()> + Send + 'static,
    ) -> Result<ConsumerInfo> {
        let (queue, name) = (queue.to_string(), name.to_string());
        self.store()
            .run(move |tx| {
                tx.mem.consumer(&queue, &name)?;
                change(tx, &queue, &name)?;
                tx.consumer_changed(&queue, &name);
                tx.mem.consumer_info(&queue, &name, tx.now)
            })
            .await
    }
}
