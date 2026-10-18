//! Creating, inspecting, changing, purging and removing queues.

use redb::ReadableTable;
use serde::Serialize;
use ulid::Ulid;

use crate::broker::Broker;
use crate::consumers::ConsumerInfo;
use crate::error::{Error, Result};
use crate::memory::{Memory, QueueMemory};
use crate::records::LANES;
use crate::settings::{
    DEAD_LETTER_SUFFIX, QueueSettings, QueueSettingsPatch, dead_letter_queue, validate_queue_name,
};
use crate::stats::{QueueStats, StatsPoint};
use crate::time;
use crate::tx::Tx;

/// Messages (and pending entries, dedup ids) deleted per transaction when purging or removing a
/// queue, so other work isn't held up for long by a huge queue.
const DELETE_CHUNK: usize = 10_000;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueInfo {
    pub name: String,
    pub created_at: String,
    pub settings: QueueSettings,
    pub stats: QueueStats,
    /// This queue's dead-letter queue, once it exists.
    pub dead_letter_queue: Option<String>,
    /// For a dead-letter queue: the queue it collects failures from.
    pub dead_letter_of: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueDetails {
    #[serde(flatten)]
    pub info: QueueInfo,
    pub consumers: Vec<ConsumerInfo>,
    pub history: Vec<StatsPoint>,
}

#[derive(Debug, Clone)]
pub struct NewQueue {
    pub name: String,
    pub settings: QueueSettingsPatch,
}

/// What one delete step did: entries removed in total, and how many of them were messages.
struct Deleted {
    work: usize,
    messages: u64,
}

impl Broker {
    pub async fn queues_create(&self, new: NewQueue) -> Result<QueueInfo> {
        validate_queue_name(&new.name)?;
        let settings = QueueSettings::default().apply(&new.settings)?;
        self.store()
            .run(move |tx| {
                if tx.mem.queues.contains_key(&new.name) {
                    return Err(Error::Conflict(format!(
                        "queue {} already exists",
                        new.name
                    )));
                }
                tx.create_queue(QueueMemory::new(&new.name, settings, tx.now))?;
                tx.mem.queue_info(&new.name)
            })
            .await
    }

    pub fn queues_list(&self) -> Vec<QueueInfo> {
        let memory = self.memory();
        memory
            .queues
            .keys()
            .filter_map(|name| memory.queue_info(name).ok())
            .collect()
    }

    /// One queue with its consumers. Its backlog is measured now rather than taken from the
    /// once-a-second refresh.
    pub fn queues_get(&self, name: &str) -> Result<QueueDetails> {
        self.refresh_queue(name)?;
        let memory = self.memory();
        let queue = memory.queue(name)?;
        let now = time::now_ms();
        let consumers = queue
            .consumers
            .keys()
            .map(|consumer| memory.consumer_info(name, consumer, now))
            .collect::<Result<Vec<_>>>()?;
        Ok(QueueDetails {
            info: memory.queue_info(name)?,
            consumers,
            history: queue.history.iter().cloned().collect(),
        })
    }

    pub async fn queues_update(&self, name: &str, patch: QueueSettingsPatch) -> Result<QueueInfo> {
        self.change_settings(name, move |settings| {
            *settings = settings.apply(&patch)?;
            Ok(())
        })
        .await
    }

    pub async fn queues_pause(&self, name: &str) -> Result<QueueInfo> {
        self.change_settings(name, |settings| {
            settings.paused = true;
            Ok(())
        })
        .await
    }

    pub async fn queues_resume(&self, name: &str) -> Result<QueueInfo> {
        self.change_settings(name, |settings| {
            settings.paused = false;
            Ok(())
        })
        .await
    }

    /// Deletes a queue with its consumers and all its messages. Its dead-letter queue stays.
    pub async fn queues_remove(&self, name: &str) -> Result<()> {
        self.memory().queue(name)?;
        // Big queues are emptied in chunks first, so no single transaction gets huge.
        while self.delete_chunk(name).await?.work > 0 {}
        while self.forget_chunk(name).await? > 0 {}
        let name = name.to_string();
        self.store()
            .run(move |tx| {
                while tx.delete_messages(&name, DELETE_CHUNK)?.work > 0 {}
                let queue = tx
                    .mem
                    .queues
                    .remove(&name)
                    .ok_or_else(|| Error::NotFound(format!("queue {name}")))?;
                for consumer in queue.consumers.keys() {
                    tx.consumer_changed(&name, consumer);
                }
                tx.t.queues.remove(name.as_str())?;
                tx.queue_changed(&name);
                // Wakes long-polling workers so they notice the queue is gone.
                queue.notify.notify_waiters();
                tx.forget_dedup(&name, u64::MAX, usize::MAX)?;
                tx.wake.remove(&name);
                Ok(())
            })
            .await
    }

    /// Deletes every message, including ones being processed. Returns how many were deleted.
    pub async fn queues_purge(&self, name: &str) -> Result<u64> {
        self.memory().queue(name)?;
        let mut purged = 0;
        loop {
            let deleted = self.delete_chunk(name).await?;
            purged += deleted.messages;
            if deleted.work == 0 {
                return Ok(purged);
            }
        }
    }

    async fn delete_chunk(&self, name: &str) -> Result<Deleted> {
        let name = name.to_string();
        self.store()
            .run(move |tx| tx.delete_messages(&name, DELETE_CHUNK))
            .await
    }

    async fn forget_chunk(&self, name: &str) -> Result<usize> {
        let name = name.to_string();
        self.store()
            .run(move |tx| tx.forget_dedup(&name, u64::MAX, DELETE_CHUNK))
            .await
    }

    async fn change_settings(
        &self,
        name: &str,
        change: impl FnOnce(&mut QueueSettings) -> Result<()> + Send + 'static,
    ) -> Result<QueueInfo> {
        let name = name.to_string();
        self.store()
            .run(move |tx| {
                let now = tx.now;
                let queue = tx.mem.queue_mut(&name)?;
                let mut settings = queue.settings.clone();
                change(&mut settings)?;
                queue.settings = settings;
                // Retention settings may have changed: let the timer look again.
                queue.trim_blocked = false;
                queue.retention_due = now;
                queue.wake_at = now;
                tx.wake.insert(name.clone());
                tx.save_queue(&name)?;
                tx.mem.queue_info(&name)
            })
            .await
    }
}

impl Tx<'_, '_> {
    /// Deletes up to `limit` entries of a queue: its consumers' pending messages first, then the
    /// messages (log, delayed, private copies).
    fn delete_messages(&mut self, queue: &str, limit: usize) -> Result<Deleted> {
        let consumers: Vec<String> = self.mem.queue(queue)?.consumers.keys().cloned().collect();
        let (mut work, mut messages) = (0, 0);
        for consumer in &consumers {
            let copies = self.mem.queue(queue)?.counts.copies;
            work += self.drop_pending(queue, consumer, limit - work)?;
            // Requeued dead letters go with their pending entry; they count as messages too.
            messages += copies - self.mem.queue(queue)?.counts.copies;
            if work == limit {
                return Ok(Deleted { work, messages });
            }
        }
        for lane in 0..LANES as u8 {
            let mut doomed = Vec::new();
            for entry in self
                .t
                .log
                .range((queue, lane, 0)..=(queue, lane, u64::MAX))?
            {
                if work + doomed.len() == limit {
                    break;
                }
                let (key, value) = entry?;
                doomed.push((key.value().2, value.value()));
            }
            for (seq, entry) in doomed {
                self.remove_entry(queue, lane, seq, entry)?;
                (work, messages) = (work + 1, messages + 1);
            }
        }
        let delayed: Vec<u128> = self
            .t
            .delayed
            .range((queue, 0, 0)..=(queue, u64::MAX, u128::MAX))?
            .take(limit - work)
            .map(|entry| entry.map(|(key, _)| key.value().2))
            .collect::<std::result::Result<_, _>>()?;
        for id in delayed {
            if let Some(message) = self.message(Ulid(id))? {
                self.remove_message(&message, false)?;
            }
            (work, messages) = (work + 1, messages + 1);
        }
        Ok(Deleted { work, messages })
    }

    /// The dead-letter queue for `name`, created with default settings if it doesn't exist yet.
    pub(crate) fn ensure_dead_letter_queue(&mut self, name: &str) -> Result<String> {
        let dlq = dead_letter_queue(name);
        if !self.mem.queues.contains_key(&dlq) {
            let queue = QueueMemory::new(&dlq, QueueSettings::default(), self.now);
            self.create_queue(queue)?;
        }
        Ok(dlq)
    }
}

impl Memory {
    pub(crate) fn queue_info(&self, name: &str) -> Result<QueueInfo> {
        let queue = self.queue(name)?;
        let dlq = dead_letter_queue(name);
        Ok(QueueInfo {
            name: queue.name.clone(),
            created_at: time::format(queue.created_at),
            settings: queue.settings.clone(),
            stats: queue.stats(),
            dead_letter_queue: self.queues.contains_key(&dlq).then_some(dlq),
            dead_letter_of: name.strip_suffix(DEAD_LETTER_SUFFIX).map(str::to_string),
        })
    }
}

#[cfg(test)]
#[path = "queues_tests.rs"]
mod tests;
