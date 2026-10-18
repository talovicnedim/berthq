//! Long-polling: which workers are waiting, and whether a consumer has anything to take now.

use std::sync::Arc;

use tokio::sync::Notify;

use crate::broker::Broker;
use crate::error::{Error, Result};
use crate::memory::Worker;
use crate::time;

impl Broker {
    /// What wakes the consumer's long-poll, and whether to try a take now.
    pub(crate) fn poll_state(&self, queue: &str, consumer: &str) -> Result<(Arc<Notify>, bool)> {
        let memory = self.memory();
        if memory.closing {
            return Err(Error::Unavailable);
        }
        let queue = memory.queue(queue)?;
        Ok((queue.notify.clone(), queue.has_work_for(consumer)))
    }
}

/// Tracks a worker as connected and waiting for as long as a consume call runs, including when
/// the call is cancelled because the client went away.
pub(crate) struct Waiting<'a> {
    broker: &'a Broker,
    queue: String,
    key: (String, String),
}

impl<'a> Waiting<'a> {
    pub(crate) fn start(
        broker: &'a Broker,
        queue: &str,
        consumer: &str,
        worker: &str,
    ) -> Result<Self> {
        let key = (consumer.to_string(), worker.to_string());
        let mut memory = broker.memory();
        let entry = memory
            .queue_mut(queue)?
            .workers
            .entry(key.clone())
            .or_insert(Worker {
                last_seen: 0,
                waiting: 0,
            });
        entry.waiting += 1;
        entry.last_seen = time::now_ms();
        Ok(Waiting {
            broker,
            queue: queue.to_string(),
            key,
        })
    }
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        let mut memory = self.broker.memory();
        if let Ok(queue) = memory.queue_mut(&self.queue)
            && let Some(worker) = queue.workers.get_mut(&self.key)
        {
            worker.waiting = worker.waiting.saturating_sub(1);
            worker.last_seen = time::now_ms();
        }
    }
}
