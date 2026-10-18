//! One write transaction plus the in-memory state, handed to every job.

use std::collections::{BTreeMap, BTreeSet};

use crate::error::{Error, Result};
use crate::memory::{Memory, QueueMemory};
use crate::records::QueueRecord;
use crate::tables::Tables;

/// Jobs must check everything that can fail validation **before** their first write: a failing
/// job's writes are not rolled back on their own (storage errors abort the whole transaction).
pub(crate) struct Tx<'a, 'txn> {
    pub t: &'a mut Tables<'txn>,
    pub mem: &'a mut Memory,
    pub now: u64,
    /// Queues whose waiting workers are woken once the transaction is committed.
    pub wake: BTreeSet<String>,
    /// Queues whose counts changed and must be saved.
    dirty_queues: BTreeSet<String>,
    /// Consumers (by queue) whose record (position, counts) changed and must be saved.
    dirty_consumers: BTreeMap<String, BTreeSet<String>>,
}

impl<'a, 'txn> Tx<'a, 'txn> {
    pub fn new(t: &'a mut Tables<'txn>, mem: &'a mut Memory, now: u64) -> Self {
        Self {
            t,
            mem,
            now,
            wake: BTreeSet::new(),
            dirty_queues: BTreeSet::new(),
            dirty_consumers: BTreeMap::new(),
        }
    }

    /// Wakes the queue's long-polling workers once the transaction is committed.
    pub fn wake_workers(&mut self, queue: &str) {
        if !self.wake.contains(queue) {
            self.wake.insert(queue.to_string());
        }
    }

    pub fn queue_changed(&mut self, queue: &str) {
        if !self.dirty_queues.contains(queue) {
            self.dirty_queues.insert(queue.to_string());
        }
    }

    pub fn consumer_changed(&mut self, queue: &str, consumer: &str) {
        // Called for every pending change: checks before allocating.
        if let Some(consumers) = self.dirty_consumers.get_mut(queue) {
            if !consumers.contains(consumer) {
                consumers.insert(consumer.to_string());
            }
            return;
        }
        let consumers = BTreeSet::from([consumer.to_string()]);
        self.dirty_consumers.insert(queue.to_string(), consumers);
    }

    /// Creates a queue's records and memory.
    pub fn create_queue(&mut self, queue: QueueMemory) -> Result<()> {
        let name = queue.name.clone();
        self.mem.queues.insert(name.clone(), queue);
        self.save_queue(&name)?;
        self.queue_changed(&name);
        Ok(())
    }

    pub fn save_queue(&mut self, name: &str) -> Result<()> {
        let queue = self.mem.queue(name)?;
        let record = QueueRecord {
            name: queue.name.clone(),
            settings: queue.settings.clone(),
            created_at: queue.created_at,
        };
        self.t
            .queues
            .insert(name, serde_json::to_vec(&record)?.as_slice())?;
        Ok(())
    }

    /// Saves the counts and consumer records changed in this transaction.
    pub fn save_counts(&mut self) -> Result<()> {
        for name in std::mem::take(&mut self.dirty_queues) {
            match self.mem.queues.get(&name) {
                Some(queue) => {
                    let counts = serde_json::to_vec(&queue.counts)?;
                    self.t.counts.insert(name.as_str(), counts.as_slice())?;
                }
                None => {
                    self.t.counts.remove(name.as_str())?;
                }
            }
        }
        let dirty = std::mem::take(&mut self.dirty_consumers);
        for (queue, consumers) in &dirty {
            for consumer in consumers {
                let key = (queue.as_str(), consumer.as_str());
                match self.mem.consumer(queue, consumer) {
                    Ok(memory) => {
                        let record = serde_json::to_vec(&memory.record)?;
                        self.t.consumers.insert(key, record.as_slice())?;
                    }
                    Err(_) => {
                        self.t.consumers.remove(key)?;
                    }
                }
            }
        }
        Ok(())
    }
}

impl Error {
    pub(crate) fn is_storage(&self) -> bool {
        matches!(self, Error::Storage(_))
    }
}
