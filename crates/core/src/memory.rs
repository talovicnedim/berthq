//! The little the broker keeps in RAM: per-queue and per-consumer settings, counts and positions,
//! throughput counters and connected workers. Messages and their ordering live on disk (see
//! `tables.rs`), so memory use does not grow with the number of messages.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use tokio::sync::Notify;

use crate::error::{Error, Result};
use crate::ids::IdGenerator;
use crate::records::{ConsumerRecord, LANES, QueueCounts};
use crate::settings::QueueSettings;
use crate::stats::{Counters, StatsPoint};

/// Workers that haven't asked for messages for this long are no longer listed.
pub(crate) const WORKER_IDLE_MS: u64 = 60_000;

pub(crate) struct Memory {
    pub ids: IdGenerator,
    pub queues: BTreeMap<String, QueueMemory>,
    /// Set when shutdown starts: no new publishes or deliveries, acks still work.
    pub closing: bool,
    /// Broker-wide throughput for the last minute, one point per second.
    pub history: VecDeque<StatsPoint>,
}

pub(crate) struct QueueMemory {
    pub name: String,
    pub settings: QueueSettings,
    pub created_at: u64,
    /// Mirrors the `counts` table; saved in the same transaction as every change.
    pub counts: QueueCounts,
    pub consumers: BTreeMap<String, ConsumerMemory>,
    /// Workers by (consumer, worker name), seen in the last minute.
    pub workers: BTreeMap<(String, String), Worker>,
    /// Earliest time something on this queue needs the timer.
    pub wake_at: u64,
    /// When retention could delete the oldest message.
    pub retention_due: u64,
    /// The oldest message is past its retention but a consumer still holds it; acks re-arm
    /// the timer instead of it checking every tick.
    pub trim_blocked: bool,
    /// Refreshed every second: messages some consumer still has to process, and the oldest
    /// sequence in each lane.
    pub backlog: u64,
    pub tails: [u64; LANES],
    pub counters: Counters,
    pub history: VecDeque<StatsPoint>,
    /// Wakes long-polling workers when messages become available.
    pub notify: Arc<Notify>,
}

pub(crate) struct ConsumerMemory {
    /// Mirrors the `consumers` table.
    pub record: ConsumerRecord,
    /// Earliest retry or lease deadline of this consumer.
    pub wake_at: u64,
}

pub(crate) struct Worker {
    pub last_seen: u64,
    /// Long-poll requests currently waiting for messages.
    pub waiting: u32,
}

impl Memory {
    pub fn new() -> Self {
        Self {
            ids: IdGenerator::new(),
            queues: BTreeMap::new(),
            closing: false,
            history: VecDeque::new(),
        }
    }

    pub fn queue(&self, name: &str) -> Result<&QueueMemory> {
        self.queues.get(name).ok_or_else(|| not_found(name))
    }

    pub fn queue_mut(&mut self, name: &str) -> Result<&mut QueueMemory> {
        self.queues.get_mut(name).ok_or_else(|| not_found(name))
    }

    pub fn consumer(&self, queue: &str, name: &str) -> Result<&ConsumerMemory> {
        self.queue(queue)?
            .consumers
            .get(name)
            .ok_or_else(|| consumer_not_found(queue, name))
    }

    pub fn consumer_mut(&mut self, queue: &str, name: &str) -> Result<&mut ConsumerMemory> {
        self.queue_mut(queue)?
            .consumers
            .get_mut(name)
            .ok_or_else(|| consumer_not_found(queue, name))
    }
}

impl QueueMemory {
    pub fn new(name: &str, settings: QueueSettings, created_at: u64) -> Self {
        Self {
            name: name.to_string(),
            settings,
            created_at,
            counts: QueueCounts::default(),
            consumers: BTreeMap::new(),
            workers: BTreeMap::new(),
            wake_at: u64::MAX,
            retention_due: u64::MAX,
            trim_blocked: false,
            backlog: 0,
            tails: [1; LANES],
            counters: Counters::default(),
            history: VecDeque::new(),
            notify: Arc::new(Notify::new()),
        }
    }

    /// Messages being processed right now, across consumers.
    pub fn processing(&self) -> u64 {
        self.consumers.values().map(|c| c.record.processing).sum()
    }

    /// Whether a consumer (or one that doesn't exist yet) may have something to receive.
    pub fn has_work_for(&self, consumer: &str) -> bool {
        if self.settings.paused {
            return false;
        }
        let Some(consumer) = self.consumers.get(consumer) else {
            return true;
        };
        let new = (0..LANES).any(|lane| self.counts.heads[lane] > consumer.record.positions[lane]);
        !consumer.record.paused && (consumer.record.ready > 0 || new)
    }

    pub fn workers_of(&self, consumer: &str) -> impl Iterator<Item = (&str, &Worker)> {
        self.workers
            .iter()
            .filter(move |((c, _), _)| c == consumer)
            .map(|((_, name), worker)| (name.as_str(), worker))
    }

    pub fn prune_workers(&mut self, now: u64) {
        self.workers
            .retain(|_, w| w.waiting > 0 || now.saturating_sub(w.last_seen) < WORKER_IDLE_MS);
    }
}

impl ConsumerMemory {
    pub fn new(record: ConsumerRecord) -> Self {
        Self {
            record,
            wake_at: u64::MAX,
        }
    }
}

fn not_found(name: &str) -> Error {
    Error::NotFound(format!("queue {name}"))
}

fn consumer_not_found(queue: &str, name: &str) -> Error {
    Error::NotFound(format!("consumer {name} of queue {queue}"))
}
