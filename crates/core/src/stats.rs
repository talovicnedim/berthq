//! Counters, per-second throughput and the broker-wide overview.

use std::collections::VecDeque;

use serde::Serialize;

use crate::broker::Broker;
use crate::disk::DiskStats;
use crate::error::Result;
use crate::memory::{Memory, QueueMemory};
use crate::records::{LANES, QueueCounts};
use crate::retention::measure;
use crate::settings::is_dead_letter_queue;
use crate::tables::{LOG, P_FLOOR};
use crate::time;

/// Seconds of throughput history kept for charts.
const HISTORY_LEN: usize = 60;

/// Totals since the broker started (not persisted).
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Counters {
    pub published: u64,
    pub delivered: u64,
    pub acked: u64,
    /// Failed deliveries (nacks and expired leases) that were scheduled for another attempt.
    pub retried: u64,
    pub dead_lettered: u64,
    /// Ran out of attempts in a dead-letter queue, which has no dead-letter queue of its own.
    pub dropped: u64,
}

/// Throughput during one second.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StatsPoint {
    pub at: String,
    pub published: u64,
    pub delivered: u64,
    pub acked: u64,
    /// Retried, dead-lettered and dropped messages.
    pub failed: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueStats {
    /// Everything stored, including messages kept only for replay and delayed ones.
    pub messages: u64,
    /// Messages at least one consumer still has to process.
    pub backlog: u64,
    /// Waiting for their `deliverAt`.
    pub delayed: u64,
    /// Leased right now, across consumers.
    pub processing: u64,
    /// Sum of body sizes in bytes.
    pub bytes: u64,
    pub consumers: u64,
    /// Workers seen in the last minute, across consumers.
    pub workers: u64,
    pub counters: Counters,
    /// Messages per second during the last second.
    pub publish_rate: u64,
    pub deliver_rate: u64,
    pub ack_rate: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Overview {
    pub version: String,
    pub started_at: String,
    pub queues: u64,
    /// Everything stored in every queue.
    pub messages: u64,
    /// Backlog of the regular queues.
    pub backlog: u64,
    /// Messages leased to workers.
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

/// What the once-a-second refresh needs from memory, copied so the lock isn't held while
/// reading.
type Snapshot = Vec<(String, QueueCounts, Vec<(String, [u64; LANES])>)>;

impl Counters {
    fn failed(&self) -> u64 {
        self.retried + self.dead_lettered + self.dropped
    }

    fn add(&mut self, other: &Counters) {
        self.published += other.published;
        self.delivered += other.delivered;
        self.acked += other.acked;
        self.retried += other.retried;
        self.dead_lettered += other.dead_lettered;
        self.dropped += other.dropped;
    }
}

impl QueueMemory {
    pub(crate) fn stats(&self) -> QueueStats {
        let last = self.history.back();
        QueueStats {
            messages: self.counts.messages(),
            backlog: self.backlog,
            delayed: self.counts.delayed,
            processing: self.processing(),
            bytes: self.counts.bytes,
            consumers: self.consumers.len() as u64,
            workers: self.workers.len() as u64,
            counters: self.counters,
            publish_rate: last.map_or(0, |p| p.published),
            deliver_rate: last.map_or(0, |p| p.delivered),
            ack_rate: last.map_or(0, |p| p.acked),
        }
    }
}

impl Memory {
    /// Records the last second of throughput for every queue and the broker as a whole, and
    /// forgets idle workers. `previous` holds each queue's counters at the last sample.
    pub(crate) fn sample(
        &mut self,
        now: u64,
        previous: &mut std::collections::HashMap<String, Counters>,
    ) {
        let at = time::format(now);
        let mut total = StatsPoint::empty(&at);
        for queue in self.queues.values_mut() {
            let before = previous.get(&queue.name).copied().unwrap_or_default();
            let point = StatsPoint {
                at: at.clone(),
                published: queue.counters.published.saturating_sub(before.published),
                delivered: queue.counters.delivered.saturating_sub(before.delivered),
                acked: queue.counters.acked.saturating_sub(before.acked),
                failed: queue.counters.failed().saturating_sub(before.failed()),
            };
            total.published += point.published;
            total.delivered += point.delivered;
            total.acked += point.acked;
            total.failed += point.failed;
            push(&mut queue.history, point);
            previous.insert(queue.name.clone(), queue.counters);
            queue.prune_workers(now);
        }
        previous.retain(|name, _| self.queues.contains_key(name));
        push(&mut self.history, total);
    }
}

impl StatsPoint {
    fn empty(at: &str) -> Self {
        Self {
            at: at.to_string(),
            published: 0,
            delivered: 0,
            acked: 0,
            failed: 0,
        }
    }
}

fn push(history: &mut VecDeque<StatsPoint>, point: StatsPoint) {
    if history.len() == HISTORY_LEN {
        history.pop_front();
    }
    history.push_back(point);
}

impl Broker {
    pub fn overview(&self) -> Overview {
        let state = self.memory();
        let mut overview = Overview {
            version: env!("CARGO_PKG_VERSION").to_string(),
            started_at: time::format(self.started_at()),
            queues: state.queues.len() as u64,
            messages: 0,
            backlog: 0,
            processing: 0,
            failed: 0,
            consumers: 0,
            workers: 0,
            publish_rate: 0,
            deliver_rate: 0,
            ack_rate: 0,
            counters: Counters::default(),
            history: state.history.iter().cloned().collect(),
            disk: self.disk().stats(),
        };
        for queue in state.queues.values() {
            overview.messages += queue.counts.messages();
            if is_dead_letter_queue(&queue.name) {
                overview.failed += queue.counts.messages();
            } else {
                overview.backlog += queue.backlog;
            }
            overview.processing += queue.processing();
            overview.consumers += queue.consumers.len() as u64;
            overview.workers += queue.workers.len() as u64;
            overview.counters.add(&queue.counters);
        }
        if let Some(last) = state.history.back() {
            overview.publish_rate = last.published;
            overview.deliver_rate = last.delivered;
            overview.ack_rate = last.acked;
        }
        overview
    }

    /// Measures every queue's backlog and tails in one snapshot; run once a second.
    pub(crate) fn refresh_stats(&self) -> Result<()> {
        let names: Vec<String> = self.memory().queues.keys().cloned().collect();
        self.refresh(&names)
    }

    pub(crate) fn refresh_queue(&self, name: &str) -> Result<()> {
        self.memory().queue(name)?;
        self.refresh(&[name.to_string()])
    }

    fn refresh(&self, names: &[String]) -> Result<()> {
        let snapshot: Snapshot = {
            let memory = self.memory();
            names
                .iter()
                .filter_map(|name| memory.queues.get(name))
                .map(|queue| {
                    let consumers = queue
                        .consumers
                        .iter()
                        .map(|(name, c)| (name.clone(), c.record.positions))
                        .collect();
                    (queue.name.clone(), queue.counts, consumers)
                })
                .collect()
        };
        let txn = self.store().read()?;
        let (log, floors) = (txn.open_table(LOG)?, txn.open_table(P_FLOOR)?);
        let mut measured = Vec::with_capacity(snapshot.len());
        for (name, counts, consumers) in &snapshot {
            let consumers = consumers.iter().map(|(c, p)| (c.as_str(), p));
            measured.push((name, measure(&log, &floors, name, counts, consumers)?));
        }
        let mut memory = self.memory();
        for (name, (backlog, tails)) in measured {
            if let Some(queue) = memory.queues.get_mut(name) {
                queue.backlog = backlog;
                queue.tails = tails;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "stats_tests.rs"]
mod tests;
