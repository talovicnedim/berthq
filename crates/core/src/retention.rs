//! Retention: messages past `retentionMs` are deleted once every consumer has finished them. A
//! consumer's floor (per lane, the lowest sequence it still has to finish) says what it holds
//! back; the same floors give the queue's backlog.

use redb::ReadableTable;

use crate::error::Result;
use crate::positions::{LogTable, tails};
use crate::records::{LANES, QueueCounts};
use crate::tables::LogEntry;
use crate::tx::Tx;

/// Most log entries one tick deletes per queue, so a big backlog is trimmed over several ticks.
const TRIM_LIMIT: usize = 5_000;

/// Pending log messages: (queue, consumer, lane, seq).
pub(crate) trait FloorTable:
    ReadableTable<(&'static str, &'static str, u8, u64), ()>
{
}
impl<T: ReadableTable<(&'static str, &'static str, u8, u64), ()>> FloorTable for T {}

/// Why trimming a lane stopped.
enum Stop {
    /// The next message is not past retention yet; it is at this time.
    Young(u64),
    /// The next message is past retention, but a consumer still holds it.
    Held,
    /// This tick deleted enough.
    Budget,
    /// The lane is empty.
    End,
}

/// Per lane, the lowest sequence a consumer still has to finish: its oldest pending message, or
/// the one after its position.
pub(crate) fn consumer_floors(
    floors: &impl FloorTable,
    queue: &str,
    consumer: &str,
    positions: &[u64; LANES],
) -> Result<[u64; LANES]> {
    let mut result = [0; LANES];
    for (lane, floor) in result.iter_mut().enumerate() {
        let lane_u8 = lane as u8;
        let range = (queue, consumer, lane_u8, 0)..=(queue, consumer, lane_u8, u64::MAX);
        let oldest = match floors.range(range)?.next() {
            Some(entry) => entry?.0.value().3,
            None => u64::MAX,
        };
        *floor = oldest.min(positions[lane] + 1);
    }
    Ok(result)
}

/// Per lane, the lowest sequence any consumer still has to finish; `None` without consumers.
pub(crate) fn queue_floors<'c>(
    floors: &impl FloorTable,
    queue: &str,
    consumers: impl IntoIterator<Item = (&'c str, &'c [u64; LANES])>,
) -> Result<Option<[u64; LANES]>> {
    let mut result: Option<[u64; LANES]> = None;
    for (consumer, positions) in consumers {
        let mine = consumer_floors(floors, queue, consumer, positions)?;
        let all = result.get_or_insert([u64::MAX; LANES]);
        for lane in 0..LANES {
            all[lane] = all[lane].min(mine[lane]);
        }
    }
    Ok(result)
}

/// Messages at least one consumer still has to process; every visible one without consumers.
/// Counted from sequences, so a message deleted in between may still be counted.
pub(crate) fn backlog(
    counts: &QueueCounts,
    tails: &[u64; LANES],
    floors: Option<&[u64; LANES]>,
) -> u64 {
    let mut total = 0;
    for lane in 0..LANES {
        let start = floors.map_or(tails[lane], |f| f[lane].max(tails[lane]));
        total += (counts.heads[lane] + 1).saturating_sub(start);
    }
    total.min(counts.log) + counts.copies
}

/// Backlog and tails of a queue, read in one snapshot (for stats).
pub(crate) fn measure<'c>(
    log: &impl LogTable,
    floors: &impl FloorTable,
    queue: &str,
    counts: &QueueCounts,
    consumers: impl IntoIterator<Item = (&'c str, &'c [u64; LANES])>,
) -> Result<(u64, [u64; LANES])> {
    let tails = tails(log, queue, &counts.heads)?;
    let floors = queue_floors(floors, queue, consumers)?;
    Ok((backlog(counts, &tails, floors.as_ref()), tails))
}

impl Tx<'_, '_> {
    /// Deletes messages past retention that no consumer holds back (up to `TRIM_LIMIT`) and
    /// works out when retention is due next.
    pub fn trim(&mut self, name: &str) -> Result<()> {
        let retention = self.mem.queue(name)?.settings.retention_ms;
        // Without consumers nothing is finished yet, so everything is held.
        let floors = self.floors_now(name)?.unwrap_or([0; LANES]);
        let old_enough = self.now.saturating_sub(retention);
        let (mut due, mut held, mut budget) = (u64::MAX, false, TRIM_LIMIT);
        for lane in 0..LANES as u8 {
            let floor = floors[usize::from(lane)];
            let (doomed, stop) = self.trimmable(name, lane, old_enough, floor, budget)?;
            budget -= doomed.len();
            match stop {
                Stop::Young(visible_at) => due = due.min(visible_at.saturating_add(retention)),
                Stop::Held => held = true,
                Stop::Budget => due = self.now,
                Stop::End => {}
            }
            for (seq, entry) in doomed {
                self.remove_entry(name, lane, seq, entry)?;
            }
        }
        let queue = self.mem.queue_mut(name)?;
        queue.retention_due = due;
        queue.trim_blocked = held;
        Ok(())
    }

    /// A consumer finished or dropped messages: retention may delete more now.
    pub fn floors_moved(&mut self, name: &str) {
        let now = self.now;
        if let Ok(queue) = self.mem.queue_mut(name)
            && queue.trim_blocked
        {
            queue.trim_blocked = false;
            queue.retention_due = queue.retention_due.min(now);
            queue.wake_at = queue.wake_at.min(now);
        }
    }

    /// The backlog right now, read from the indexes (for `maxMessages`).
    pub fn backlog_now(&self, name: &str) -> Result<u64> {
        let queue = self.mem.queue(name)?;
        let tails = tails(&self.t.log, name, &queue.counts.heads)?;
        let floors = self.floors_now(name)?;
        Ok(backlog(&queue.counts, &tails, floors.as_ref()))
    }

    pub fn floors_now(&self, name: &str) -> Result<Option<[u64; LANES]>> {
        let queue = self.mem.queue(name)?;
        let consumers = queue
            .consumers
            .iter()
            .map(|(consumer, memory)| (consumer.as_str(), &memory.record.positions));
        queue_floors(&self.t.p_floor, name, consumers)
    }

    /// The oldest entries of a lane that retention may delete now.
    fn trimmable(
        &self,
        name: &str,
        lane: u8,
        old_enough: u64,
        floor: u64,
        budget: usize,
    ) -> Result<(Vec<(u64, LogEntry)>, Stop)> {
        let mut doomed = Vec::new();
        for entry in self.t.log.range((name, lane, 0)..=(name, lane, u64::MAX))? {
            let (key, value) = entry?;
            let (seq, entry) = (key.value().2, value.value());
            if entry.1 > old_enough {
                return Ok((doomed, Stop::Young(entry.1)));
            }
            if seq >= floor {
                return Ok((doomed, Stop::Held));
            }
            if doomed.len() == budget {
                return Ok((doomed, Stop::Budget));
            }
            doomed.push((seq, entry));
        }
        Ok((doomed, Stop::End))
    }
}

#[cfg(test)]
#[path = "retention_tests.rs"]
mod tests;
