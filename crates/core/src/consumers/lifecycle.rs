//! Creating, moving and removing consumers inside a job.

use std::ops::Bound;

use redb::ReadableTable;
use ulid::Ulid;

use super::MAX_CONSUMERS;
use crate::error::{Error, Result};
use crate::memory::ConsumerMemory;
use crate::positions::{Start, comes_again, positions};
use crate::records::{ConsumerRecord, LANES, Pending};
use crate::settings::validate_name;
use crate::tx::Tx;

impl Tx<'_, '_> {
    /// Creates the consumer at `from` unless it exists.
    pub fn ensure_consumer(&mut self, queue: &str, name: &str, from: Start) -> Result<()> {
        if self.mem.queue(queue)?.consumers.contains_key(name) {
            return Ok(());
        }
        self.create_consumer(queue, name, from)
    }

    pub fn create_consumer(&mut self, queue: &str, name: &str, from: Start) -> Result<()> {
        validate_name("consumer", name)?;
        if self.mem.queue(queue)?.consumers.len() >= MAX_CONSUMERS {
            return Err(Error::Invalid(format!(
                "queue {queue} already has {MAX_CONSUMERS} consumers, the most allowed: remove one \
                 first"
            )));
        }
        let positions = self.positions_for(queue, from)?;
        let record = ConsumerRecord {
            created_at: self.now,
            positions,
            ..ConsumerRecord::default()
        };
        self.mem
            .queue_mut(queue)?
            .consumers
            .insert(name.to_string(), ConsumerMemory::new(record));
        self.consumer_changed(queue, name);
        // A consumer starting at the end holds nothing back; retention may go on.
        self.floors_moved(queue);
        self.wake.insert(queue.to_string());
        Ok(())
    }

    /// Where `from` points in the queue right now, per lane.
    pub fn positions_for(&self, queue: &str, from: Start) -> Result<[u64; LANES]> {
        let heads = self.mem.queue(queue)?.counts.heads;
        positions(&self.t.log, &self.t.messages, queue, &heads, from)
    }

    /// Replays a consumer to `positions`: drops what it still has pending, except the private
    /// copies `replay_pending` keeps, and moves its position.
    pub fn move_consumer(
        &mut self,
        queue: &str,
        name: &str,
        positions: [u64; LANES],
    ) -> Result<()> {
        self.mem.consumer(queue, name)?;
        self.replay_pending(queue, name, &positions, None, usize::MAX)?;
        self.mem.consumer_mut(queue, name)?.record.positions = positions;
        self.consumer_changed(queue, name);
        self.floors_moved(queue);
        self.wake.insert(queue.to_string());
        Ok(())
    }

    pub fn remove_consumer(&mut self, queue: &str, name: &str) -> Result<()> {
        self.mem.consumer(queue, name)?;
        while self.drop_pending(queue, name, usize::MAX)? > 0 {}
        self.mem.queue_mut(queue)?.consumers.remove(name);
        // Saving a changed consumer that no longer exists deletes its record.
        self.consumer_changed(queue, name);
        self.floors_moved(queue);
        Ok(())
    }

    /// Drops up to `limit` pending messages of a consumer, with their private copies. Leases on
    /// them become unknown. Returns how many were dropped.
    pub fn drop_pending(&mut self, queue: &str, name: &str, limit: usize) -> Result<usize> {
        let ids = self.pending_ids(queue, name, None, limit)?;
        for id in &ids {
            if let Some(pending) = self.pending(queue, name, *id)? {
                self.finish(queue, name, &pending)?;
            }
        }
        Ok(ids.len())
    }

    /// Drops up to `limit` pending messages of a consumer that replays to `positions`, after the
    /// id `after`; leases on them become unknown. A private copy of a message the consumer won't
    /// get again from the log stays. Returns the id to go on after, or `None` at the end.
    pub fn replay_pending(
        &mut self,
        queue: &str,
        name: &str,
        positions: &[u64; LANES],
        after: Option<Ulid>,
        limit: usize,
    ) -> Result<Option<Ulid>> {
        let ids = self.pending_ids(queue, name, after, limit)?;
        for id in &ids {
            let Some(pending) = self.pending(queue, name, *id)? else {
                continue;
            };
            if pending.is_copy() && !comes_again(&self.t.messages, queue, *id, positions)? {
                self.keep_copy(queue, name, &pending)?;
            } else {
                self.finish(queue, name, &pending)?;
            }
        }
        Ok(if ids.len() == limit {
            ids.last().copied()
        } else {
            None
        })
    }

    /// Puts a kept copy back as `requeue` left it: no lease and no attempts yet, so its next
    /// delivery can't go past `maxAttempts`. One nobody received since stays as it is.
    fn keep_copy(&mut self, queue: &str, name: &str, pending: &Pending) -> Result<()> {
        if pending.attempts == 0 {
            return Ok(());
        }
        let error = pending.last_error.clone();
        let again = Pending::requeued(pending.id, pending.priority, error, self.now);
        self.update_pending(queue, name, pending, &again)
    }

    /// Up to `limit` ids of a consumer's pending messages, in id order, after `after` if given.
    fn pending_ids(
        &self,
        queue: &str,
        name: &str,
        after: Option<Ulid>,
        limit: usize,
    ) -> Result<Vec<Ulid>> {
        let start = match after {
            Some(id) => Bound::Excluded((queue, name, id.0)),
            None => Bound::Included((queue, name, 0)),
        };
        let end = Bound::Included((queue, name, u128::MAX));
        let mut ids = Vec::new();
        for entry in self
            .t
            .pending
            .range::<(&str, &str, u128)>((start, end))?
            .take(limit)
        {
            ids.push(Ulid(entry?.0.value().2));
        }
        Ok(ids)
    }
}
