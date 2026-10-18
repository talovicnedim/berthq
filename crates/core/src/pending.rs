//! What a consumer is processing or retrying. Every change goes through `insert_pending`,
//! `update_pending` and `delete_pending`, which keep the indexes and the consumer's counts in
//! step with the record.

use redb::{ReadableTable, Table};
use ulid::Ulid;

use crate::error::Result;
use crate::records::{Pending, Slot};
use crate::tx::Tx;

type TimeIndex<'txn> = Table<'txn, (&'static str, &'static str, u64, u128), ()>;

impl<'txn> Tx<'_, 'txn> {
    pub fn pending(&self, queue: &str, consumer: &str, id: Ulid) -> Result<Option<Pending>> {
        let Some(record) = self.t.pending.get((queue, consumer, id.0))? else {
            return Ok(None);
        };
        let mut pending: Pending = serde_json::from_slice(record.value())?;
        pending.id = id;
        Ok(Some(pending))
    }

    pub fn insert_pending(&mut self, queue: &str, consumer: &str, pending: &Pending) -> Result<()> {
        self.put_pending(queue, consumer, pending)?;
        self.file(queue, consumer, pending)
    }

    pub fn update_pending(
        &mut self,
        queue: &str,
        consumer: &str,
        before: &Pending,
        after: &Pending,
    ) -> Result<()> {
        self.unfile(queue, consumer, before)?;
        self.put_pending(queue, consumer, after)?;
        self.file(queue, consumer, after)
    }

    pub fn delete_pending(&mut self, queue: &str, consumer: &str, pending: &Pending) -> Result<()> {
        self.unfile(queue, consumer, pending)?;
        self.t.pending.remove((queue, consumer, pending.id.0))?;
        Ok(())
    }

    /// Ids in a consumer's time-ordered index (retries or leases) due at or before `until`.
    pub fn due_pending(
        table: &TimeIndex<'txn>,
        queue: &str,
        consumer: &str,
        until: u64,
        limit: usize,
    ) -> Result<Vec<Ulid>> {
        let range = table.range((queue, consumer, 0, 0)..=(queue, consumer, until, u128::MAX))?;
        let mut ids = Vec::new();
        for entry in range.take(limit) {
            ids.push(Ulid(entry?.0.value().3));
        }
        Ok(ids)
    }

    fn put_pending(&mut self, queue: &str, consumer: &str, pending: &Pending) -> Result<()> {
        let record = serde_json::to_vec(pending)?;
        self.t
            .pending
            .insert((queue, consumer, pending.id.0), record.as_slice())?;
        Ok(())
    }

    fn file(&mut self, queue: &str, consumer: &str, pending: &Pending) -> Result<()> {
        let id = pending.id.0;
        let due = match pending.slot {
            Slot::Ready => {
                self.t
                    .p_ready
                    .insert((queue, consumer, pending.lane(), id), ())?;
                self.wake_workers(queue);
                None
            }
            Slot::Scheduled => {
                let at = pending.ready_at;
                self.t.p_scheduled.insert((queue, consumer, at, id), ())?;
                Some(at)
            }
            Slot::Leased => {
                let at = deadline(pending);
                self.t.p_leased.insert((queue, consumer, at, id), ())?;
                Some(at)
            }
        };
        if let Some(seq) = pending.seq {
            self.t
                .p_floor
                .insert((queue, consumer, pending.lane(), seq), ())?;
        }
        self.count(queue, consumer, pending.slot, 1, due)
    }

    fn unfile(&mut self, queue: &str, consumer: &str, pending: &Pending) -> Result<()> {
        let id = pending.id.0;
        match pending.slot {
            Slot::Ready => self
                .t
                .p_ready
                .remove((queue, consumer, pending.lane(), id))?,
            Slot::Scheduled => {
                self.t
                    .p_scheduled
                    .remove((queue, consumer, pending.ready_at, id))?
            }
            Slot::Leased => self
                .t
                .p_leased
                .remove((queue, consumer, deadline(pending), id))?,
        };
        if let Some(seq) = pending.seq {
            self.t
                .p_floor
                .remove((queue, consumer, pending.lane(), seq))?;
        }
        self.count(queue, consumer, pending.slot, -1, None)
    }

    fn count(
        &mut self,
        queue: &str,
        consumer: &str,
        slot: Slot,
        delta: i64,
        due: Option<u64>,
    ) -> Result<()> {
        let memory = self.mem.consumer_mut(queue, consumer)?;
        let count = match slot {
            Slot::Ready => &mut memory.record.ready,
            Slot::Scheduled => &mut memory.record.scheduled,
            Slot::Leased => &mut memory.record.processing,
        };
        *count = count.saturating_add_signed(delta);
        if let Some(due) = due {
            memory.wake_at = memory.wake_at.min(due);
            self.wake_queue_at(queue, due)?;
        }
        self.consumer_changed(queue, consumer);
        Ok(())
    }
}

fn deadline(pending: &Pending) -> u64 {
    pending.lease.as_ref().map_or(0, |lease| lease.deadline)
}
