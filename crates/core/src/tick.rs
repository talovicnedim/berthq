//! The timer: makes delayed messages visible, retries ready, retries messages whose lease ran
//! out, forgets old dedup ids and applies retention. Runs only for queues whose `wake_at` has
//! passed, so an idle broker doesn't write anything.

use redb::ReadableTable;
use ulid::Ulid;

use crate::error::Result;
use crate::failure::Failure;
use crate::records::Slot;
use crate::tx::Tx;

/// Most entries one tick handles per queue and kind, so a huge backlog of due work is spread
/// over several ticks instead of one giant transaction.
const TICK_LIMIT: usize = 5_000;

impl Tx<'_, '_> {
    pub(crate) fn tick(&mut self) -> Result<()> {
        let now = self.now;
        let due: Vec<String> = self
            .mem
            .queues
            .values()
            .filter(|queue| queue.wake_at <= now)
            .map(|queue| queue.name.clone())
            .collect();
        for name in due {
            self.tick_queue(&name)?;
        }
        Ok(())
    }

    fn tick_queue(&mut self, name: &str) -> Result<()> {
        let now = self.now;
        let range = (name, 0, 0)..=(name, now, u128::MAX);
        let delayed: Vec<(u64, u128)> = self
            .t
            .delayed
            .range(range)?
            .take(TICK_LIMIT)
            .map(|entry| entry.map(|(key, _)| (key.value().1, key.value().2)))
            .collect::<std::result::Result<_, _>>()?;
        for (deliver_at, id) in delayed {
            self.release(name, deliver_at, Ulid(id))?;
        }

        let consumers: Vec<String> = self
            .mem
            .queue(name)?
            .consumers
            .iter()
            .filter(|(_, consumer)| consumer.wake_at <= now)
            .map(|(consumer, _)| consumer.clone())
            .collect();
        for consumer in consumers {
            self.tick_consumer(name, &consumer)?;
        }

        self.forget_dedup(name, now, TICK_LIMIT)?;
        if self.mem.queue(name)?.retention_due <= now {
            self.trim(name)?;
        }
        let wake_at = self.next_wake(name)?;
        self.mem.queue_mut(name)?.wake_at = wake_at;
        Ok(())
    }

    fn tick_consumer(&mut self, queue: &str, consumer: &str) -> Result<()> {
        let now = self.now;
        for id in Tx::due_pending(&self.t.p_scheduled, queue, consumer, now, TICK_LIMIT)? {
            if let Some(pending) = self.pending(queue, consumer, id)? {
                let mut ready = pending.clone();
                ready.slot = Slot::Ready;
                self.update_pending(queue, consumer, &pending, &ready)?;
            }
        }
        let failure = Failure {
            delay_ms: None,
            dead_letter: false,
            error: Some("lease expired before the message was acked".to_string()),
        };
        for id in Tx::due_pending(&self.t.p_leased, queue, consumer, now, TICK_LIMIT)? {
            if let Some(pending) = self.pending(queue, consumer, id)? {
                self.fail(queue, consumer, pending, &failure)?;
            }
        }
        let next = self.next_consumer_wake(queue, consumer)?;
        if let Ok(memory) = self.mem.consumer_mut(queue, consumer) {
            memory.wake_at = next;
        }
        Ok(())
    }

    /// The earliest retry or lease deadline of a consumer.
    fn next_consumer_wake(&self, queue: &str, consumer: &str) -> Result<u64> {
        let range = (queue, consumer, 0, 0)..=(queue, consumer, u64::MAX, u128::MAX);
        let mut next = u64::MAX;
        for table in [&self.t.p_scheduled, &self.t.p_leased] {
            if let Some(entry) = table.range(range.clone())?.next() {
                next = next.min(entry?.0.value().2);
            }
        }
        Ok(next)
    }

    /// The earliest time anything on the queue is due, from the heads of its indexes.
    fn next_wake(&self, name: &str) -> Result<u64> {
        let range = (name, 0, 0)..=(name, u64::MAX, u128::MAX);
        let queue = self.mem.queue(name)?;
        let mut next = queue.retention_due;
        for consumer in queue.consumers.values() {
            next = next.min(consumer.wake_at);
        }
        if let Some(entry) = self.t.delayed.range(range)?.next() {
            next = next.min(entry?.0.value().1);
        }
        let dedup = (name, 0, "")..=(name, u64::MAX, "\u{10FFFF}");
        if let Some(entry) = self.t.dedup_expiring.range(dedup)?.next() {
            next = next.min(entry?.0.value().1);
        }
        Ok(next)
    }
}

#[cfg(test)]
mod tests {
    use redb::ReadableTableMetadata;

    use super::TICK_LIMIT;
    use crate::dedup::DEDUP_WINDOW_MS;
    use crate::publish::{NewMessage, check};
    use crate::settings::QueueSettings;
    use crate::testing::{TestDb, body, publish};

    fn remembered(db: &mut TestDb) -> (u64, u64) {
        db.run(0, |tx| Ok((tx.t.dedup.len()?, tx.t.dedup_expiring.len()?)))
            .unwrap()
    }

    #[test]
    fn a_tick_forgets_at_most_its_limit_of_dedup_ids_and_comes_back_for_the_rest() {
        let mut db = TestDb::with_queue(QueueSettings::default());
        let limit = TICK_LIMIT as u64;
        let total = 2 * limit + 1;
        let messages = (0..total)
            .map(|n| {
                let message = NewMessage {
                    dedup_id: Some(n.to_string()),
                    ..body(1)
                };
                check(message, 0).unwrap()
            })
            .collect();
        db.run(0, |tx| tx.publish("jobs", messages)).unwrap();

        let mut left = Vec::new();
        for _ in 0..3 {
            db.tick(DEDUP_WINDOW_MS);
            left.push(remembered(&mut db).0);
        }
        assert_eq!(left, [total - limit, total - 2 * limit, 0]);
        assert_eq!(remembered(&mut db), (0, 0));
    }

    #[test]
    fn dedup_ids_are_forgotten_after_the_window() {
        let mut db = TestDb::with_queue(QueueSettings::default());
        let message = || NewMessage {
            dedup_id: Some("a".to_string()),
            ..body(1)
        };
        publish(&mut db, message(), 0).unwrap();
        assert_eq!(db.mem.queue("jobs").unwrap().wake_at, DEDUP_WINDOW_MS);

        assert!(
            publish(&mut db, message(), DEDUP_WINDOW_MS - 1)
                .unwrap()
                .duplicate
        );
        db.tick(DEDUP_WINDOW_MS);
        assert!(
            !publish(&mut db, message(), DEDUP_WINDOW_MS)
                .unwrap()
                .duplicate
        );
    }

    #[test]
    fn idle_queues_need_no_timer() {
        let mut db = TestDb::with_queue(QueueSettings {
            retention_ms: 0,
            ..QueueSettings::default()
        });
        publish(&mut db, body(1), 0).unwrap();
        db.tick(0);
        db.tick(0);
        assert_eq!(db.mem.queue("jobs").unwrap().wake_at, u64::MAX);
    }
}
