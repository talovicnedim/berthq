//! Requeueing dead letters: each one goes back to the queue and consumer that failed it, as a
//! private copy with its original id, so other consumers don't get it again. When a replay
//! already brings the original back to that consumer, it gets no copy.

use redb::ReadableTable;
use serde::Serialize;
use ulid::Ulid;

use crate::broker::Broker;
use crate::error::{Error, Result};
use crate::ids;
use crate::positions::comes_again;
use crate::records::{DeadLetter, LANES, Message, Pending};
use crate::settings::{DEAD_LETTER_SUFFIX, is_dead_letter_queue};
use crate::tx::Tx;

/// Most dead letters one requeue call may name. More would hold up every other write while the
/// writer moves them; requeueing all of them goes in chunks instead.
pub const MAX_REQUEUE: usize = 1000;
/// Dead letters requeued per transaction when requeueing all of them.
const REQUEUE_CHUNK: usize = 5_000;

/// Where requeueing all dead letters continues: (lane, seq) of the last one looked at.
type Cursor = (u8, u64);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Requeued {
    pub requeued: u32,
    /// Left in place: not dead letters, or their queue or consumer no longer exists.
    pub skipped: u32,
}

impl Broker {
    /// Moves dead letters back: the given ids, or every dead letter in the queue.
    pub async fn requeue(&self, queue: &str, ids: Option<Vec<String>>) -> Result<Requeued> {
        if let Some(selected) = ids {
            if selected.len() > MAX_REQUEUE {
                return Err(Error::Invalid(format!(
                    "ids holds at most {MAX_REQUEUE} ids; leave ids out to requeue all of them"
                )));
            }
            let queue = queue.to_string();
            return self
                .store()
                .run(move |tx| {
                    tx.check_dead_letter_queue(&queue)?;
                    let ids: Vec<Option<Ulid>> = selected.iter().map(|id| ids::parse(id)).collect();
                    tx.requeue(&queue, ids)
                })
                .await;
        }
        let mut total = Requeued::default();
        let mut after = (0, 0);
        loop {
            let queue = queue.to_string();
            let (requeued, next) = self
                .store()
                .run(move |tx| {
                    tx.check_dead_letter_queue(&queue)?;
                    let (ids, next) = tx.dead_letters_after(&queue, after, REQUEUE_CHUNK)?;
                    Ok((tx.requeue(&queue, ids)?, next))
                })
                .await?;
            total.requeued += requeued.requeued;
            total.skipped += requeued.skipped;
            match next {
                Some(next) => after = next,
                None => return Ok(total),
            }
        }
    }
}

impl Tx<'_, '_> {
    fn check_dead_letter_queue(&self, queue: &str) -> Result<()> {
        self.mem.queue(queue)?;
        if !is_dead_letter_queue(queue) {
            return Err(Error::Invalid(format!(
                "{queue} is not a dead-letter queue: requeue {queue}{DEAD_LETTER_SUFFIX} instead"
            )));
        }
        Ok(())
    }

    /// Up to `limit` messages of a dead-letter queue after the (lane, seq) cursor, and the cursor
    /// to continue from (`None` when this was the last chunk).
    fn dead_letters_after(
        &self,
        queue: &str,
        after: Cursor,
        limit: usize,
    ) -> Result<(Vec<Option<Ulid>>, Option<Cursor>)> {
        let mut ids = Vec::new();
        let mut last = after;
        for lane in after.0..LANES as u8 {
            let from = if lane == after.0 { after.1 + 1 } else { 0 };
            for entry in self
                .t
                .log
                .range((queue, lane, from)..=(queue, lane, u64::MAX))?
            {
                if ids.len() == limit {
                    return Ok((ids, Some(last)));
                }
                let (key, value) = entry?;
                last = (lane, key.value().2);
                ids.push(Some(Ulid(value.value().0)));
            }
        }
        Ok((ids, None))
    }

    fn requeue(&mut self, queue: &str, ids: Vec<Option<Ulid>>) -> Result<Requeued> {
        let mut result = Requeued::default();
        for id in ids {
            let message = match id {
                Some(id) => self.message(id)?.filter(|m| m.queue == queue),
                None => None,
            };
            match message {
                Some(message) if self.give_back(&message)? => result.requeued += 1,
                _ => result.skipped += 1,
            }
        }
        Ok(result)
    }

    /// Gives a dead letter back to the consumer that failed it, as a private copy unless the
    /// consumer gets the original from the log again anyway (after a replay). False when that's
    /// not possible.
    fn give_back(&mut self, message: &Message) -> Result<bool> {
        let Some(dead) = &message.dead_letter else {
            return Ok(false);
        };
        let Some(original) = ids::parse(&dead.message_id) else {
            return Ok(false);
        };
        let (queue, consumer) = (dead.queue.as_str(), dead.consumer.as_str());
        let Ok(target) = self.mem.consumer(queue, consumer) else {
            return Ok(false);
        };
        let positions = target.record.positions;
        if self.pending(queue, consumer, original)?.is_some() {
            return Ok(false);
        }
        if !comes_again(&self.t.messages, queue, original, &positions)? {
            let body = self.body(message.id)?.unwrap_or_default();
            let copy = private_copy(message, dead, original);
            self.insert_copy(queue, consumer, &copy, &body)?;
            let error = dead.last_error.clone();
            let pending = Pending::requeued(original, message.priority, error, self.now);
            self.insert_pending(queue, consumer, &pending)?;
        }
        self.remove_message(message, true)?;
        Ok(true)
    }
}

/// The dead letter as it goes back to the consumer that failed it, under its original id.
fn private_copy(message: &Message, dead: &DeadLetter, original: Ulid) -> Message {
    Message {
        id: original,
        queue: dead.queue.clone(),
        visible_at: None,
        seq: None,
        deliver_at: None,
        dead_letter: None,
        ..message.clone()
    }
}

#[cfg(test)]
#[path = "deadletters_tests.rs"]
mod tests;
