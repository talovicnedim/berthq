//! What happens to a consumer's message after a delivery: done (`finish`), or failed (`fail`):
//! retried after a backoff, dead-lettered, or dropped. Other consumers are not affected.

use crate::error::Result;
use crate::records::{DeadLetter, DeadLetterReason, Message, Pending, Slot};
use crate::settings::is_dead_letter_queue;
use crate::tx::Tx;

const MAX_ERROR_LEN: usize = 2048;

/// How a failed delivery (a nack or an expired lease) should be handled.
pub(crate) struct Failure {
    pub delay_ms: Option<u64>,
    pub dead_letter: bool,
    pub error: Option<String>,
}

impl Tx<'_, '_> {
    /// The consumer is done with a message: forget it (and its private copy).
    pub fn finish(&mut self, queue: &str, consumer: &str, pending: &Pending) -> Result<()> {
        self.delete_pending(queue, consumer, pending)?;
        if pending.is_copy() {
            self.remove_copy(queue, consumer, pending.id)?;
        }
        self.floors_moved(queue);
        Ok(())
    }

    /// The message a pending record refers to: in the log, or the consumer's private copy.
    pub fn pending_message(
        &self,
        queue: &str,
        consumer: &str,
        pending: &Pending,
    ) -> Result<Option<Message>> {
        if pending.is_copy() {
            self.copy(queue, consumer, pending.id)
        } else {
            self.message(pending.id)
        }
    }

    /// Handles a failed delivery: retry later, dead-letter, or drop for this consumer.
    pub fn fail(
        &mut self,
        queue: &str,
        consumer: &str,
        pending: Pending,
        failure: &Failure,
    ) -> Result<()> {
        let settings = self.mem.queue(queue)?.settings.clone();
        let Some(message) = self.pending_message(queue, consumer, &pending)? else {
            return self.delete_pending(queue, consumer, &pending);
        };
        let last_error = match &failure.error {
            Some(error) => Some(truncate(error, MAX_ERROR_LEN)),
            None => pending.last_error.clone(),
        };
        let exhausted = failure.dead_letter
            || (settings.max_attempts > 0 && pending.attempts >= settings.max_attempts);
        if !exhausted {
            let delay = failure
                .delay_ms
                .unwrap_or_else(|| settings.retry_delay(pending.attempts));
            return self.retry(queue, consumer, &pending, last_error, delay);
        }
        // A dead-letter queue has no dead-letter queue of its own.
        if is_dead_letter_queue(queue) {
            self.finish(queue, consumer, &pending)?;
            self.mem.queue_mut(queue)?.counters.dropped += 1;
            return Ok(());
        }
        let reason = if failure.dead_letter {
            DeadLetterReason::Rejected
        } else {
            DeadLetterReason::MaxAttempts
        };
        let dead_letter = DeadLetter {
            queue: queue.to_string(),
            consumer: consumer.to_string(),
            reason,
            at: self.now,
            message_id: message.id.to_string(),
            attempts: pending.attempts,
            last_error,
        };
        self.give_up(&pending, message, dead_letter)
    }

    /// The consumer is out of attempts: it forgets the message and a copy goes to the
    /// dead-letter queue.
    fn give_up(&mut self, pending: &Pending, message: Message, dead: DeadLetter) -> Result<()> {
        let (queue, consumer) = (dead.queue.clone(), dead.consumer.clone());
        // Read before `finish`, which deletes a private copy with its body.
        let body = if pending.is_copy() {
            self.copy_body(&queue, &consumer, pending.id)?
        } else {
            self.body(pending.id)?
        };
        self.finish(&queue, &consumer, pending)?;
        self.dead_letter(message, dead, &body.unwrap_or_default())
    }

    fn retry(
        &mut self,
        queue: &str,
        consumer: &str,
        pending: &Pending,
        last_error: Option<String>,
        delay: u64,
    ) -> Result<()> {
        let mut after = pending.clone();
        after.lease = None;
        after.last_error = last_error;
        after.ready_at = self.now + delay;
        after.slot = if delay > 0 {
            Slot::Scheduled
        } else {
            Slot::Ready
        };
        self.update_pending(queue, consumer, pending, &after)?;
        self.mem.queue_mut(queue)?.counters.retried += 1;
        Ok(())
    }

    /// Stores a copy in the dead-letter queue, created on demand.
    fn dead_letter(
        &mut self,
        message: Message,
        dead_letter: DeadLetter,
        body: &[u8],
    ) -> Result<()> {
        let queue = dead_letter.queue.clone();
        let dlq = self.ensure_dead_letter_queue(&queue)?;
        let copy = Message {
            id: self.mem.ids.next(self.now),
            queue: dlq.clone(),
            visible_at: None,
            seq: None,
            deliver_at: None,
            dead_letter: Some(dead_letter),
            ..message
        };
        self.store_message(copy, body)?;
        self.mem.queue_mut(&queue)?.counters.dead_lettered += 1;
        self.mem.queue_mut(&dlq)?.counters.published += 1;
        Ok(())
    }
}

fn truncate(text: &str, max: usize) -> String {
    if text.len() <= max {
        return text.to_string();
    }
    let mut end = max;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_string()
}
