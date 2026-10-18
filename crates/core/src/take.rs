//! Handing messages to a consumer: due retries and new messages after its position, highest
//! priority first. With a lease each one becomes pending; with `autoAck` the position just moves.

use serde_json::value::RawValue;
use ulid::Ulid;

use crate::delivery::Delivery;
use crate::error::{Error, Result};
use crate::ids;
use crate::positions::{Next, Place, Start, next_up};
use crate::records::{Lease, Message, Pending, Slot};
use crate::settle::lease_token;
use crate::time;
use crate::tx::Tx;

/// A checked consume request.
#[derive(Debug, Clone)]
pub(crate) struct Take {
    pub consumer: String,
    pub from: Start,
    pub max: usize,
    pub wait_ms: u64,
    pub visibility_ms: Option<u64>,
    pub auto_ack: bool,
    pub worker: String,
}

impl Tx<'_, '_> {
    /// Hands up to `take.max` messages to the consumer, creating it first if needed.
    pub(crate) fn take(&mut self, queue: &str, take: &Take) -> Result<Vec<Delivery>> {
        if self.mem.closing {
            return Err(Error::Unavailable);
        }
        self.ensure_consumer(queue, &take.consumer, take.from)?;
        let (now, consumer) = (self.now, take.consumer.as_str());
        let queue_memory = self.mem.queue_mut(queue)?;
        let settings = queue_memory.settings.clone();
        let memory = queue_memory
            .consumers
            .get_mut(consumer)
            .ok_or_else(|| Error::NotFound(format!("consumer {consumer}")))?;
        memory.record.last_seen = Some(now);
        let paused = settings.paused || memory.record.paused;
        self.consumer_changed(queue, consumer);
        if paused {
            return Ok(Vec::new());
        }

        let next = self.next_for(queue, consumer, take.max)?;
        // Fewer than asked means every lane was read to its end: the position can skip over
        // messages deleted in between, so it doesn't look like there is more to come.
        let caught_up = next.len() < take.max;
        let visibility = take.visibility_ms.unwrap_or(settings.visibility_timeout_ms);
        let mut deliveries = Vec::with_capacity(next.len());
        for item in next {
            let delivered = match item {
                Next::Retry(id) => self.take_retry(queue, take, id, visibility)?,
                Next::New { lane, seq, id } => {
                    self.take_new(queue, take, lane, seq, id, visibility)?
                }
            };
            deliveries.extend(delivered);
        }

        if caught_up {
            let heads = self.mem.queue(queue)?.counts.heads;
            self.mem.consumer_mut(queue, consumer)?.record.positions = heads;
        }
        self.count_deliveries(queue, take, deliveries.len())?;
        Ok(deliveries)
    }

    /// What the consumer receives next: due retries, then new messages.
    fn next_for(&self, queue: &str, consumer: &str, max: usize) -> Result<Vec<Next>> {
        let queue_memory = self.mem.queue(queue)?;
        let record = &self.mem.consumer(queue, consumer)?.record;
        let place = Place {
            queue,
            consumer,
            record,
            heads: &queue_memory.counts.heads,
        };
        next_up(&self.t.p_ready, &self.t.log, &place, max, true)
    }

    fn count_deliveries(&mut self, queue: &str, take: &Take, count: usize) -> Result<()> {
        let queue_memory = self.mem.queue_mut(queue)?;
        queue_memory.counters.delivered += count as u64;
        if take.auto_ack {
            queue_memory.counters.acked += count as u64;
            self.floors_moved(queue);
        }
        Ok(())
    }

    fn take_new(
        &mut self,
        queue: &str,
        take: &Take,
        lane: u8,
        seq: u64,
        id: Ulid,
        visibility: u64,
    ) -> Result<Option<Delivery>> {
        let consumer = take.consumer.as_str();
        self.mem.consumer_mut(queue, consumer)?.record.positions[usize::from(lane)] = seq;
        let Some(message) = self.message(id)? else {
            return Ok(None);
        };
        let body = self.body(id)?;
        if take.auto_ack {
            let handed = Handed::unleased(1, None);
            return Ok(Some(delivery(queue, consumer, &message, &handed, body)));
        }
        let lease = self.lease(take, visibility);
        let pending = Pending {
            id,
            slot: Slot::Leased,
            attempts: 1,
            priority: message.priority,
            seq: Some(seq),
            ready_at: self.now,
            lease: Some(lease.clone()),
            last_error: None,
        };
        self.insert_pending(queue, consumer, &pending)?;
        let handed = Handed::leased(1, &lease, visibility, None);
        Ok(Some(delivery(queue, consumer, &message, &handed, body)))
    }

    fn take_retry(
        &mut self,
        queue: &str,
        take: &Take,
        id: Ulid,
        visibility: u64,
    ) -> Result<Option<Delivery>> {
        let consumer = take.consumer.as_str();
        let Some(pending) = self.pending(queue, consumer, id)? else {
            return Ok(None);
        };
        let Some(message) = self.pending_message(queue, consumer, &pending)? else {
            self.delete_pending(queue, consumer, &pending)?;
            return Ok(None);
        };
        let body = if pending.is_copy() {
            self.copy_body(queue, consumer, id)?
        } else {
            self.body(id)?
        };
        let (attempt, error) = (pending.attempts + 1, pending.last_error.clone());
        if take.auto_ack {
            self.finish(queue, consumer, &pending)?;
            let handed = Handed::unleased(attempt, error);
            return Ok(Some(delivery(queue, consumer, &message, &handed, body)));
        }
        let lease = self.lease(take, visibility);
        let mut leased = pending.clone();
        leased.slot = Slot::Leased;
        leased.attempts = attempt;
        leased.lease = Some(lease.clone());
        self.update_pending(queue, consumer, &pending, &leased)?;
        let handed = Handed::leased(attempt, &lease, visibility, error);
        Ok(Some(delivery(queue, consumer, &message, &handed, body)))
    }

    fn lease(&self, take: &Take, visibility: u64) -> Lease {
        Lease {
            worker: take.worker.clone(),
            deadline: self.now + visibility,
            token: ids::random_u64(),
        }
    }
}

/// How a message was handed out: which attempt, and under which lease.
struct Handed<'a> {
    attempt: u32,
    lease: Option<&'a Lease>,
    visibility_ms: u64,
    last_error: Option<String>,
}

impl<'a> Handed<'a> {
    fn leased(attempt: u32, lease: &'a Lease, visibility_ms: u64, error: Option<String>) -> Self {
        Self {
            attempt,
            lease: Some(lease),
            visibility_ms,
            last_error: error,
        }
    }

    fn unleased(attempt: u32, error: Option<String>) -> Self {
        Self {
            attempt,
            lease: None,
            visibility_ms: 0,
            last_error: error,
        }
    }
}

fn delivery(
    queue: &str,
    consumer: &str,
    message: &Message,
    handed: &Handed,
    body: Option<Vec<u8>>,
) -> Delivery {
    let lease = handed.lease;
    Delivery {
        id: message.id.to_string(),
        lease: lease.map(|lease| lease_token(message.id, handed.attempt, lease.token, consumer)),
        queue: queue.to_string(),
        consumer: consumer.to_string(),
        body: parse_body(message.id, body.as_deref()),
        priority: message.priority,
        attempt: handed.attempt,
        published_at: time::format(message.created_at),
        lease_expires_at: lease.map(|lease| time::format(lease.deadline)),
        visibility_timeout_ms: handed.visibility_ms,
        dedup_id: message.dedup_id.clone(),
        last_error: handed.last_error.clone(),
    }
}

/// The stored body as sent. Never a tree: that can take dozens of times the body's size.
pub(crate) fn parse_body(id: Ulid, body: Option<&[u8]>) -> Box<RawValue> {
    let Some(body) = body else {
        tracing::warn!("body of message {id} is missing on disk");
        return RawValue::NULL.to_owned();
    };
    match serde_json::from_slice::<&RawValue>(body) {
        Ok(raw) => raw.to_owned(),
        Err(err) => {
            tracing::warn!("body of message {id} is not valid json: {err}");
            RawValue::NULL.to_owned()
        }
    }
}
