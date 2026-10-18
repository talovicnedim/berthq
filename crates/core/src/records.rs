//! What is stored in the tables, as JSON: messages, pending deliveries, consumers and queues.

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::publish::MAX_PRIORITY;
use crate::settings::QueueSettings;

/// One lane per priority: lane 0 holds priority 9, so lanes are read in ascending order.
pub(crate) const LANES: usize = MAX_PRIORITY as usize + 1;

pub(crate) fn lane(priority: u8) -> u8 {
    MAX_PRIORITY - priority.min(MAX_PRIORITY)
}

/// Message metadata, stored in `messages` (or `copies`); the body is stored separately.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Message {
    #[serde(skip, default = "Ulid::nil")]
    pub id: Ulid,
    pub queue: String,
    pub priority: u8,
    pub created_at: u64,
    /// When it joined the log; `None` while delayed and for private copies.
    pub visible_at: Option<u64>,
    /// Its place in its lane once visible.
    pub seq: Option<u64>,
    /// Set while the message is delayed.
    pub deliver_at: Option<u64>,
    pub dedup_id: Option<String>,
    /// Body size in bytes.
    pub size: u32,
    /// Set on messages in a dead-letter queue.
    pub dead_letter: Option<DeadLetter>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeadLetter {
    /// Queue and consumer that failed it; `requeue` gives it back to them.
    pub queue: String,
    pub consumer: String,
    pub reason: DeadLetterReason,
    pub at: u64,
    /// Id of the message in `queue`, kept when it is requeued.
    pub message_id: String,
    pub attempts: u32,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum DeadLetterReason {
    /// Failed `maxAttempts` times.
    MaxAttempts,
    /// A worker nacked it with `deadLetter: true`.
    Rejected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum Slot {
    /// A retry that is due, or a requeued copy: delivered before newer messages.
    Ready,
    /// Waiting for its retry backoff to end.
    Scheduled,
    /// Delivered, waiting for an ack.
    Leased,
}

/// A message a consumer has received but not finished, stored in `pending`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Pending {
    #[serde(skip, default = "Ulid::nil")]
    pub id: Ulid,
    pub slot: Slot,
    /// Deliveries so far, including the current one while leased.
    pub attempts: u32,
    pub priority: u8,
    /// The message's place in the log; `None` for a private copy.
    pub seq: Option<u64>,
    /// When a scheduled retry becomes ready.
    pub ready_at: u64,
    pub lease: Option<Lease>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Lease {
    pub worker: String,
    pub deadline: u64,
    /// Random per delivery, so a lease from before a replay never matches a new delivery.
    pub token: u64,
}

impl Pending {
    /// A private copy as `requeue` leaves it: ready for its first delivery.
    pub fn requeued(id: Ulid, priority: u8, last_error: Option<String>, now: u64) -> Pending {
        Pending {
            id,
            slot: Slot::Ready,
            attempts: 0,
            priority,
            seq: None,
            ready_at: now,
            lease: None,
            last_error,
        }
    }

    pub fn is_copy(&self) -> bool {
        self.seq.is_none()
    }

    pub fn lane(&self) -> u8 {
        lane(self.priority)
    }
}

/// A consumer's position and settings, stored in `consumers`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ConsumerRecord {
    pub created_at: u64,
    pub paused: bool,
    /// Per lane: the last sequence delivered to this consumer.
    pub positions: [u64; LANES],
    /// Pending messages by slot: leased, waiting for their backoff, and due retries.
    pub processing: u64,
    pub scheduled: u64,
    pub ready: u64,
    pub last_seen: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct QueueRecord {
    pub name: String,
    pub settings: QueueSettings,
    pub created_at: u64,
}

/// How many messages a queue holds, saved in `counts` with every change.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct QueueCounts {
    /// Visible messages in the log.
    pub log: u64,
    pub delayed: u64,
    /// Requeued dead letters, private to one consumer.
    pub copies: u64,
    /// Sum of body sizes in bytes.
    pub bytes: u64,
    /// Per lane: the last sequence handed out. Never reused, so positions stay valid.
    pub heads: [u64; LANES],
}

impl ConsumerRecord {
    /// Messages received before that wait for their next attempt, due or not.
    pub fn retrying(&self) -> u64 {
        self.ready + self.scheduled
    }
}

impl QueueCounts {
    pub fn messages(&self) -> u64 {
        self.log + self.delayed + self.copies
    }
}
