//! The tables in the redb file. All keyed by queue first, so each queue is a contiguous range.
//!
//! - `log` (queue, lane, seq): visible messages, one sequence per priority lane
//! - `delayed` (queue, deliver at, id): messages waiting for their `deliverAt`
//! - `pending` (queue, consumer, id): what a consumer is processing or retrying, with the
//!   `pending_ready`, `pending_scheduled`, `pending_leased` and `pending_floor` indexes
//! - `copies` (queue, consumer, id): requeued dead letters, private to one consumer

use redb::{Table, TableDefinition, WriteTransaction};

use crate::error::Result;

pub(crate) const META: TableDefinition<&str, u64> = TableDefinition::new("meta");
pub(crate) const QUEUES: TableDefinition<&str, &[u8]> = TableDefinition::new("queues");
pub(crate) const COUNTS: TableDefinition<&str, &[u8]> = TableDefinition::new("counts");
pub(crate) const CONSUMERS: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("consumers");
pub(crate) const MESSAGES: TableDefinition<u128, &[u8]> = TableDefinition::new("messages");
pub(crate) const BODIES: TableDefinition<u128, &[u8]> = TableDefinition::new("bodies");
/// (queue, lane, seq) → (id, visible at, body size).
pub(crate) const LOG: TableDefinition<(&str, u8, u64), LogEntry> = TableDefinition::new("log");
pub(crate) const DELAYED: TableDefinition<(&str, u64, u128), ()> = TableDefinition::new("delayed");
/// (queue, dedup id) → (message id, expires at).
pub(crate) const DEDUP: TableDefinition<(&str, &str), (u128, u64)> = TableDefinition::new("dedup");
pub(crate) const DEDUP_EXPIRING: TableDefinition<(&str, u64, &str), ()> =
    TableDefinition::new("dedup_expiring");
pub(crate) const PENDING: TableDefinition<(&str, &str, u128), &[u8]> =
    TableDefinition::new("pending");
/// (queue, consumer, lane, id): retries that are due and requeued copies, in delivery order.
pub(crate) const P_READY: TableDefinition<(&str, &str, u8, u128), ()> =
    TableDefinition::new("pending_ready");
pub(crate) const P_SCHEDULED: TableDefinition<(&str, &str, u64, u128), ()> =
    TableDefinition::new("pending_scheduled");
pub(crate) const P_LEASED: TableDefinition<(&str, &str, u64, u128), ()> =
    TableDefinition::new("pending_leased");
/// (queue, consumer, lane, seq) of every pending log message: the lowest one per lane is how far
/// back the consumer still holds messages (see `retention.rs`).
pub(crate) const P_FLOOR: TableDefinition<(&str, &str, u8, u64), ()> =
    TableDefinition::new("pending_floor");
pub(crate) const COPIES: TableDefinition<(&str, &str, u128), &[u8]> =
    TableDefinition::new("copies");
pub(crate) const COPY_BODIES: TableDefinition<(&str, &str, u128), &[u8]> =
    TableDefinition::new("copy_bodies");

pub(crate) type LogEntry = (u128, u64, u32);

pub(crate) struct Tables<'txn> {
    pub meta: Table<'txn, &'static str, u64>,
    pub queues: Table<'txn, &'static str, &'static [u8]>,
    pub counts: Table<'txn, &'static str, &'static [u8]>,
    pub consumers: Table<'txn, (&'static str, &'static str), &'static [u8]>,
    pub messages: Table<'txn, u128, &'static [u8]>,
    pub bodies: Table<'txn, u128, &'static [u8]>,
    pub log: Table<'txn, (&'static str, u8, u64), LogEntry>,
    pub delayed: Table<'txn, (&'static str, u64, u128), ()>,
    pub dedup: Table<'txn, (&'static str, &'static str), (u128, u64)>,
    pub dedup_expiring: Table<'txn, (&'static str, u64, &'static str), ()>,
    pub pending: Table<'txn, (&'static str, &'static str, u128), &'static [u8]>,
    pub p_ready: Table<'txn, (&'static str, &'static str, u8, u128), ()>,
    pub p_scheduled: Table<'txn, (&'static str, &'static str, u64, u128), ()>,
    pub p_leased: Table<'txn, (&'static str, &'static str, u64, u128), ()>,
    pub p_floor: Table<'txn, (&'static str, &'static str, u8, u64), ()>,
    pub copies: Table<'txn, (&'static str, &'static str, u128), &'static [u8]>,
    pub copy_bodies: Table<'txn, (&'static str, &'static str, u128), &'static [u8]>,
}

impl<'txn> Tables<'txn> {
    /// Opens every table, creating the missing ones.
    pub fn open(txn: &'txn WriteTransaction) -> Result<Self> {
        Ok(Self {
            meta: txn.open_table(META)?,
            queues: txn.open_table(QUEUES)?,
            counts: txn.open_table(COUNTS)?,
            consumers: txn.open_table(CONSUMERS)?,
            messages: txn.open_table(MESSAGES)?,
            bodies: txn.open_table(BODIES)?,
            log: txn.open_table(LOG)?,
            delayed: txn.open_table(DELAYED)?,
            dedup: txn.open_table(DEDUP)?,
            dedup_expiring: txn.open_table(DEDUP_EXPIRING)?,
            pending: txn.open_table(PENDING)?,
            p_ready: txn.open_table(P_READY)?,
            p_scheduled: txn.open_table(P_SCHEDULED)?,
            p_leased: txn.open_table(P_LEASED)?,
            p_floor: txn.open_table(P_FLOOR)?,
            copies: txn.open_table(COPIES)?,
            copy_bodies: txn.open_table(COPY_BODIES)?,
        })
    }
}
