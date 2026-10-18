//! Where a consumer starts or replays from (`from`), and what it receives next. These read
//! through `ReadableTable`, so they work in a write job and in a read transaction alike.

use redb::ReadableTable;
use ulid::Ulid;

use crate::error::{Error, Result};
use crate::ids;
use crate::records::{ConsumerRecord, LANES, Message, lane};
use crate::tables::LogEntry;
use crate::time;

/// The log of every queue: (queue, lane, seq) → entry.
pub(crate) trait LogTable: ReadableTable<(&'static str, u8, u64), LogEntry> {}
impl<T: ReadableTable<(&'static str, u8, u64), LogEntry>> LogTable for T {}

/// Due retries and requeued copies: (queue, consumer, lane, id).
pub(crate) trait ReadyTable:
    ReadableTable<(&'static str, &'static str, u8, u128), ()>
{
}
impl<T: ReadableTable<(&'static str, &'static str, u8, u128), ()>> ReadyTable for T {}

/// Where a new consumer starts, or where a replay goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Start {
    /// The oldest kept message.
    Oldest,
    /// Only messages published from now on.
    Newest,
    /// Messages that became visible at or after this time.
    Time(u64),
    /// This message and everything after it.
    Message(Ulid),
}

/// A consumer's place in its queue: what `next_up` needs besides the tables.
#[derive(Clone, Copy)]
pub(crate) struct Place<'a> {
    pub queue: &'a str,
    pub consumer: &'a str,
    pub record: &'a ConsumerRecord,
    pub heads: &'a [u64; LANES],
}

/// Something a consumer receives next, in delivery order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Next {
    /// A pending message of this consumer: a due retry or a requeued copy.
    Retry(Ulid),
    /// A log message after the consumer's position.
    New { lane: u8, seq: u64, id: Ulid },
}

impl Start {
    pub fn parse(from: Option<&str>) -> Result<Start> {
        match from {
            None | Some("start") => Ok(Start::Oldest),
            Some("end") => Ok(Start::Newest),
            Some(text) => time::parse(text)
                .map(Start::Time)
                .or_else(|| ids::parse(text).map(Start::Message))
                .ok_or_else(|| {
                    Error::Invalid(format!(
                        "invalid from {text}: use start, end, an RFC 3339 time or a message id"
                    ))
                }),
        }
    }
}

/// Per lane, the oldest sequence still in the log (`head + 1` when the lane is empty).
pub(crate) fn tails(
    log: &impl LogTable,
    queue: &str,
    heads: &[u64; LANES],
) -> Result<[u64; LANES]> {
    let mut tails = [0; LANES];
    for (lane, tail) in tails.iter_mut().enumerate() {
        *tail = match first_at_or_after(log, queue, lane as u8, 0)? {
            Some((seq, _)) => seq,
            None => heads[lane] + 1,
        };
    }
    Ok(tails)
}

/// Positions (the last sequence delivered, per lane) of a consumer starting at `start`.
pub(crate) fn positions(
    log: &impl LogTable,
    messages: &impl ReadableTable<u128, &'static [u8]>,
    queue: &str,
    heads: &[u64; LANES],
    start: Start,
) -> Result<[u64; LANES]> {
    let mut positions = *heads;
    match start {
        Start::Newest => {}
        Start::Oldest => {
            let tails = tails(log, queue, heads)?;
            for lane in 0..LANES {
                positions[lane] = tails[lane] - 1;
            }
        }
        Start::Time(at) => {
            for (lane, position) in positions.iter_mut().enumerate() {
                if let Some(seq) = first_visible_at(log, queue, lane as u8, at, heads[lane])? {
                    *position = seq - 1;
                }
            }
        }
        Start::Message(id) => {
            let gone =
                || Error::Invalid(format!("message {id} is no longer kept in queue {queue}"));
            let (message_lane, seq, at) = kept(messages, queue, id)?.ok_or_else(gone)?;
            for (lane, position) in positions.iter_mut().enumerate() {
                if lane as u8 == message_lane {
                    *position = seq - 1;
                } else if let Some(seq) = first_visible_at(log, queue, lane as u8, at, heads[lane])?
                {
                    *position = seq - 1;
                }
            }
        }
    }
    Ok(positions)
}

/// What a consumer receives next, in delivery order: per lane (highest priority first) its due
/// retries, then new messages after its position. `retries: false` lists only new messages.
pub(crate) fn next_up(
    ready: &impl ReadyTable,
    log: &impl LogTable,
    place: &Place,
    limit: usize,
    retries: bool,
) -> Result<Vec<Next>> {
    let Place {
        queue,
        consumer,
        record,
        heads,
    } = *place;
    let mut next = Vec::new();
    for lane in 0..LANES as u8 {
        if retries && record.ready > 0 {
            let range = (queue, consumer, lane, 0)..=(queue, consumer, lane, u128::MAX);
            for entry in ready.range(range)?.take(limit - next.len()) {
                next.push(Next::Retry(Ulid(entry?.0.value().3)));
            }
        }
        let position = record.positions[usize::from(lane)];
        if heads[usize::from(lane)] > position {
            let range = (queue, lane, position + 1)..=(queue, lane, u64::MAX);
            for entry in log.range(range)?.take(limit - next.len()) {
                let (key, value) = entry?;
                let (seq, id) = (key.value().2, Ulid(value.value().0));
                next.push(Next::New { lane, seq, id });
            }
        }
        if next.len() >= limit {
            break;
        }
    }
    Ok(next)
}

/// Whether a consumer at `positions` gets message `id` from the log again: it is still kept in
/// `queue`, after the consumer's position in its lane.
pub(crate) fn comes_again(
    messages: &impl ReadableTable<u128, &'static [u8]>,
    queue: &str,
    id: Ulid,
    positions: &[u64; LANES],
) -> Result<bool> {
    let found = kept(messages, queue, id)?;
    Ok(found.is_some_and(|(lane, seq, _)| seq > positions[usize::from(lane)]))
}

/// The first log entry of a lane at or after `seq`.
fn first_at_or_after(
    log: &impl LogTable,
    queue: &str,
    lane: u8,
    seq: u64,
) -> Result<Option<(u64, LogEntry)>> {
    match log
        .range((queue, lane, seq)..=(queue, lane, u64::MAX))?
        .next()
    {
        Some(entry) => {
            let (key, value) = entry?;
            Ok(Some((key.value().2, value.value())))
        }
        None => Ok(None),
    }
}

/// The first sequence of a lane that became visible at or after `at`: a binary search, since
/// entries are appended in time order (some may have been deleted in between).
fn first_visible_at(
    log: &impl LogTable,
    queue: &str,
    lane: u8,
    at: u64,
    head: u64,
) -> Result<Option<u64>> {
    let Some((tail, _)) = first_at_or_after(log, queue, lane, 0)? else {
        return Ok(None);
    };
    // Every entry before `low` is older than `at`; the first entry at or after `high` isn't.
    let (mut low, mut high) = (tail, head + 1);
    while low < high {
        let mid = low + (high - low) / 2;
        match first_at_or_after(log, queue, lane, mid)? {
            Some((seq, (_, visible_at, _))) if visible_at < at => low = seq + 1,
            _ => high = mid,
        }
    }
    let found = first_at_or_after(log, queue, lane, low)?;
    Ok(found.filter(|(_, entry)| entry.1 >= at).map(|(seq, _)| seq))
}

/// Lane, sequence and visible time of a message still in `queue`'s log.
fn kept(
    messages: &impl ReadableTable<u128, &'static [u8]>,
    queue: &str,
    id: Ulid,
) -> Result<Option<(u8, u64, u64)>> {
    let Some(record) = messages.get(id.0)? else {
        return Ok(None);
    };
    let message: Message = serde_json::from_slice(record.value())?;
    match (message.queue == queue, message.seq, message.visible_at) {
        (true, Some(seq), Some(at)) => Ok(Some((lane(message.priority), seq, at))),
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_reads_start_end_times_and_message_ids() {
        assert_eq!(Start::parse(None).unwrap(), Start::Oldest);
        assert_eq!(Start::parse(Some("start")).unwrap(), Start::Oldest);
        assert_eq!(Start::parse(Some("end")).unwrap(), Start::Newest);
        assert_eq!(
            Start::parse(Some("2026-10-14T09:00:00Z")).unwrap(),
            Start::Time(1_791_968_400_000)
        );
        let id = Ulid::from_parts(1_000, 7);
        assert_eq!(
            Start::parse(Some(&id.to_string())).unwrap(),
            Start::Message(id)
        );
        assert!(matches!(Start::parse(Some("soon")), Err(Error::Invalid(_))));
    }
}
