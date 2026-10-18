//! Browsing stored messages without consuming them, and deleting single messages.

use redb::ReadTransaction;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use ulid::Ulid;

use crate::broker::Broker;
use crate::error::{Error, Result};
use crate::ids;
use crate::positions::{Start, positions};
use crate::records::{DeadLetterReason, LANES, Message};
use crate::tables::{BODIES, DELAYED, LOG, MESSAGES};
use crate::take::parse_body;
use crate::time;

/// Most messages returned by one listing.
pub const MAX_LIST: u32 = 500;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MessageState {
    Visible,
    Delayed,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageInfo {
    pub id: String,
    pub queue: String,
    pub state: MessageState,
    pub priority: u8,
    pub published_at: String,
    /// When a delayed message becomes visible.
    pub deliver_at: Option<String>,
    pub dedup_id: Option<String>,
    pub size: u32,
    /// For dead letters: deliveries to the consumer that failed it.
    pub attempts: u32,
    pub last_error: Option<String>,
    pub dead_letter: Option<MessageDeadLetter>,
    /// Only included when fetching a single message.
    pub body: Option<Box<RawValue>>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageDeadLetter {
    /// Queue and consumer that failed it.
    pub queue: String,
    pub consumer: String,
    /// The message's id in that queue; requeueing gives it back under this id.
    pub message_id: String,
    pub reason: DeadLetterReason,
    pub at: String,
}

impl Broker {
    /// Up to `limit` stored messages, newest first; `before` (a message id) pages to older ones.
    pub fn messages_list(
        &self,
        queue: &str,
        state: Option<MessageState>,
        limit: Option<u32>,
        before: Option<&str>,
    ) -> Result<Vec<MessageInfo>> {
        let heads = self.memory().queue(queue)?.counts.heads;
        let limit = limit.unwrap_or(50).min(MAX_LIST) as usize;
        let before = match before {
            Some(id) => Some(ids::parse(id).ok_or_else(|| gone(id, queue))?),
            None => None,
        };
        let txn = self.store().read()?;
        let ids = match state.unwrap_or(MessageState::Visible) {
            MessageState::Visible => newest_visible(&txn, queue, &heads, limit, before)?,
            MessageState::Delayed => newest_delayed(&txn, queue, limit, before)?,
        };
        let messages = txn.open_table(MESSAGES)?;
        let mut infos = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(record) = messages.get(id.0)? {
                let mut message: Message = serde_json::from_slice(record.value())?;
                message.id = id;
                infos.push(info(&message));
            }
        }
        Ok(infos)
    }

    pub fn messages_get(&self, queue: &str, id: &str) -> Result<MessageInfo> {
        let not_found = || Error::NotFound(format!("message {id}"));
        let ulid = ids::parse(id).ok_or_else(not_found)?;
        let txn = self.store().read()?;
        let message = read(&txn, ulid)?
            .filter(|m| m.queue == queue)
            .ok_or_else(not_found)?;
        let body = txn.open_table(BODIES)?.get(ulid.0)?;
        let mut info = info(&message);
        info.body = Some(parse_body(ulid, body.as_ref().map(|b| b.value())));
        Ok(info)
    }

    /// Deletes one message for every consumer; leases on it become unknown.
    pub async fn messages_remove(&self, queue: &str, id: &str) -> Result<()> {
        let not_found = || Error::NotFound(format!("message {id}"));
        let ulid = ids::parse(id).ok_or_else(not_found)?;
        let (queue, id) = (queue.to_string(), id.to_string());
        self.store()
            .run(move |tx| {
                let message = tx
                    .message(ulid)?
                    .filter(|m| m.queue == queue)
                    .ok_or_else(|| Error::NotFound(format!("message {id}")))?;
                tx.remove_message(&message, true)
            })
            .await
    }
}

/// Ids of the newest visible messages across lanes, older than `before` when given.
fn newest_visible(
    txn: &ReadTransaction,
    queue: &str,
    heads: &[u64; LANES],
    limit: usize,
    before: Option<Ulid>,
) -> Result<Vec<Ulid>> {
    let log = txn.open_table(LOG)?;
    let cursor = match before {
        Some(id) => {
            let message = read(txn, id)?.filter(|m| m.queue == queue);
            let at = message.and_then(|m| m.visible_at);
            Some((at.ok_or_else(|| gone(&id.to_string(), queue))?, id.0))
        }
        None => None,
    };
    // Per lane, where entries no newer than the cursor end.
    let ends = match cursor {
        Some((at, _)) => positions(
            &log,
            &txn.open_table(MESSAGES)?,
            queue,
            heads,
            Start::Time(at + 1),
        )?,
        None => *heads,
    };
    let mut found: Vec<(u64, u128)> = Vec::new();
    for (lane, end) in ends.iter().enumerate() {
        let range = (queue, lane as u8, 0)..=(queue, lane as u8, *end);
        let mut taken = 0;
        for entry in log.range(range)?.rev() {
            let (id, visible_at, _) = entry?.1.value();
            if cursor.is_some_and(|cursor| (visible_at, id) >= cursor) {
                continue;
            }
            found.push((visible_at, id));
            taken += 1;
            if taken == limit {
                break;
            }
        }
    }
    found.sort_unstable_by(|a, b| b.cmp(a));
    Ok(found
        .into_iter()
        .take(limit)
        .map(|(_, id)| Ulid(id))
        .collect())
}

/// Ids of delayed messages, latest `deliverAt` first.
fn newest_delayed(
    txn: &ReadTransaction,
    queue: &str,
    limit: usize,
    before: Option<Ulid>,
) -> Result<Vec<Ulid>> {
    let upper = match before {
        Some(id) => {
            let message = read(txn, id)?.filter(|m| m.queue == queue);
            let at = message.and_then(|m| m.deliver_at);
            let at = at.ok_or_else(|| gone(&id.to_string(), queue))?;
            std::ops::Bound::Excluded((queue, at, id.0))
        }
        None => std::ops::Bound::Included((queue, u64::MAX, u128::MAX)),
    };
    let lower = std::ops::Bound::Included((queue, 0, 0));
    let delayed = txn.open_table(DELAYED)?;
    let mut ids = Vec::new();
    for entry in delayed
        .range::<(&str, u64, u128)>((lower, upper))?
        .rev()
        .take(limit)
    {
        ids.push(Ulid(entry?.0.value().2));
    }
    Ok(ids)
}

fn read(txn: &ReadTransaction, id: Ulid) -> Result<Option<Message>> {
    let Some(record) = txn.open_table(MESSAGES)?.get(id.0)? else {
        return Ok(None);
    };
    let mut message: Message = serde_json::from_slice(record.value())?;
    message.id = id;
    Ok(Some(message))
}

fn gone(id: &str, queue: &str) -> Error {
    Error::Invalid(format!("message {id} is no longer kept in queue {queue}"))
}

pub(crate) fn info(message: &Message) -> MessageInfo {
    let state = match message.deliver_at {
        Some(_) => MessageState::Delayed,
        None => MessageState::Visible,
    };
    let dead = message.dead_letter.as_ref();
    MessageInfo {
        id: message.id.to_string(),
        queue: message.queue.clone(),
        state,
        priority: message.priority,
        published_at: time::format(message.created_at),
        deliver_at: message.deliver_at.map(time::format),
        dedup_id: message.dedup_id.clone(),
        size: message.size,
        attempts: dead.map_or(0, |d| d.attempts),
        last_error: dead.and_then(|d| d.last_error.clone()),
        dead_letter: dead.map(|d| MessageDeadLetter {
            queue: d.queue.clone(),
            consumer: d.consumer.clone(),
            message_id: d.message_id.clone(),
            reason: d.reason,
            at: time::format(d.at),
        }),
        body: None,
    }
}
