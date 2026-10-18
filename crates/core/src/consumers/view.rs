//! What a consumer is doing: the messages it hasn't received yet, is processing, or retries.
//! Read from a snapshot, without the writer.

use redb::ReadTransaction;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::broker::Broker;
use crate::error::Result;
use crate::messages::MAX_LIST;
use crate::positions::{Next, Place, next_up};
use crate::records::{Message, Pending, Slot};
use crate::tables::{COPIES, LOG, MESSAGES, P_LEASED, P_READY, P_SCHEDULED, PENDING};
use crate::time;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ConsumerMessageState {
    /// Not received yet, in delivery order.
    Waiting,
    Processing,
    /// Received before, waiting for the next attempt (due or not).
    Retrying,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerMessage {
    pub id: String,
    pub queue: String,
    pub consumer: String,
    pub state: ConsumerMessageState,
    pub priority: u8,
    pub attempts: u32,
    pub published_at: String,
    pub last_error: Option<String>,
    /// Set while processing.
    pub worker: Option<String>,
    pub lease_expires_at: Option<String>,
    /// Set while retrying.
    pub retry_at: Option<String>,
    pub size: u32,
}

impl Broker {
    pub fn consumers_messages(
        &self,
        queue: &str,
        name: &str,
        state: Option<ConsumerMessageState>,
        limit: Option<u32>,
    ) -> Result<Vec<ConsumerMessage>> {
        let limit = limit.unwrap_or(50).min(MAX_LIST) as usize;
        let (record, heads) = {
            let memory = self.memory();
            let record = memory.consumer(queue, name)?.record.clone();
            (record, memory.queue(queue)?.counts.heads)
        };
        let txn = self.store().read()?;
        let state = state.unwrap_or(ConsumerMessageState::Processing);
        let ids = match state {
            ConsumerMessageState::Waiting => {
                let place = Place {
                    queue,
                    consumer: name,
                    record: &record,
                    heads: &heads,
                };
                let (ready, log) = (txn.open_table(P_READY)?, txn.open_table(LOG)?);
                let next = next_up(&ready, &log, &place, limit, false)?;
                let waiting = next.into_iter().filter_map(|next| match next {
                    Next::New { id, .. } => Some(id),
                    Next::Retry(_) => None,
                });
                return waiting
                    .filter_map(|id| waiting_message(&txn, queue, name, id).transpose())
                    .collect();
            }
            ConsumerMessageState::Processing => leased_ids(&txn, queue, name, limit)?,
            ConsumerMessageState::Retrying => retrying_ids(&txn, queue, name, limit)?,
        };
        ids.into_iter()
            .filter_map(|id| pending_message(&txn, queue, name, id).transpose())
            .collect()
    }
}

fn leased_ids(txn: &ReadTransaction, queue: &str, name: &str, limit: usize) -> Result<Vec<Ulid>> {
    let table = txn.open_table(P_LEASED)?;
    let range = (queue, name, 0, 0)..=(queue, name, u64::MAX, u128::MAX);
    let mut ids = Vec::new();
    for entry in table.range(range)?.take(limit) {
        ids.push(Ulid(entry?.0.value().3));
    }
    Ok(ids)
}

/// Due retries first (in delivery order), then the ones still waiting for their backoff.
fn retrying_ids(txn: &ReadTransaction, queue: &str, name: &str, limit: usize) -> Result<Vec<Ulid>> {
    let mut ids = Vec::new();
    let ready = txn.open_table(P_READY)?;
    let range = (queue, name, 0, 0)..=(queue, name, u8::MAX, u128::MAX);
    for entry in ready.range(range)?.take(limit) {
        ids.push(Ulid(entry?.0.value().3));
    }
    let scheduled = txn.open_table(P_SCHEDULED)?;
    let range = (queue, name, 0, 0)..=(queue, name, u64::MAX, u128::MAX);
    for entry in scheduled.range(range)?.take(limit - ids.len()) {
        ids.push(Ulid(entry?.0.value().3));
    }
    Ok(ids)
}

fn waiting_message(
    txn: &ReadTransaction,
    queue: &str,
    name: &str,
    id: Ulid,
) -> Result<Option<ConsumerMessage>> {
    let Some(message) = read_message(txn, id)? else {
        return Ok(None);
    };
    Ok(Some(item(
        queue,
        name,
        ConsumerMessageState::Waiting,
        &message,
        None,
    )))
}

fn pending_message(
    txn: &ReadTransaction,
    queue: &str,
    name: &str,
    id: Ulid,
) -> Result<Option<ConsumerMessage>> {
    let Some(record) = txn.open_table(PENDING)?.get((queue, name, id.0))? else {
        return Ok(None);
    };
    let mut pending: Pending = serde_json::from_slice(record.value())?;
    pending.id = id;
    let message = if pending.is_copy() {
        read_copy(txn, queue, name, id)?
    } else {
        read_message(txn, id)?
    };
    let Some(message) = message else {
        return Ok(None);
    };
    let state = match pending.slot {
        Slot::Leased => ConsumerMessageState::Processing,
        Slot::Ready | Slot::Scheduled => ConsumerMessageState::Retrying,
    };
    Ok(Some(item(queue, name, state, &message, Some(&pending))))
}

fn read_message(txn: &ReadTransaction, id: Ulid) -> Result<Option<Message>> {
    let Some(record) = txn.open_table(MESSAGES)?.get(id.0)? else {
        return Ok(None);
    };
    let mut message: Message = serde_json::from_slice(record.value())?;
    message.id = id;
    Ok(Some(message))
}

fn read_copy(txn: &ReadTransaction, queue: &str, name: &str, id: Ulid) -> Result<Option<Message>> {
    let Some(record) = txn.open_table(COPIES)?.get((queue, name, id.0))? else {
        return Ok(None);
    };
    let mut message: Message = serde_json::from_slice(record.value())?;
    message.id = id;
    Ok(Some(message))
}

fn item(
    queue: &str,
    name: &str,
    state: ConsumerMessageState,
    message: &Message,
    pending: Option<&Pending>,
) -> ConsumerMessage {
    let lease = pending.and_then(|pending| pending.lease.as_ref());
    let retry_at = pending
        .filter(|pending| pending.slot != Slot::Leased)
        .map(|pending| time::format(pending.ready_at));
    ConsumerMessage {
        id: message.id.to_string(),
        queue: queue.to_string(),
        consumer: name.to_string(),
        state,
        priority: message.priority,
        attempts: pending.map_or(0, |pending| pending.attempts),
        published_at: time::format(message.created_at),
        last_error: pending.and_then(|pending| pending.last_error.clone()),
        worker: lease.map(|lease| lease.worker.clone()),
        lease_expires_at: lease.map(|lease| time::format(lease.deadline)),
        retry_at,
        size: message.size,
    }
}
