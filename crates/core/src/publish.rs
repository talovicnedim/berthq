//! Publishing: validation, room checks (`maxMessages`, free disk) and deduplication, then a
//! durable write before the messages become visible to consumers.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use ulid::Ulid;

use crate::body::check_readable;
use crate::broker::Broker;
use crate::dedup::DEDUP_WINDOW_MS;
use crate::error::{Error, Result};
use crate::records::Message;
use crate::time;
use crate::tx::Tx;

pub const MAX_PRIORITY: u8 = 9;
/// Largest message body, counted as sent.
pub const MAX_BODY_BYTES: usize = 4 * 1024 * 1024;
/// Most messages accepted by one batch publish.
pub const MAX_BATCH: usize = 1000;
/// Longest a message can be hidden (`delayMs`, `deliverAt`) or a nack can delay its retry.
pub(crate) const MAX_DELAY_MS: u64 = 365 * 24 * 60 * 60 * 1000;
const MAX_DEDUP_ID_LEN: usize = 256;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NewMessage {
    /// Any JSON value, stored and delivered exactly as sent. Plain text is a JSON string.
    pub body: Box<RawValue>,
    /// 0-9, higher is delivered first. Defaults to 0.
    pub priority: Option<u8>,
    /// Hide the message for this long after publishing, up to 365 days.
    pub delay_ms: Option<u64>,
    /// Hide the message until this time (RFC 3339), up to 365 days ahead. Overrides `delayMs`.
    pub deliver_at: Option<String>,
    /// Publishing the same id again within an hour is a no-op.
    pub dedup_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Published {
    pub id: String,
    /// True when the `dedupId` was already seen: nothing was stored and `id` is the original.
    pub duplicate: bool,
}

/// A message that passed validation, ready to be stored.
#[derive(Clone)]
pub(crate) struct Checked {
    body: Vec<u8>,
    priority: u8,
    delay_ms: u64,
    deliver_at: Option<u64>,
    dedup_id: Option<String>,
}

impl Broker {
    pub async fn publish(&self, queue: &str, message: NewMessage) -> Result<Published> {
        let mut published = self.publish_batch(queue, vec![message]).await?;
        published
            .pop()
            .ok_or_else(|| Error::Invalid("nothing to publish".to_string()))
    }

    /// Publishes all messages in one transaction: either all are stored or none.
    pub async fn publish_batch(
        &self,
        queue: &str,
        messages: Vec<NewMessage>,
    ) -> Result<Vec<Published>> {
        if messages.len() > MAX_BATCH {
            return Err(Error::Invalid(format!(
                "a batch holds at most {MAX_BATCH} messages"
            )));
        }
        let now = time::now_ms();
        let checked = messages
            .into_iter()
            .map(|message| check(message, now))
            .collect::<Result<Vec<_>>>()?;
        self.disk().check()?;
        let queue = queue.to_string();
        self.store()
            .run(move |tx| tx.publish(&queue, checked))
            .await
    }
}

/// What publishing to one queue will do, decided before anything is written: an id per message,
/// or the original's id for a duplicate.
pub(crate) struct Plan {
    queue: String,
    decided: Vec<(Ulid, bool)>,
}

impl Tx<'_, '_> {
    pub(crate) fn publish(
        &mut self,
        queue_name: &str,
        messages: Vec<Checked>,
    ) -> Result<Vec<Published>> {
        let plan = self.plan(queue_name, &messages)?;
        self.apply(plan, messages)
    }

    /// Checks a publish without writing: the queue exists, isn't full, and which messages are
    /// duplicates. Fails with nothing changed.
    pub(crate) fn plan(&mut self, queue_name: &str, messages: &[Checked]) -> Result<Plan> {
        if self.mem.closing {
            return Err(Error::Unavailable);
        }
        self.mem.queue(queue_name)?;
        let mut decided: Vec<(Ulid, bool)> = Vec::with_capacity(messages.len());
        let mut in_batch: HashMap<&str, Ulid> = HashMap::new();
        for message in messages {
            let dedup_id = message.dedup_id.as_deref();
            let original = match dedup_id {
                Some(dedup_id) => match in_batch.get(dedup_id) {
                    Some(id) => Some(*id),
                    None => self.seen(queue_name, dedup_id)?,
                },
                None => None,
            };
            if let Some(original) = original {
                decided.push((original, true));
                continue;
            }
            let id = self.mem.ids.next(self.now);
            if let Some(dedup_id) = dedup_id {
                in_batch.insert(dedup_id, id);
            }
            decided.push((id, false));
        }
        let fresh = decided.iter().filter(|(_, duplicate)| !duplicate).count() as u64;
        self.check_room(queue_name, fresh)?;
        Ok(Plan {
            queue: queue_name.to_string(),
            decided,
        })
    }

    /// Refuses a publish that would take the backlog past `maxMessages`.
    fn check_room(&self, queue_name: &str, fresh: u64) -> Result<()> {
        let queue = self.mem.queue(queue_name)?;
        let Some(max) = queue.settings.max_messages.filter(|_| fresh > 0) else {
            return Ok(());
        };
        let backlog = self.backlog_now(queue_name)?;
        if backlog + queue.counts.delayed + fresh > u64::from(max) {
            return Err(Error::QueueFull(format!(
                "queue {queue_name} is full ({max} messages to process)"
            )));
        }
        Ok(())
    }

    /// Writes a planned publish.
    pub(crate) fn apply(&mut self, plan: Plan, messages: Vec<Checked>) -> Result<Vec<Published>> {
        let queue_name = plan.queue.as_str();
        let mut published = Vec::with_capacity(messages.len());
        let mut fresh = 0;
        for (checked, (id, duplicate)) in messages.into_iter().zip(plan.decided) {
            published.push(Published {
                id: id.to_string(),
                duplicate,
            });
            if duplicate {
                continue;
            }
            fresh += 1;
            let deliver_at = checked.deliver_at.unwrap_or(self.now + checked.delay_ms);
            let message = Message {
                id,
                queue: queue_name.to_string(),
                priority: checked.priority,
                created_at: self.now,
                visible_at: None,
                seq: None,
                deliver_at: (deliver_at > self.now).then_some(deliver_at),
                dedup_id: checked.dedup_id,
                size: u32::try_from(checked.body.len()).unwrap_or(u32::MAX),
                dead_letter: None,
            };
            let dedup_id = message.dedup_id.clone();
            self.store_message(message, &checked.body)?;
            if let Some(dedup_id) = dedup_id {
                self.remember(queue_name, &dedup_id, id, self.now + DEDUP_WINDOW_MS)?;
            }
        }
        self.mem.queue_mut(queue_name)?.counters.published += fresh;
        Ok(published)
    }
}

/// Checks a message. `deliverAt` may be at most a year after `now`.
pub(crate) fn check(message: NewMessage, now: u64) -> Result<Checked> {
    let priority = message.priority.unwrap_or(0);
    if priority > MAX_PRIORITY {
        return Err(Error::Invalid(format!("priority must be 0-{MAX_PRIORITY}")));
    }
    if let Some(dedup_id) = &message.dedup_id
        && (dedup_id.is_empty() || dedup_id.len() > MAX_DEDUP_ID_LEN)
    {
        return Err(Error::Invalid(format!(
            "dedupId must be 1-{MAX_DEDUP_ID_LEN} characters"
        )));
    }
    let delay_ms = message.delay_ms.unwrap_or(0);
    check_delay(delay_ms)?;
    let deliver_at = match &message.deliver_at {
        Some(at) => Some(check_deliver_at(at, now)?),
        None => None,
    };
    let size = message.body.get().len();
    if size > MAX_BODY_BYTES {
        return Err(Error::Invalid(format!(
            "body is {size} bytes; the limit is {MAX_BODY_BYTES}"
        )));
    }
    check_readable(&message.body)?;
    Ok(Checked {
        body: String::from(Box::<str>::from(message.body)).into_bytes(),
        priority,
        delay_ms,
        deliver_at,
        dedup_id: message.dedup_id,
    })
}

/// The delay rule for publish and nack.
pub(crate) fn check_delay(ms: u64) -> Result<()> {
    if ms > MAX_DELAY_MS {
        return Err(Error::Invalid(
            "delayMs must be at most 365 days".to_string(),
        ));
    }
    Ok(())
}

fn check_deliver_at(text: &str, now: u64) -> Result<u64> {
    let at = time::parse(text)
        .ok_or_else(|| Error::Invalid(format!("invalid deliverAt {text}: use RFC 3339")))?;
    if at.saturating_sub(now) > MAX_DELAY_MS {
        return Err(Error::Invalid(
            "deliverAt must be at most 365 days ahead".to_string(),
        ));
    }
    Ok(at)
}

#[cfg(test)]
#[path = "publish_tests.rs"]
mod tests;
