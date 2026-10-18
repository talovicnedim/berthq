//! Delivering messages to a consumer's workers: long-polling, ack, nack and extend. Leases name
//! their consumer, so settling one needs nothing else.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use crate::broker::Broker;
use crate::error::{Error, Result};
use crate::failure::Failure;
use crate::polling::Waiting;
use crate::positions::Start;
use crate::publish::check_delay;
use crate::settings::{check_visibility_timeout, validate_name};
use crate::take::Take;

/// Most messages handed out by one consume call.
pub const MAX_CONSUME: u32 = 100;
/// Longest a consume call may wait for messages.
pub const MAX_WAIT_MS: u64 = 60_000;
/// The consumer used when a call doesn't name one.
pub const DEFAULT_CONSUMER: &str = "default";
/// Most leases one call may ack, nack or extend, also on the way in a consume call. More would
/// hold up every other write while the writer works through them.
pub const MAX_ACK: usize = 1000;
const MAX_WORKER_NAME_LEN: usize = 128;

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConsumeOptions {
    /// Whose position to use. Defaults to "default"; created if it doesn't exist.
    pub consumer: Option<String>,
    /// Where a new consumer starts: `start`, `end`, an RFC 3339 time or a message id.
    pub from: Option<String>,
    /// Most messages to return, 1-100. Defaults to 1.
    pub max: Option<u32>,
    /// Wait up to this long for a message when none is ready (long-polling). Defaults to 0.
    pub wait_ms: Option<u64>,
    /// Lease length for these messages. Defaults to the queue's `visibilityTimeoutMs`.
    pub visibility_timeout_ms: Option<u64>,
    /// No lease: the position moves now and nothing is retried.
    #[serde(default)]
    pub auto_ack: bool,
    /// Shown in the dashboard's worker list. Defaults to "anonymous".
    pub worker: Option<String>,
    /// Leases to ack before new messages are handed out (any consumer's, up to 1000).
    #[serde(default)]
    pub ack: Vec<String>,
}

/// What a consume call returns: new messages, and what happened to its `ack` leases.
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Consumed {
    pub messages: Vec<Delivery>,
    #[serde(flatten)]
    pub acked: Settled,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Delivery {
    pub id: String,
    /// Pass this to `ack`, `nack` or `extend`; `None` with `autoAck`.
    pub lease: Option<String>,
    pub queue: String,
    pub consumer: String,
    pub body: Box<RawValue>,
    pub priority: u8,
    /// 1 on the first delivery to this consumer.
    pub attempt: u32,
    pub published_at: String,
    pub lease_expires_at: Option<String>,
    /// Length of this lease (0 with `autoAck`), so clients can time `extend` calls without
    /// comparing clocks.
    pub visibility_timeout_ms: u64,
    pub dedup_id: Option<String>,
    /// The error from the previous failed attempt, if any.
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NackOptions {
    pub leases: Vec<String>,
    /// Retry after this long (up to 365 days) instead of the queue's backoff.
    pub delay_ms: Option<u64>,
    /// Skip remaining attempts and dead-letter right away.
    #[serde(default)]
    pub dead_letter: bool,
    /// Why processing failed; kept with the message and shown in the dashboard.
    pub error: Option<String>,
}

/// Result of `ack`, `nack` and `extend`.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Settled {
    pub settled: u32,
    /// Leases that no longer exist: already settled, expired and redelivered, or dropped by a
    /// replay.
    pub unknown: Vec<String>,
}

impl Broker {
    /// Acks `ack` first, then gives up to `max` messages to a consumer, waiting up to `waitMs`
    /// for one to arrive. The acks are on disk before the call waits. Fails with
    /// `Unavailable` once shutdown has started (acks still count), so polling clients back off.
    pub async fn consume(&self, queue: &str, options: ConsumeOptions) -> Result<Consumed> {
        let (take, mut acks) = Take::check(options)?;
        let deadline = tokio::time::Instant::now() + Duration::from_millis(take.wait_ms);
        let _waiting = Waiting::start(self, queue, &take.consumer, &take.worker)?;
        let mut consumed = Consumed::default();
        loop {
            let (notify, ready) = match self.poll_state(queue, &take.consumer) {
                Ok(state) => state,
                Err(err) => {
                    self.ack_now(queue, &mut acks, &mut consumed.acked).await?;
                    return Err(err);
                }
            };
            let notified = notify.notified();
            tokio::pin!(notified);
            // Registers interest before looking, so a publish in between still wakes us.
            notified.as_mut().enable();

            if ready {
                consumed.messages = self
                    .take_and_ack(queue, &take, &mut acks, &mut consumed.acked)
                    .await?;
                if !consumed.messages.is_empty() {
                    return Ok(consumed);
                }
            }
            self.ack_now(queue, &mut acks, &mut consumed.acked).await?;
            if tokio::time::Instant::now() >= deadline {
                return Ok(consumed);
            }
            tokio::select! {
                () = &mut notified => {}
                () = tokio::time::sleep_until(deadline) => {}
            }
        }
    }

    /// One transaction for both, so a worker's ack and its next batch share one disk flush.
    /// The take runs first: it does all its checks before writing, so a failed take acks
    /// nothing.
    async fn take_and_ack(
        &self,
        queue: &str,
        take: &Take,
        acks: &mut Vec<String>,
        acked: &mut Settled,
    ) -> Result<Vec<Delivery>> {
        let (name, request, leases) = (queue.to_string(), take.clone(), std::mem::take(acks));
        let (taken, settled) = self
            .store()
            .run(move |tx| {
                let taken = tx.take(&name, &request)?;
                let settled = tx.ack(&name, &leases)?;
                Ok((taken, settled))
            })
            .await?;
        acked.absorb(settled);
        Ok(taken)
    }

    /// Acks what is left on its own, before the call waits or gives up.
    async fn ack_now(
        &self,
        queue: &str,
        acks: &mut Vec<String>,
        acked: &mut Settled,
    ) -> Result<()> {
        if acks.is_empty() {
            return Ok(());
        }
        acked.absorb(self.ack(queue, std::mem::take(acks)).await?);
        Ok(())
    }

    /// Marks messages as done for the lease's consumer.
    pub async fn ack(&self, queue: &str, leases: Vec<String>) -> Result<Settled> {
        check_leases("leases", &leases)?;
        let queue = queue.to_string();
        self.store().run(move |tx| tx.ack(&queue, &leases)).await
    }

    /// Marks messages as failed: they are retried after a backoff or dead-lettered.
    pub async fn nack(&self, queue: &str, options: NackOptions) -> Result<Settled> {
        check_leases("leases", &options.leases)?;
        options.delay_ms.map_or(Ok(()), check_delay)?;
        let failure = Failure {
            delay_ms: options.delay_ms,
            dead_letter: options.dead_letter,
            error: options.error,
        };
        let queue = queue.to_string();
        self.store()
            .run(move |tx| tx.nack(&queue, &options.leases, &failure))
            .await
    }

    /// Restarts the lease clock for long-running work.
    pub async fn extend(
        &self,
        queue: &str,
        leases: Vec<String>,
        visibility_timeout_ms: Option<u64>,
    ) -> Result<Settled> {
        check_leases("leases", &leases)?;
        visibility_timeout_ms.map_or(Ok(()), check_visibility_timeout)?;
        let queue = queue.to_string();
        self.store()
            .run(move |tx| tx.extend(&queue, &leases, visibility_timeout_ms))
            .await
    }
}

impl Settled {
    fn absorb(&mut self, other: Settled) {
        self.settled += other.settled;
        self.unknown.extend(other.unknown);
    }
}

impl Take {
    /// Validates consume options before anything runs; returns the leases to ack separately.
    fn check(options: ConsumeOptions) -> Result<(Take, Vec<String>)> {
        check_leases("ack", &options.ack)?;
        let consumer = options
            .consumer
            .unwrap_or_else(|| DEFAULT_CONSUMER.to_string());
        validate_name("consumer", &consumer)?;
        let worker = options.worker.unwrap_or_else(|| "anonymous".to_string());
        if worker.is_empty() || worker.len() > MAX_WORKER_NAME_LEN {
            return Err(Error::Invalid(
                "worker must be 1-128 characters".to_string(),
            ));
        }
        options
            .visibility_timeout_ms
            .map_or(Ok(()), check_visibility_timeout)?;
        let take = Take {
            consumer,
            from: Start::parse(options.from.as_deref())?,
            max: options.max.unwrap_or(1).clamp(1, MAX_CONSUME) as usize,
            wait_ms: options.wait_ms.unwrap_or(0).min(MAX_WAIT_MS),
            visibility_ms: options.visibility_timeout_ms,
            auto_ack: options.auto_ack,
            worker,
        };
        Ok((take, options.ack))
    }
}

fn check_leases(field: &str, leases: &[String]) -> Result<()> {
    if leases.len() > MAX_ACK {
        return Err(Error::Invalid(format!(
            "{field} holds at most {MAX_ACK} leases"
        )));
    }
    Ok(())
}

#[cfg(test)]
#[path = "delivery_tests.rs"]
mod tests;
