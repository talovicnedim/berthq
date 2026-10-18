//! `Client::process`: a ready-made worker loop.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use tokio::time::Instant;

use crate::acks::PendingAcks;
use crate::{Client, ConsumeOptions, Delivery, Error, NackOptions, Result};

/// How long one consume request waits for messages before asking again.
const WAIT_MS: u64 = 20_000;
/// Pause before trying again while the broker is restarting or out of reach.
const RETRY_DELAY: Duration = Duration::from_secs(1);

#[derive(Debug, Clone)]
pub struct ProcessOptions {
    /// Whose position to use; `default` when `None`.
    pub consumer: Option<String>,
    /// Where a new consumer starts: `start`, `end`, an RFC 3339 time or a message id.
    pub from: Option<String>,
    /// This worker's name in the dashboard.
    pub worker: Option<String>,
    /// Messages fetched per request (1-100).
    pub batch: u32,
    /// Lease length per message; defaults to the queue's visibility timeout.
    pub visibility_timeout_ms: Option<u64>,
    /// No leases: the position moves on delivery, and nothing is acked or retried.
    pub auto_ack: bool,
    /// Stop after this many messages; never leases more than that.
    pub limit: Option<u64>,
}

impl Default for ProcessOptions {
    fn default() -> Self {
        Self {
            consumer: None,
            from: None,
            worker: None,
            batch: 10,
            visibility_timeout_ms: None,
            auto_ack: false,
            limit: None,
        }
    }
}

impl Client {
    /// Consumes `queue` until `stop` completes: runs `handler` for each message, acks it on
    /// `Ok`, and nacks it with the error on `Err` (so it is retried, then dead-lettered). The
    /// leases of a batch are extended while it is handled, however long that takes, and acks
    /// travel with the next consume call. While the broker restarts or is out of reach, the loop
    /// logs the failure (with `tracing`) and goes on: it waits and consumes again, and a message
    /// whose ack or nack is lost comes again once its lease runs out. When `stop` completes, the
    /// batch being handled is finished first. Other errors end the loop. Either way the waiting
    /// acks are sent before it returns.
    ///
    /// ```no_run
    /// # async fn example(client: berthq_client::Client) -> berthq_client::Result<()> {
    /// let stop = async { tokio::signal::ctrl_c().await.ok(); };
    /// client
    ///     .process("emails", Default::default(), stop, |delivery| async move {
    ///         println!("sending {}", delivery.body);
    ///         Ok::<(), String>(())
    ///     })
    ///     .await
    /// # }
    /// ```
    pub async fn process<F, Fut, E>(
        &self,
        queue: &str,
        options: ProcessOptions,
        stop: impl Future<Output = ()>,
        handler: F,
    ) -> Result<()>
    where
        F: Fn(Delivery) -> Fut,
        Fut: Future<Output = std::result::Result<(), E>>,
        E: std::fmt::Display,
    {
        let mut acks = PendingAcks::default();
        let result = self
            .handle_until_stopped(queue, &options, stop, &handler, &mut acks)
            .await;
        let flushed = acks.flush(self, queue).await;
        result.and(flushed)
    }

    async fn handle_until_stopped<F, Fut, E>(
        &self,
        queue: &str,
        options: &ProcessOptions,
        stop: impl Future<Output = ()>,
        handler: &F,
        acks: &mut PendingAcks,
    ) -> Result<()>
    where
        F: Fn(Delivery) -> Fut,
        Fut: Future<Output = std::result::Result<(), E>>,
        E: std::fmt::Display,
    {
        let mut handled = 0;
        tokio::pin!(stop);
        loop {
            let remaining = options.limit.map(|limit| limit.saturating_sub(handled));
            if remaining == Some(0) {
                return Ok(());
            }
            let consume = consume_options(options, remaining);
            let Some(batch) = self.next_batch(queue, &consume, acks, &mut stop).await? else {
                return Ok(());
            };
            handled += batch.len() as u64;
            self.handle_batch(queue, batch, handler, acks).await?;
        }
    }

    /// The next messages, sending the waiting acks along; `None` once `stop` completes.
    async fn next_batch(
        &self,
        queue: &str,
        consume: &ConsumeOptions,
        acks: &mut PendingAcks,
        stop: &mut Pin<&mut impl Future<Output = ()>>,
    ) -> Result<Option<Vec<Delivery>>> {
        loop {
            let options = ConsumeOptions {
                ack: acks.take(),
                ..consume.clone()
            };
            let result = tokio::select! {
                result = self.consume(queue, &options) => result,
                () = stop.as_mut() => {
                    acks.restore(options.ack);
                    return Ok(None);
                }
            };
            match result {
                Ok(consumed) => return Ok(Some(consumed.messages)),
                Err(err) if err.is_temporary() => {
                    tracing::warn!("could not get messages from {queue}, trying again: {err}");
                    acks.restore(options.ack);
                }
                Err(err) => {
                    acks.restore(options.ack);
                    return Err(err);
                }
            }
            tokio::select! {
                () = tokio::time::sleep(RETRY_DELAY) => {}
                () = stop.as_mut() => return Ok(None),
            }
        }
    }

    /// Handles a batch in order while one heartbeat extends all of its leases, so neither the
    /// messages waiting their turn nor the finished ones whose acks wait for the next consume
    /// can run out.
    async fn handle_batch<F, Fut, E>(
        &self,
        queue: &str,
        batch: Vec<Delivery>,
        handler: &F,
        acks: &mut PendingAcks,
    ) -> Result<()>
    where
        F: Fn(Delivery) -> Fut,
        Fut: Future<Output = std::result::Result<(), E>>,
        E: std::fmt::Display,
    {
        let leases: Vec<String> = batch.iter().filter_map(|d| d.lease.clone()).collect();
        let lease_ms = batch
            .first()
            .map_or(0, |delivery| delivery.visibility_timeout_ms);
        let work = async {
            for delivery in batch {
                self.handle(queue, delivery, handler, acks).await?;
            }
            Ok::<(), Error>(())
        };
        if leases.is_empty() {
            return work.await;
        }
        tokio::pin!(work);
        // Half the lease, so an extension always lands before it runs out.
        let interval = Duration::from_millis((lease_ms / 2).max(100));
        let mut heartbeat = tokio::time::interval_at(Instant::now() + interval, interval);
        loop {
            tokio::select! {
                result = &mut work => return result,
                _ = heartbeat.tick() => {
                    let extended = self.extend(queue, &leases, Some(lease_ms)).await;
                    unless_temporary(extended, &format!("could not extend leases on {queue}"))?;
                }
            }
        }
    }

    /// Runs the handler for one delivery: a nack right away, an ack with the next consume.
    async fn handle<F, Fut, E>(
        &self,
        queue: &str,
        delivery: Delivery,
        handler: &F,
        acks: &mut PendingAcks,
    ) -> Result<()>
    where
        F: Fn(Delivery) -> Fut,
        Fut: Future<Output = std::result::Result<(), E>>,
        E: std::fmt::Display,
    {
        let (id, lease) = (delivery.id.clone(), delivery.lease.clone());
        let outcome = handler(delivery).await;
        // Without a lease (auto-ack) there is nothing to settle.
        let Some(lease) = lease else {
            return Ok(());
        };
        match outcome {
            Ok(()) => {
                acks.push(lease);
                Ok(())
            }
            Err(err) => {
                let nack = NackOptions {
                    leases: vec![lease],
                    error: Some(err.to_string()),
                    ..NackOptions::default()
                };
                let failed = format!("could not nack message {id} on {queue}");
                unless_temporary(self.nack(queue, &nack).await, &failed)
            }
        }
    }
}

/// Logs a failure that passes (the broker restarting or out of reach) and turns it into `Ok`,
/// so the worker goes on. What it was about comes again once its lease runs out.
pub(crate) fn unless_temporary<T>(result: Result<T>, failed: &str) -> Result<()> {
    match result {
        Ok(_) => Ok(()),
        Err(err) if err.is_temporary() => {
            tracing::warn!("{failed}: {err}");
            Ok(())
        }
        Err(err) => Err(err),
    }
}

fn consume_options(options: &ProcessOptions, remaining: Option<u64>) -> ConsumeOptions {
    // Never lease more than will be handled, or the extra messages would wait for their lease
    // to run out.
    let max = remaining.map_or(options.batch, |n| {
        options.batch.min(u32::try_from(n).unwrap_or(u32::MAX))
    });
    ConsumeOptions {
        consumer: options.consumer.clone(),
        from: options.from.clone(),
        max: Some(max.clamp(1, 100)),
        wait_ms: Some(WAIT_MS),
        visibility_timeout_ms: options.visibility_timeout_ms,
        auto_ack: options.auto_ack,
        worker: options.worker.clone(),
        ack: Vec::new(),
    }
}
