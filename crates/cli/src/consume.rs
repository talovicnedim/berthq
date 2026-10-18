//! `berthq consume`: long-polls a queue for a consumer and prints each message, or runs a command
//! per message and acks or nacks based on its exit code. The SDK's worker loop does the rest:
//! leases, acks with the next consume, retries while the broker restarts.

use std::cell::Cell;
use std::time::Duration;

use berthq_client::{Client, Delivery, ProcessOptions, Result};
use clap::Args;
use serde_json::Value;
use tokio::sync::Notify;

use crate::duration::{self, millis};
use crate::exec;
use crate::output;

#[derive(Args)]
pub struct ConsumeArgs {
    pub queue: String,
    /// Whose position to use (default: default); a new name starts at --from
    #[arg(long)]
    pub consumer: Option<String>,
    /// Where a new consumer starts: start (default), end, an RFC 3339 time or a message id
    #[arg(long)]
    pub from: Option<String>,
    /// This worker's name in the dashboard
    #[arg(long)]
    pub worker: Option<String>,
    /// Stop after this many messages
    #[arg(long, short = 'n')]
    pub count: Option<u64>,
    /// Run this shell command per message with the body on stdin; ack on exit 0, nack otherwise.
    /// Ctrl-C lets the messages already received finish first; a second Ctrl-C stops at once
    #[arg(long, conflicts_with = "auto_ack")]
    pub exec: Option<String>,
    /// Don't ack: the position moves as messages arrive and nothing is retried
    #[arg(long)]
    pub auto_ack: bool,
    /// Lease length, e.g. 5m for slow jobs (defaults to the queue's setting)
    #[arg(long, value_parser = duration::parse)]
    pub visibility_timeout: Option<Duration>,
    /// Messages fetched per request
    #[arg(long, default_value_t = 10)]
    pub batch: u32,
}

pub async fn run(client: &Client, json: bool, args: ConsumeArgs) -> Result<()> {
    let worker = args
        .worker
        .unwrap_or_else(|| format!("cli-{}", std::process::id()));
    if !json {
        let consumer = args.consumer.as_deref().unwrap_or("default");
        output::note(&format!(
            "Waiting for messages on {} as {consumer} (Ctrl-C to stop)",
            args.queue
        ));
    }
    let options = ProcessOptions {
        consumer: args.consumer,
        from: args.from,
        worker: Some(worker),
        batch: args.batch,
        visibility_timeout_ms: args.visibility_timeout.map(millis),
        auto_ack: args.auto_ack,
        limit: args.count,
    };
    // Fired by the first Ctrl-C, or when stdout closes (`| head`) so the loop stops instead of
    // acking messages nobody reads.
    let stopping = Notify::new();
    // The process id of the `--exec` command running now.
    let running = Cell::new(None);
    let (queue, exec) = (args.queue.as_str(), args.exec.as_deref());
    let (stopping, running) = (&stopping, &running);
    let work = client.process(queue, options, stopping.notified(), |delivery| async move {
        match exec {
            Some(command) => exec::run_command(queue, command, &delivery, running).await,
            None => print(json, &delivery).map_err(|err| {
                stopping.notify_one();
                format!("failed to print the message: {err}")
            }),
        }
    });
    tokio::select! {
        result = work => result,
        () = second_ctrl_c(stopping, running) => {
            output::note("Stopped now; messages not acked yet come again once their lease runs out");
            std::process::exit(130)
        }
    }
}

/// Returns on the second Ctrl-C, once it is passed on to the running command. The first one stops
/// the loop once the messages already received are done: each of them has used up an attempt.
async fn second_ctrl_c(stopping: &Notify, running: &Cell<Option<u32>>) {
    ctrl_c().await;
    if running.get().is_some() {
        output::note("Finishing the messages already received; press Ctrl-C again to quit now");
    }
    stopping.notify_one();
    ctrl_c().await;
    // Taken here, before the loop is dropped, so the command gets this Ctrl-C and isn't killed.
    if let Some(command) = running.take() {
        exec::interrupt(command);
    }
}

async fn ctrl_c() {
    if tokio::signal::ctrl_c().await.is_err() {
        // Without a handler, Ctrl-C ends berthq the usual way.
        std::future::pending::<()>().await;
    }
}

fn print(json: bool, delivery: &Delivery) -> std::io::Result<()> {
    if json {
        return output::write_line(&serde_json::to_string(delivery)?);
    }
    let retry = if delivery.attempt > 1 {
        format!(" (attempt {})", delivery.attempt)
    } else {
        String::new()
    };
    output::note(&format!(
        "{} {} {}{retry}",
        delivery.queue, delivery.consumer, delivery.id
    ));
    output::write_line(&body_text(&delivery.body))
}

/// Strings print as plain text; everything else as compact JSON.
pub fn body_text(body: &Value) -> String {
    match body {
        Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}
