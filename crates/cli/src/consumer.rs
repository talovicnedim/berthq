//! `berthq consumer …`: list consumers, create them, replay, pause and remove them.

use berthq_client::{Client, ConsumerInfo, Result};
use clap::Subcommand;
use serde_json::json;

use crate::output::{self, number, or_dash};

#[derive(Subcommand)]
pub enum ConsumerCommand {
    /// List a queue's consumers
    #[command(alias = "ls")]
    List { queue: String },
    /// Create a consumer
    Create {
        queue: String,
        consumer: String,
        /// Where it starts: start, end, an RFC 3339 time or a message id
        #[arg(long)]
        from: Option<String>,
    },
    /// Show one consumer
    #[command(alias = "info")]
    Get { queue: String, consumer: String },
    /// Remove a consumer with its position, leases and retries
    #[command(alias = "rm")]
    Remove { queue: String, consumer: String },
    /// Stop delivering to this consumer
    Pause { queue: String, consumer: String },
    /// Start delivering to this consumer again
    Resume { queue: String, consumer: String },
    /// Move the consumer's position; what it was processing is dropped
    Replay {
        queue: String,
        consumer: String,
        /// start, end, an RFC 3339 time or a message id
        #[arg(long)]
        from: String,
    },
}

pub async fn run(client: &Client, json: bool, command: ConsumerCommand) -> Result<()> {
    match command {
        ConsumerCommand::List { queue } => list(client, json, &queue).await?,
        ConsumerCommand::Create {
            queue,
            consumer,
            from,
        } => {
            let info = client
                .consumers_create(&queue, &consumer, from.as_deref())
                .await?;
            output::done(
                json,
                &info,
                &format!("Created consumer {consumer} of {queue}"),
            );
        }
        ConsumerCommand::Get { queue, consumer } => {
            let info = client.consumers_get(&queue, &consumer).await?;
            if json {
                output::json(&info);
            } else {
                table(std::slice::from_ref(&info));
            }
        }
        ConsumerCommand::Remove { queue, consumer } => {
            client.consumers_remove(&queue, &consumer).await?;
            let message = format!("Removed consumer {consumer} of {queue}");
            output::done(json, &json!({ "removed": consumer }), &message);
        }
        ConsumerCommand::Pause { queue, consumer } => {
            let info = client.consumers_pause(&queue, &consumer).await?;
            output::done(
                json,
                &info,
                &format!("Paused {consumer}: others still get messages"),
            );
        }
        ConsumerCommand::Resume { queue, consumer } => {
            let info = client.consumers_resume(&queue, &consumer).await?;
            output::done(json, &info, &format!("Resumed {consumer}"));
        }
        ConsumerCommand::Replay {
            queue,
            consumer,
            from,
        } => {
            let info = client.consumers_replay(&queue, &consumer, &from).await?;
            let waiting = number(info.stats.waiting);
            output::done(
                json,
                &info,
                &format!("{consumer} now starts at {from}: {waiting} waiting"),
            );
        }
    }
    Ok(())
}

async fn list(client: &Client, json: bool, queue: &str) -> Result<()> {
    let consumers = client.consumers_list(queue).await?;
    if json {
        output::json(&consumers);
    } else if consumers.is_empty() {
        output::line(&format!(
            "No consumers yet. Start one with `berthq consume {queue}`."
        ));
    } else {
        table(&consumers);
    }
    Ok(())
}

/// Consumers with what they hold back, as a table (also under `berthq queue get`).
pub fn table(consumers: &[ConsumerInfo]) {
    if consumers.is_empty() {
        return;
    }
    output::line("");
    let rows: Vec<Vec<String>> = consumers
        .iter()
        .map(|c| {
            let status = match (c.paused, c.idle, c.workers.is_empty()) {
                (true, _, _) => "paused".to_string(),
                (false, true, _) => "idle".to_string(),
                (false, false, true) => "no workers".to_string(),
                (false, false, false) => output::count(c.workers.len(), "worker"),
            };
            vec![
                c.name.clone(),
                number(c.stats.waiting),
                number(c.stats.processing),
                number(c.stats.retrying),
                status,
                or_dash(c.last_seen.as_deref()),
            ]
        })
        .collect();
    let headers = [
        "CONSUMER",
        "WAITING",
        "PROCESSING",
        "RETRYING",
        "STATUS",
        "LAST SEEN",
    ];
    output::table(&headers, &rows);
}
