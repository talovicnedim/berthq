//! `berthq queue …`: create, inspect and manage queues.

use berthq_client::{Client, QueueDetails, QueueInfo, Result};
use clap::Subcommand;
use serde_json::json;

use crate::duration;
use crate::output::{self, number, or_dash};
use crate::settings::{SettingsArgs, patch};

#[derive(Subcommand)]
pub enum QueueCommand {
    /// Create a queue
    Create {
        name: String,
        #[command(flatten)]
        settings: SettingsArgs,
    },
    /// Change a queue's settings
    Update {
        name: String,
        #[command(flatten)]
        settings: SettingsArgs,
    },
    /// List queues
    #[command(alias = "ls")]
    List,
    /// Show a queue's settings, stats and consumers
    #[command(alias = "info")]
    Get { name: String },
    /// Delete a queue, its consumers and all its messages
    #[command(alias = "rm")]
    Delete { name: String },
    /// Delete all messages
    Purge { name: String },
    /// Stop delivering messages to every consumer (publishing still works)
    Pause { name: String },
    /// Start delivering messages again
    Resume { name: String },
    /// Give dead letters back to the consumer that failed them: all of them, or the given ids
    Requeue {
        /// The queue (`emails`) or its dead-letter queue (`emails.dlq`)
        name: String,
        ids: Vec<String>,
    },
}

pub async fn run(client: &Client, json: bool, command: QueueCommand) -> Result<()> {
    match command {
        QueueCommand::Create { name, settings } => {
            let info = client.queues_create(&name, &patch(&settings)).await?;
            output::done(json, &info, &format!("Created queue {name}"));
        }
        QueueCommand::Update { name, settings } => {
            let info = client.queues_update(&name, &patch(&settings)).await?;
            output::done(json, &info, &format!("Updated queue {name}"));
        }
        QueueCommand::List => {
            let queues = client.queues_list().await?;
            if json {
                output::json(&queues);
            } else {
                list(&queues);
            }
        }
        QueueCommand::Get { name } => {
            let details = client.queues_get(&name).await?;
            if json {
                output::json(&details);
            } else {
                show(&details);
            }
        }
        QueueCommand::Delete { name } => {
            client.queues_remove(&name).await?;
            let message = format!("Deleted queue {name}");
            output::done(json, &json!({ "deleted": name }), &message);
        }
        QueueCommand::Purge { name } => {
            let purged = client.queues_purge(&name).await?;
            let message = format!("Purged {} messages from {name}", number(purged));
            output::done(json, &json!({ "purged": purged }), &message);
        }
        QueueCommand::Pause { name } => {
            let info = client.queues_pause(&name).await?;
            let message = format!("Paused {name}: messages are kept but not delivered");
            output::done(json, &info, &message);
        }
        QueueCommand::Resume { name } => {
            let info = client.queues_resume(&name).await?;
            output::done(json, &info, &format!("Resumed {name}"));
        }
        QueueCommand::Requeue { name, ids } => requeue(client, json, name, ids).await?,
    }
    Ok(())
}

async fn requeue(client: &Client, json: bool, name: String, ids: Vec<String>) -> Result<()> {
    let dlq = if name.ends_with(".dlq") {
        name
    } else {
        format!("{name}.dlq")
    };
    let ids = (!ids.is_empty()).then_some(ids);
    let requeued = client.requeue(&dlq, ids.as_deref()).await?;
    if json {
        output::json(&requeued);
        return Ok(());
    }
    let count = number(requeued.requeued);
    output::line(&format!("Requeued {count} messages from {dlq}"));
    if requeued.skipped > 0 {
        output::line(&format!(
            "Skipped {} (not dead letters, or their queue or consumer is gone)",
            number(requeued.skipped)
        ));
    }
    Ok(())
}

fn list(queues: &[QueueInfo]) {
    if queues.is_empty() {
        output::line("No queues yet. Create one with `berthq queue create <name>`.");
        return;
    }
    let rows: Vec<Vec<String>> = queues
        .iter()
        .map(|q| {
            let mut name = q.name.clone();
            if q.settings.paused {
                name.push_str(" (paused)");
            }
            vec![
                name,
                number(q.stats.backlog),
                number(q.stats.processing),
                number(q.stats.messages),
                number(q.stats.consumers),
                number(q.stats.publish_rate),
                number(q.stats.ack_rate),
            ]
        })
        .collect();
    let headers = [
        "QUEUE",
        "BACKLOG",
        "PROCESSING",
        "STORED",
        "CONSUMERS",
        "IN/S",
        "ACK/S",
    ];
    output::table(&headers, &rows);
}

fn show(details: &QueueDetails) {
    let info = &details.info;
    let (stats, settings) = (&info.stats, &info.settings);
    let paused = if settings.paused { " (paused)" } else { "" };
    let mut lines = vec![
        format!("Queue        {}{paused}", info.name),
        format!("Backlog      {}", number(stats.backlog)),
        format!("Processing   {}", number(stats.processing)),
        format!("Delayed      {}", number(stats.delayed)),
        format!(
            "Stored       {} (kept for replay: {}, and until every consumer acked)",
            number(stats.messages),
            duration::format(settings.retention_ms)
        ),
        format!(
            "Throughput   {}/s in, {}/s acked",
            number(stats.publish_rate),
            number(stats.ack_rate)
        ),
        format!(
            "Totals       {} published, {} acked, {} retried, {} dead-lettered, {} dropped",
            number(stats.counters.published),
            number(stats.counters.acked),
            number(stats.counters.retried),
            number(stats.counters.dead_lettered),
            number(stats.counters.dropped)
        ),
    ];
    if let Some(dlq) = &info.dead_letter_queue {
        lines.push(format!("Dead letters {dlq}"));
    }
    lines.push(String::new());
    lines.push(settings_line(info));
    for line in lines {
        output::line(&line);
    }
    crate::consumer::table(&details.consumers);
}

fn settings_line(info: &QueueInfo) -> String {
    let settings = &info.settings;
    let attempts = if settings.max_attempts == 0 {
        "unlimited".to_string()
    } else {
        settings.max_attempts.to_string()
    };
    format!(
        "Settings     visibility timeout {}, {attempts} attempts, retry after {}, max {} messages",
        duration::format(settings.visibility_timeout_ms),
        duration::format(settings.retry_delay_ms),
        or_dash(settings.max_messages.map(number).as_deref())
    )
}
