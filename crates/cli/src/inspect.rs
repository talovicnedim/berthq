//! `berthq messages` and `berthq stats`: read-only views.

use berthq_client::{Client, ConsumerMessageState, MessageState, Result};
use clap::{Args, ValueEnum};

use crate::output::{self, number, or_dash};

#[derive(Args)]
pub struct MessagesArgs {
    pub queue: String,
    /// Show what this consumer is doing instead of what the queue stores
    #[arg(long)]
    pub consumer: Option<String>,
    /// visible or delayed for the queue; waiting, processing or retrying with --consumer
    #[arg(long, value_enum)]
    pub state: Option<State>,
    #[arg(long, default_value_t = 20)]
    pub limit: u32,
    /// Page to messages older than this id
    #[arg(long)]
    pub before: Option<String>,
    /// Show the full message with its body
    #[arg(long)]
    pub id: Option<String>,
}

#[derive(Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum State {
    Visible,
    Delayed,
    Waiting,
    Processing,
    Retrying,
}

pub async fn messages(client: &Client, json: bool, args: MessagesArgs) -> Result<()> {
    if let Some(id) = &args.id {
        output::json(&client.messages_get(&args.queue, id).await?);
        return Ok(());
    }
    match &args.consumer {
        Some(consumer) => consumer_messages(client, json, &args, consumer).await,
        None => queue_messages(client, json, &args).await,
    }
}

async fn queue_messages(client: &Client, json: bool, args: &MessagesArgs) -> Result<()> {
    let state = match args.state {
        None | Some(State::Visible) => MessageState::Visible,
        Some(State::Delayed) => MessageState::Delayed,
        Some(_) => return Err(needs_consumer()),
    };
    let messages = client
        .messages_list(
            &args.queue,
            Some(state),
            Some(args.limit),
            args.before.as_deref(),
        )
        .await?;
    if json {
        output::json(&messages);
        return Ok(());
    }
    if messages.is_empty() {
        output::line(&format!("No messages in {}", args.queue));
        return Ok(());
    }
    let rows: Vec<Vec<String>> = messages
        .iter()
        .map(|m| {
            let error = m
                .last_error
                .as_deref()
                .map(|e| e.lines().next().unwrap_or(e));
            vec![
                m.id.clone(),
                m.priority.to_string(),
                m.published_at.clone(),
                or_dash(m.deliver_at.as_deref()),
                or_dash(error),
            ]
        })
        .collect();
    output::table(
        &["ID", "PRIORITY", "PUBLISHED", "DELIVER AT", "LAST ERROR"],
        &rows,
    );
    output::note("Show a message with its body: berthq messages <queue> --id <id>");
    Ok(())
}

async fn consumer_messages(
    client: &Client,
    json: bool,
    args: &MessagesArgs,
    consumer: &str,
) -> Result<()> {
    let state = match args.state {
        Some(State::Waiting) => ConsumerMessageState::Waiting,
        Some(State::Retrying) => ConsumerMessageState::Retrying,
        Some(State::Processing) | None => ConsumerMessageState::Processing,
        Some(State::Visible | State::Delayed) => {
            return Err(berthq_client::Error::Http(
                "--state visible and delayed are for a queue, not a consumer".to_string(),
            ));
        }
    };
    let messages = client
        .consumers_messages(&args.queue, consumer, Some(state), Some(args.limit))
        .await?;
    if json {
        output::json(&messages);
        return Ok(());
    }
    let rows: Vec<Vec<String>> = messages
        .iter()
        .map(|m| {
            let error = m
                .last_error
                .as_deref()
                .map(|e| e.lines().next().unwrap_or(e));
            let when = m.lease_expires_at.as_deref().or(m.retry_at.as_deref());
            vec![
                m.id.clone(),
                m.attempts.to_string(),
                or_dash(m.worker.as_deref()),
                or_dash(when),
                or_dash(error),
            ]
        })
        .collect();
    output::table(&["ID", "ATTEMPTS", "WORKER", "UNTIL", "LAST ERROR"], &rows);
    Ok(())
}

fn needs_consumer() -> berthq_client::Error {
    berthq_client::Error::Http(
        "--state waiting, processing and retrying need --consumer <name>".to_string(),
    )
}

pub async fn stats(client: &Client, json: bool) -> Result<()> {
    let overview = client.stats().await?;
    if json {
        output::json(&overview);
        return Ok(());
    }
    let lines = [
        format!(
            "Messages/sec  {} in, {} out, {} acked",
            number(overview.publish_rate),
            number(overview.deliver_rate),
            number(overview.ack_rate)
        ),
        format!("Backlog       {}", number(overview.backlog)),
        format!("Processing    {}", number(overview.processing)),
        format!("Failed        {}", number(overview.failed)),
        format!("Stored        {}", number(overview.messages)),
        format!(
            "Consumers     {} ({})",
            number(overview.consumers),
            output::count(overview.workers as usize, "worker")
        ),
        String::new(),
    ];
    for line in lines {
        output::line(&line);
    }
    let queues = client.queues_list().await?;
    let rows: Vec<Vec<String>> = queues
        .iter()
        .map(|q| {
            vec![
                q.name.clone(),
                number(q.stats.backlog),
                number(q.stats.messages),
                number(q.stats.publish_rate),
            ]
        })
        .collect();
    if rows.is_empty() {
        output::line("No queues yet. Create one with `berthq queue create <name>`.");
    } else {
        output::table(&["QUEUE", "BACKLOG", "STORED", "IN/S"], &rows);
    }
    Ok(())
}
