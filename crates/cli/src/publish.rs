//! `berthq publish`: one message from the command line, or one per line from stdin.

use std::io::BufRead;
use std::time::Duration;

use berthq_client::{
    Client, Error, NewMessage, Pending, Published, Publisher, PublisherOptions, Result,
};
use clap::Args;
use serde_json::Value;
use tokio::sync::{Notify, mpsc};

use crate::duration::{self, millis};
use crate::output;

/// Stdin messages whose results aren't printed yet; reading pauses beyond this.
const IN_FLIGHT: usize = 10_000;

#[derive(Args)]
pub struct PublishArgs {
    pub queue: String,
    #[command(flatten)]
    pub message: MessageArgs,
}

#[derive(Args)]
pub struct MessageArgs {
    /// JSON, or plain text sent as a JSON string; `-` or missing reads stdin, one message per line
    pub body: Option<String>,
    /// 0-9, higher is delivered first
    #[arg(long)]
    pub priority: Option<u8>,
    /// Deliver after this long, e.g. 10s
    #[arg(long, value_parser = duration::parse)]
    pub delay: Option<Duration>,
    /// Deliver at this time (RFC 3339), e.g. 2026-12-24T18:00:00Z
    #[arg(long)]
    pub at: Option<String>,
    /// Publishing the same id again within an hour does nothing (one message only, not stdin)
    #[arg(long)]
    pub dedup_id: Option<String>,
}

/// What happened to the lines read from stdin.
#[derive(Default)]
struct Outcome {
    lines: u64,
    published: u64,
    failed: u64,
    /// The first line that failed for a reason other than its content; reading stopped there.
    stopped: Option<(u64, Error)>,
    /// Lines after `stopped` that were on their way and still got published.
    published_after_stop: u64,
}

pub async fn run(client: &Client, json: bool, args: PublishArgs) -> Result<()> {
    if let Some(message) = single(&args.message) {
        let published = client.publish(&args.queue, &message).await?;
        if json {
            output::json(&published);
        } else if published.duplicate {
            output::line(&format!(
                "Duplicate of {} (dedup id already seen)",
                published.id
            ));
        } else {
            output::line(&published.id);
        }
        return Ok(());
    }
    if args.message.dedup_id.is_some() {
        return Err(Error::Http(
            "--dedup-id needs the body on the command line: every line from stdin would get the \
             same id"
                .to_string(),
        ));
    }
    publish_lines(client, json, &args)
        .await
        .finish(json, &args.queue)
}

/// Publishes one message per non-empty stdin line through a publisher, so lines go out in
/// batches as they arrive, and reports the results in input order. A line refused as `invalid`
/// is reported and skipped; any other error stops reading, and what is on its way is finished.
async fn publish_lines(client: &Client, json: bool, args: &PublishArgs) -> Outcome {
    let publisher = client.publisher(&args.queue, PublisherOptions::default());
    let (results, waiting) = mpsc::channel(IN_FLIGHT);
    let stop = Notify::new();
    let send = send_lines(&publisher, &args.message, results, &stop);
    let ((), outcome) = tokio::join!(send, report_results(json, waiting, &stop));
    outcome
}

async fn send_lines(
    publisher: &Publisher,
    args: &MessageArgs,
    results: mpsc::Sender<(u64, Result<Pending>)>,
    stop: &Notify,
) {
    let mut lines = stdin_lines();
    let mut number = 0;
    loop {
        let line = tokio::select! {
            biased;
            () = stop.notified() => return,
            line = lines.recv() => line,
        };
        let Some(line) = line else {
            return;
        };
        number += 1;
        let sent = match line {
            Ok(line) if line.trim().is_empty() => continue,
            Ok(line) => publisher.send(message(args, &line)).await,
            Err(err) => Err(Error::Http(format!("failed to read stdin: {err}"))),
        };
        if results.send((number, sent)).await.is_err() {
            return;
        }
    }
}

/// Waits for each line's result in input order. Publishes finish in that order too: the
/// publisher has one batch on its way at a time.
async fn report_results(
    json: bool,
    mut results: mpsc::Receiver<(u64, Result<Pending>)>,
    stop: &Notify,
) -> Outcome {
    let mut outcome = Outcome::default();
    while let Some((line, sent)) = results.recv().await {
        let result = match sent {
            Ok(pending) => pending.await,
            Err(err) => Err(err),
        };
        outcome.add(json, line, result);
        if outcome.stopped.is_some() {
            stop.notify_one();
        }
    }
    outcome
}

/// Stdin lines, read on a thread of their own: a read can't be cancelled, and a blocking task
/// would keep berthq from exiting until stdin ends.
fn stdin_lines() -> mpsc::Receiver<std::io::Result<String>> {
    let (sender, receiver) = mpsc::channel(IN_FLIGHT);
    std::thread::spawn(move || {
        for line in std::io::stdin().lock().lines() {
            if sender.blocking_send(line).is_err() {
                return;
            }
        }
    });
    receiver
}

impl Outcome {
    fn add(&mut self, json: bool, line: u64, result: Result<Published>) {
        self.lines += 1;
        match result {
            Ok(published) => {
                if json {
                    output::json_line(&published);
                }
                self.published += 1;
                self.published_after_stop += u64::from(self.stopped.is_some());
            }
            Err(err) if err.kind() == "invalid" => {
                output::note(&format!("line {line}: {err}"));
                self.failed += 1;
            }
            Err(err) => {
                self.failed += 1;
                if self.stopped.is_none() {
                    self.stopped = Some((line, err));
                }
            }
        }
    }

    /// Prints the summary; an error when any line was not published.
    fn finish(self, json: bool, queue: &str) -> Result<()> {
        if !json {
            let count = output::number(self.published);
            output::note(&format!("Published {count} messages to {queue}"));
        }
        if let Some((line, err)) = self.stopped {
            let after = match self.published_after_stop {
                0 => String::new(),
                n => format!("; {n} lines after it were published"),
            };
            output::note(&format!(
                "Stopped at line {line}, the first line not published{after}"
            ));
            return Err(err);
        }
        if self.failed > 0 {
            return Err(Error::Http(format!(
                "{} of {} lines were not published",
                self.failed, self.lines
            )));
        }
        Ok(())
    }
}

/// The message given on the command line, or `None` when it should be read from stdin.
fn single(args: &MessageArgs) -> Option<NewMessage> {
    match args.body.as_deref() {
        Some(body) if body != "-" => Some(message(args, body)),
        _ => None,
    }
}

fn message(args: &MessageArgs, body: &str) -> NewMessage {
    NewMessage {
        body: parse_body(body),
        priority: args.priority,
        delay_ms: args.delay.map(millis),
        deliver_at: args.at.clone(),
        dedup_id: args.dedup_id.clone(),
    }
}

/// JSON when it parses, otherwise the text as a JSON string, so `berthq publish q hello` works.
fn parse_body(text: &str) -> Value {
    serde_json::from_str(text).unwrap_or_else(|_| Value::String(text.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn bodies_are_json_or_plain_text() {
        assert_eq!(parse_body(r#"{"user":123}"#), json!({ "user": 123 }));
        assert_eq!(parse_body("42"), json!(42));
        assert_eq!(parse_body("hello world"), json!("hello world"));
    }
}
