//! `Client::publisher`: collects messages and publishes them in batches.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

use crate::{Client, Error, NewMessage, Published, Result};

/// The broker takes up to 1000 messages per batch.
const MAX_BATCH: usize = 1000;

#[derive(Debug, Clone)]
pub struct PublisherOptions {
    /// Most messages per batch (1-1000).
    pub max_batch: usize,
    /// How long the first message of a batch waits for more. The default, zero, sends right
    /// away: messages that arrive while a batch is on its way form the next one.
    pub linger: Duration,
    /// A batch is sent before its JSON grows past this; a bigger message goes alone.
    pub max_bytes: usize,
}

impl Default for PublisherOptions {
    fn default() -> Self {
        Self {
            max_batch: MAX_BATCH,
            linger: Duration::ZERO,
            max_bytes: 4 * 1024 * 1024,
        }
    }
}

/// Publishes messages in batches: never slower than one request per message, and up to about
/// 10 times faster when many are sent at once. One batch is in flight at a time, so messages
/// keep their order. Clones share the same batches.
#[derive(Debug, Clone)]
pub struct Publisher {
    commands: mpsc::Sender<Command>,
}

/// A message's result, ready once its batch is on disk. The message is published whether or not
/// this is awaited.
#[derive(Debug)]
pub struct Pending(oneshot::Receiver<Result<Published>>);

#[derive(Debug)]
enum Command {
    Publish(Entry),
    Flush(oneshot::Sender<()>),
}

/// A message encoded once, and where its result goes.
#[derive(Debug)]
struct Entry {
    json: Vec<u8>,
    done: oneshot::Sender<Result<Published>>,
}

impl Client {
    /// A publisher for `queue`. It sends batches from a task on the current Tokio runtime, which
    /// ends once every clone of the publisher is dropped.
    pub fn publisher(&self, queue: &str, options: PublisherOptions) -> Publisher {
        let options = PublisherOptions {
            max_batch: options.max_batch.clamp(1, MAX_BATCH),
            ..options
        };
        let (commands, receiver) = mpsc::channel(options.max_batch * 4);
        tokio::spawn(run(self.clone(), queue.to_string(), options, receiver));
        Publisher { commands }
    }
}

impl Publisher {
    /// Adds `message` to the next batch, waiting only while the publisher is full, and returns
    /// its result to await later.
    pub async fn send(&self, message: NewMessage) -> Result<Pending> {
        let json = serde_json::to_vec(&message)
            .map_err(|err| Error::Http(format!("could not encode the message: {err}")))?;
        let (done, result) = oneshot::channel();
        self.commands
            .send(Command::Publish(Entry { json, done }))
            .await
            .map_err(|_| stopped())?;
        Ok(Pending(result))
    }

    /// Adds `message` to the next batch and returns its result once the batch is on disk.
    pub async fn publish(&self, message: NewMessage) -> Result<Published> {
        self.send(message).await?.await
    }

    /// Sends what is waiting now, and returns once it is on disk or failed.
    pub async fn flush(&self) -> Result<()> {
        let (done, flushed) = oneshot::channel();
        self.commands
            .send(Command::Flush(done))
            .await
            .map_err(|_| stopped())?;
        flushed.await.map_err(|_| stopped())
    }

    /// Flushes and drops this publisher.
    pub async fn close(self) -> Result<()> {
        self.flush().await
    }
}

impl Future for Pending {
    type Output = Result<Published>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0)
            .poll(cx)
            .map(|result| result.unwrap_or_else(|_| Err(stopped())))
    }
}

async fn run(
    client: Client,
    queue: String,
    options: PublisherOptions,
    mut commands: mpsc::Receiver<Command>,
) {
    let mut next = None;
    loop {
        let first = match next.take() {
            Some(command) => command,
            None => match commands.recv().await {
                Some(command) => command,
                None => return,
            },
        };
        let (batch, flushed, carried) = collect(first, &mut commands, &options).await;
        next = carried;
        send(&client, &queue, batch).await;
        if let Some(done) = flushed {
            let _ = done.send(());
        }
    }
}

/// Fills a batch until it is full, nothing more is waiting after `linger`, or a flush asks for
/// it. Returns the batch, the flush to answer after sending it, and a message that didn't fit.
async fn collect(
    first: Command,
    commands: &mut mpsc::Receiver<Command>,
    options: &PublisherOptions,
) -> (Vec<Entry>, Option<oneshot::Sender<()>>, Option<Command>) {
    let mut batch = match first {
        Command::Flush(done) => return (Vec::new(), Some(done), None),
        Command::Publish(entry) => vec![entry],
    };
    let mut bytes = batch[0].json.len();
    let deadline = Instant::now() + options.linger;
    while batch.len() < options.max_batch {
        // What is already waiting joins first; waiting for more only while `linger` allows.
        let command = match commands.try_recv() {
            Ok(command) => Some(command),
            Err(TryRecvError::Disconnected) => None,
            Err(TryRecvError::Empty) if Instant::now() >= deadline => None,
            Err(TryRecvError::Empty) => tokio::select! {
                biased;
                command = commands.recv() => command,
                () = tokio::time::sleep_until(deadline) => None,
            },
        };
        match command {
            None => break,
            Some(Command::Flush(done)) => return (batch, Some(done), None),
            Some(Command::Publish(entry)) if bytes + entry.json.len() > options.max_bytes => {
                return (batch, None, Some(Command::Publish(entry)));
            }
            Some(Command::Publish(entry)) => {
                bytes += entry.json.len();
                batch.push(entry);
            }
        }
    }
    (batch, None, None)
}

/// Publishes a batch and hands each message its result. A batch rejected as `invalid` is split
/// in half until the bad messages are alone, so only they fail and the rest still go in order.
async fn send(client: &Client, queue: &str, batch: Vec<Entry>) {
    // Parts still to send, the next one last.
    let mut parts = vec![batch];
    while let Some(mut part) = parts.pop() {
        if part.is_empty() {
            continue;
        }
        let messages: Vec<&[u8]> = part.iter().map(|entry| entry.json.as_slice()).collect();
        match client.publish_encoded(queue, &messages).await {
            Ok(published) if published.len() == part.len() => {
                for (entry, result) in part.into_iter().zip(published) {
                    let _ = entry.done.send(Ok(result));
                }
            }
            Ok(_) => fail_all(part, &Error::Http("unexpected batch answer".to_string())),
            Err(err) if err.kind() == "invalid" && part.len() > 1 => {
                let second = part.split_off(part.len() / 2);
                parts.push(second);
                parts.push(part);
            }
            Err(err) => fail_all(part, &err),
        }
    }
}

fn fail_all(part: Vec<Entry>, err: &Error) {
    for entry in part {
        let _ = entry.done.send(Err(err.clone()));
    }
}

fn stopped() -> Error {
    Error::Http("the publisher has stopped".to_string())
}
