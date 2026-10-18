# berthq-client (Rust)

Rust client for Berthq, the single-binary message broker. Async, built on `reqwest` and `tokio`.

```toml
[dependencies]
berthq-client = { git = "https://github.com/talovicnedim/berthq" }
```

## Quick start

```rust
use berthq_client::{Client, NewMessage};
use serde_json::json;

#[tokio::main]
async fn main() -> berthq_client::Result<()> {
    let client = Client::new("http://127.0.0.1:7331")?; // .with_api_key("…")

    client.publish("emails", &NewMessage::new(json!({ "user": 123 }))).await?;

    // Runs until Ctrl-C: Ok acks, Err nacks (retried with backoff, then dead-lettered).
    let stop = async { tokio::signal::ctrl_c().await.ok(); };
    client
        .process("emails", Default::default(), stop, |message| async move {
            println!("sending email to {}", message.body["user"]);
            Ok::<(), String>(())
        })
        .await
}
```

`process` long-polls, extends leases while a handler runs, and finishes the messages it already
received before stopping. Its acks travel with the next consume call (one request per batch
instead of two) and the rest are sent when it stops, also when it stops with an error. While
the broker restarts or is out of reach, it logs a warning through `tracing` and goes on; a
message whose ack or nack was lost then comes again once its lease runs out. Pass
`ProcessOptions { consumer: Some("billing".into()), .. }` to read with your own position: every
consumer gets every message, and its workers share them.

Replay a consumer from a time, `start`, `end` or a message id:

```rust
client.consumers_replay("orders", "billing", "2026-10-14T09:00:00Z").await?;
```

## Speed

Publish through a `Publisher`: it sends messages in batches of up to 1000 (or 4 MiB). A message
goes out right away, and the messages that arrive meanwhile form the next batch (`linger` can
make a batch wait for more). Never slower than one request per message, and up to about 13 times
faster when many are sent at once. One batch is in flight at a time, so messages keep their
order, and each `publish` returns once its batch is on disk. If a batch is rejected as
`invalid`, it is split in half until the bad message is alone, so only it fails.

```rust
use berthq_client::PublisherOptions;

let publisher = client.publisher("emails", PublisherOptions::default());
let mut results = Vec::new();
for user in &users {
    // `send` only waits while the publisher is full; the result comes later.
    results.push(publisher.send(NewMessage::new(json!({ "user": user.id }))).await?);
}
for result in results {
    result.await?;
}
publisher.close().await?;
```

`publish` is `send` plus waiting for the result, for code that sends one message at a time.

Writing your own worker loop? Send the last batch's leases in `ConsumeOptions { ack, .. }`
instead of calling `ack`, keep the batch's leases extended until then, and ack the rest on
their own before you stop. Connections are kept open and reused, and HTTPS uses HTTP/2 when the
broker offers it.

## API

Method names match the [HTTP API](../../docs/api.md):

| Area       | Methods                                                                                                                                                    |
| ---------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Queues     | `queues_list`, `queues_get`, `queues_create`, `queues_update`, `queues_remove`, `queues_purge`, `queues_pause`, `queues_resume`                            |
| Publishing | `publish`, `publish_batch`, `publisher` (`publish`, `flush`, `close`)                                                                                      |
| Consuming  | `consume`, `ack`, `nack`, `extend`, `process`                                                                                                              |
| Consumers  | `consumers_list`, `consumers_get`, `consumers_create`, `consumers_remove`, `consumers_replay`, `consumers_pause`, `consumers_resume`, `consumers_messages` |
| Messages   | `messages_list`, `messages_get`, `messages_remove`, `requeue`                                                                                              |
| Other      | `backup`, `stats`                                                                                                                                          |

`backup(path)` downloads to `<path>.partial` and renames it once the whole copy is on disk, so a
download that fails leaves no file behind. It refuses a `path` that already exists.

A delivery's `id` stays the same on every redelivery and replay (also after a dead letter is
requeued), so it works as an idempotency key. With `auto_ack`, deliveries have no `lease`.

## Errors

Every method returns `berthq_client::Result<T>`. `Error::Api { status, kind, message }` carries
the broker's error kind (`notFound`, `queueFull`, `diskFull`, `unauthorized`, …; also `err.kind()`);
`Error::Unreachable` means no connection could be made (nothing listens there, an unknown host,
a TLS failure); `Error::Http` covers timeouts, unreadable responses and local file errors. Both have
the kind `http`.
