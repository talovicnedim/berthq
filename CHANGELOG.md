# Changelog

## Unreleased

- PHP SDK: the Laravel driver adds a `berthq` connection from the `BERTHQ_*` env vars, so
  `QUEUE_CONNECTION=berthq` is all an app needs; a connection in `config/queue.php` still
  replaces it
- PHP SDK: `create_queues` is on by default, like Laravel's Redis and database drivers; set it
  to `false` to create queues explicitly
- PHP SDK: Laravel 13 support (tested on Laravel 11, 12 and 13)

## 0.1.0 (2026-10-18)

First public release.

- One binary (`berthq start`) with no configuration file: every setting has a default, and flags
  or `BERTHQ_*` environment variables override it
- Persistent queues in one embedded [redb](https://github.com/cberner/redb) file: a publish
  returns only after it is on disk, and memory stays flat however deep the queues get
- Consumers with saved positions: every consumer gets every message at its own pace, workers of
  one consumer share its messages, and a replay moves a consumer back to the start, the end, a
  time or a message id
- At-least-once delivery with leases, ack, nack and extend, or at-most-once with `autoAck`
- Retries with exponential backoff, dead-letter queues (`<queue>.dlq`) per consumer and
  `requeue` to send dead letters back
- Delays (`delayMs`, `deliverAt`, up to 365 days), priorities 0-9, deduplication by `dedupId`
  for an hour, retention for replay (24 hours by default), `maxMessages` backlog limits, and
  pausing a queue or a single consumer
- Long-polling consume with acks in the next call, batch publish, and bodies stored and
  delivered exactly as sent (JSON, up to 4 MB)
- HTTP API that works with `curl`, server-sent events (`/v1/events`), Prometheus metrics
  (`/metrics`) and `/healthz`
- Built-in dashboard: live throughput, backlog and processing, queues, consumers and how far
  behind they are, a message browser, dead letters, settings and a banner while the disk is
  almost full
- `berthq` CLI for every API call, including `consume --exec` to pipe each message into a
  command and `publish -` to publish one message per stdin line
- Safe by default: without API keys Berthq listens only on loopback and refuses requests from
  other sites; API keys with `admin`, `publish`, `consume` and `read` scopes; TLS with
  `--tls-cert` and `--tls-key`
- Publishing stops before the disk is full (`--min-free-disk`) while consuming keeps draining
- Online backups (`berthq backup`), crash recovery, graceful shutdown and a versioned file format
- SDKs for Rust, Node.js, Python, Go and PHP (with a Laravel queue driver), each with a batching
  publisher and a worker loop
- Docker image (amd64, arm64), Linux and macOS binaries with build attestations, an install
  script (`curl -fsSL …/install.sh | sh`) and a Kubernetes manifest
