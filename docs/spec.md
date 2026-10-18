# Berthq - product spec

Started 2026-10-11 as "Broker", named Berthq on 2026-10-13: `berthq-*` crates and a `berthq` binary.

## Vision

A message broker you can start with one command and understand in five minutes. One binary, no
Erlang, no ZooKeeper, no configuration files. Persistent queues with retries, dead letters,
delays and priorities, replay like Kafka without partitions, a built-in dashboard, and an HTTP
API simple enough to use with `curl`.

```
berthq start
berthq queue create orders
berthq publish orders '{"id":1}'
berthq consume orders                               # a plain job queue
berthq consume orders --consumer billing            # a second service gets every message too
berthq consumer replay orders billing --from 2026-10-14T09:00:00Z
```

## Why

- RabbitMQ is powerful but brings Erlang, plugins, exchanges, routing and a long manual.
- Kafka can replay, but it is a log, not a job queue, and needs partitions and a cluster.
- Redis-based queues lose messages unless configured carefully and have no shared dashboard.
- SQS is great but cloud-only and not something you run on your laptop or in CI.

Berthq: the job-queue features people actually use, plus replay, with at-least-once delivery,
in a single binary with a dashboard that shows what is going on.

## Principles

1. **Zero config.** Every setting has a default; flags and `BERTHQ_*` env vars override them.
   No config file.
2. **Never lose an acknowledged publish.** A publish returns only after it is on disk.
   Unacknowledged messages survive crashes and restarts (at-least-once).
3. **HTTP is the protocol.** Every feature works with `curl`. SDKs are thin and optional.
4. **One name per concept** across core, HTTP, CLI, dashboard and SDKs.
5. **Small and readable.** A contributor should be able to read the delivery code in one sitting.
6. **Safe by default.** Berthq refuses to listen on a network address without API keys unless
   told so with `--no-auth`.

## Non-goals (for now)

- Clustering. One node, run like Postgres: a durable disk, backups, fast restarts. A standby
  copy may come later; a 3-5 node cluster is not planned.
- Partitions, routing keys, exchanges and server-side filters. Use one queue per kind of message.
- AMQP, MQTT, STOMP or Kafka wire protocols.
- Exactly-once delivery. Berthq is at-least-once; [guarantees.md](guarantees.md) shows how to
  never do the work twice.

## Concepts

- **Queue**: a named, persistent list of messages. Names use `a-z A-Z 0-9 . _ -`, start with a
  letter or digit, are up to 128 characters and don't end in `.dlq` (that is left to dead-letter
  queues). Created explicitly (`berthq queue create`).
- **Message**: a JSON body plus metadata: `id` (ULID, sortable by publish time), `priority`
  (0-9, higher first), `deliverAt` and `dedupId`. The body is stored and delivered exactly as it
  was sent, and its 4 MB limit counts it as sent. Messages are not deleted on ack: they stay
  for the queue's retention (see below).
- **Consumer**: a name with a saved position in a queue, for example `billing`. Every consumer
  gets every message, at its own pace. A queue can have up to 100 consumers. Calls that don't
  name a consumer use `default`, so a plain job queue never has to think about consumers.
- **Worker**: a process that consumes for a consumer. Workers of one consumer share its
  messages: each message goes to one of them. Workers pass an optional name so the dashboard
  can show who is connected.
- **Position**: where a consumer is in the queue. A consumer that doesn't exist yet is created
  on its first `consume`, at the start of the queue (the oldest kept message) or at `from`.
  After that it always continues where it stopped: what it acked is done, the rest comes again.
- **Replay**: moves a consumer's position to `start`, `end`, a time or a message id.
- **Lease**: a delivered message is leased to its worker for the queue's visibility timeout.
  The worker `ack`s (done) or `nack`s (failed, retried). If the lease runs out, that counts as
  a failed attempt. With `autoAck` there is no lease: the position moves on delivery.
- **Dead-letter queue (DLQ)**: when a consumer runs out of attempts for a message, a copy
  moves to `<queue>.dlq`, created on demand, with the consumer, the reason and the last error.
  Only Berthq creates DLQs; their settings can be changed like any queue's. `requeue` moves
  dead letters back to the consumer that failed them.

## Delivery semantics

- **Order**: per consumer, highest priority first, then in the order messages became visible.
  A consumer's retries keep their place: once their backoff ends they come before newer
  messages of the same priority.
- **At-least-once** (default): a message is done for a consumer only on `ack`. Crashes,
  restarts, expired leases and disconnects all lead to redelivery. Consumers must be idempotent.
- **At-most-once** (`autoAck: true` on consume): the position moves when messages are
  delivered, with no lease, retries or dead letters. A worker crash loses those messages for
  that consumer; a replay can bring them back while they are still kept.
- **Retries**: `nack` or an expired lease schedules the next attempt after an exponential
  backoff: `retryDelayMs * 2^(attempts-1)`, capped at 5 minutes (or at `retryDelayMs` when that
  is longer). `nack` may pass an explicit delay, or `deadLetter: true` to skip remaining
  attempts. Attempts count per consumer.
- **Dead letters**: after `maxAttempts` deliveries to one consumer, a copy of the message moves
  to the DLQ for that consumer. Other consumers are not affected. A DLQ has no DLQ of its own:
  a message that runs out of attempts there is dropped and counted as `dropped`.
- **Delays**: `delayMs` or `deliverAt` (RFC 3339) hide a message until then, at most 365 days
  ahead. A delayed message joins the queue when its time comes; until then no consumer sees it.
  A nack's `delayMs` has the same limit.
- **Retention**: messages stay at least `retentionMs` (default 24 hours) after they became
  visible; that is how far back a replay can go. After that, a message is deleted once every
  consumer has acked it, so a message someone still has to process is never deleted by age. A
  queue without consumers keeps its messages until the first consumer has processed them.
- **Max size**: `maxMessages` limits the backlog: messages that at least one consumer still has
  to process, plus delayed ones. Messages kept only for replay don't count. When full,
  publishes fail with `queueFull`.
- **Free disk space**: while the disk holding the data has less than `--min-free-disk` free
  (default 1 GB), publishes fail with `diskFull`. Consumes, acks and deletes keep working, so
  the backlog drains and retention frees space; publishing then works again by itself.
- **Deduplication**: a publish with a `dedupId` already seen on that queue in the last hour
  returns the original message id with `duplicate: true` and stores nothing.
- **Pause**: a paused queue accepts publishes but delivers nothing; a paused consumer gets
  nothing while other consumers continue.
- **Replay and remove**: replaying or removing a consumer drops what it was processing and
  retrying. Leases from before become unknown, so their acks are ignored. A replay may deliver
  messages again on purpose. Requeued dead letters still come only once: a replay keeps the
  ones the consumer won't get again from the queue, as if they were just requeued, and drops
  the others. In the same way, requeueing a dead letter that the consumer will get from the
  queue anyway only removes it from the DLQ.

## Queue settings

| Setting               | Default  | Meaning                                                       |
| --------------------- | -------- | ------------------------------------------------------------- |
| `retentionMs`         | 86400000 | How long messages stay after they become visible (for replay) |
| `visibilityTimeoutMs` | 30000    | Lease length before an unacked message is retried             |
| `maxAttempts`         | 5        | Deliveries before dead-lettering (0 = retry forever)          |
| `retryDelayMs`        | 1000     | First retry delay, doubled per attempt up to 5 minutes        |
| `maxMessages`         | none     | Backlog limit                                                 |

`retentionMs` is 0 to 365 days. With `retentionMs: 0` a message is deleted as soon as every
consumer has acked it, like a classic job queue. Everything else is a fixed rule, not a
setting: messages are kept until every consumer has acked them, exhausted messages always go
to the DLQ, a full queue rejects publishes and a `dedupId` is remembered for one hour.

## Consumers

- Created by the first `consume` that names them (`from` decides the starting point) or
  explicitly with `POST /v1/queues/{queue}/consumers`.
- `from`: `start` (default, the oldest kept message), `end` (only messages published from now
  on), an RFC 3339 time (messages that became visible at or after it) or a message id (that
  message and everything after it).
- **Replay** (`POST …/consumers/{consumer}/replay { from }`) moves the position the same way.
- Consumers are never removed automatically: a removed consumer that comes back starts again
  from `start` and would redo everything. The dashboard shows consumers not seen for a while
  and how much they hold back.

## Storage and durability

- One embedded [redb](https://github.com/cberner/redb) database file in the data directory
  (`./data/berthq.redb` by default). redb is ACID and crash-safe, written in Rust.
- **Format version**: the file records its format version. Berthq upgrades older formats on
  start and refuses files from a newer Berthq or from development builds before versioning.
- **Disk first, RAM stays flat.** Messages, bodies and every index that decides what is
  delivered next (each consumer's position, its leases and retries, delays, dedup ids) are
  tables in the redb file. RAM holds per-queue and per-consumer settings, counts and
  positions, connected workers and the last minute of throughput, plus redb's page cache
  (capped at 64 MB, and 64 MB more for the copy while a backup runs). Memory does not grow
  with the number of messages.
- **Bodies as sent**: a body is kept as the JSON text it was sent as, spaces and escapes
  included. Berthq checks that it is valid JSON that every client can read back (numbers in
  range, no lone surrogates, at most 128 levels deep) but never builds it into a tree, so a
  body takes about its own size in memory while it is published or delivered.
- Attempts are saved when a message is delivered, so a message that keeps crashing its worker
  (or the broker) still ends up in the dead-letter queue.
- **Group commit**: one writer thread batches concurrent publishes, acks and moves into a single
  transaction, so throughput stays high while every write is fsynced before it is confirmed.
- **Crash recovery**: on start, queues, consumers and their counts are read from disk (no scan
  of the messages). A message that was leased when the broker stopped counts as a failed
  attempt, like an expired lease: it is retried after the backoff, or dead-lettered when it is
  out of attempts.
- **Storage failures**: when the disk fails a commit, redb refuses every write until the file is
  reopened. Berthq then stops writing, as it does when a bug makes a write panic: every write
  fails with `storage` and the first reason, and `/healthz` answers 503 so a supervisor (the
  Kubernetes liveness probe) restarts it. redb repairs the file on the next start.
- **Graceful shutdown** (SIGINT/SIGTERM): stop accepting work, stop delivering, wait up to
  `--shutdown-timeout` for in-flight messages to be acked, flush the writer, close the file.
- **Backups**: `berthq backup <file>` (`GET /v1/backup`) writes a consistent copy of the
  database while Berthq keeps running. One backup runs at a time (another one fails with
  `conflict`), and only while the disk has room for the copy plus `--min-free-disk` (otherwise
  `diskFull`). To restore, stop Berthq and put the file in the data directory as `berthq.redb`.

## Security

- Without API keys Berthq only listens on a loopback address (`127.0.0.1`, `::1`). On any other
  address it refuses to start unless `--no-auth` (`BERTHQ_NO_AUTH=true`) says the network is
  private on purpose.
- Without API keys, browser requests from other sites are refused (`Sec-Fetch-Site`, `Origin`),
  and on a loopback address so are host names other than `localhost` and loopback addresses
  (DNS rebinding). curl and the SDKs send neither header, so only the host name matters for
  them. With keys nothing is checked: a page can't send the key.
- API keys have scopes: `admin`, `publish`, `consume`, `read`. A key with several scopes joins
  them with `+`: `--api-key publish+consume:KEY`. `BERTHQ_API_KEYS` takes a comma-separated
  list. Keys are at least 16 characters and rotate with a restart.
- Keys are sent as `Authorization: Bearer KEY`. `?apiKey=` works only for `GET /v1/events`
  (EventSource can't set headers).
- A client has 30 seconds to send a request's headers; idle HTTP/1.1 connections close after
  30 seconds. Bodies and answers (long-polls, events) have no time limit.
- TLS with `--tls-cert` and `--tls-key`, or at a load balancer in front of Berthq.
- Never logged: message bodies and API keys.

## HTTP API

JSON in and out, camelCase fields, errors as `{ "error": { "kind", "message" } }` with a fitting
status code. Kinds: `invalid` (400), `unauthorized` (401), `forbidden` (403), `notFound` (404),
`conflict` (409), `queueFull` (507), `diskFull` (507), `unavailable` (503, shutting down),
`storage` (500).
Details in [api.md](api.md).

| Method   | Path                                               | What                                        |
| -------- | -------------------------------------------------- | ------------------------------------------- |
| `GET`    | `/v1/queues`                                       | List queues with stats                      |
| `POST`   | `/v1/queues`                                       | Create a queue `{ name, ...settings }`      |
| `GET`    | `/v1/queues/{queue}`                               | Settings, stats, consumers                  |
| `PATCH`  | `/v1/queues/{queue}`                               | Update settings                             |
| `DELETE` | `/v1/queues/{queue}`                               | Delete a queue and its messages             |
| `POST`   | `/v1/queues/{queue}/purge`                         | Delete all messages                         |
| `POST`   | `/v1/queues/{queue}/pause`                         | Stop deliveries to every consumer           |
| `POST`   | `/v1/queues/{queue}/resume`                        | Resume deliveries                           |
| `POST`   | `/v1/queues/{queue}/messages`                      | Publish one message                         |
| `POST`   | `/v1/queues/{queue}/batch`                         | Publish many `{ messages: [...] }`          |
| `GET`    | `/v1/queues/{queue}/messages`                      | Browse messages, newest first               |
| `GET`    | `/v1/queues/{queue}/messages/{id}`                 | One message with body                       |
| `DELETE` | `/v1/queues/{queue}/messages/{id}`                 | Delete one message                          |
| `POST`   | `/v1/queues/{queue}/consume`                       | Lease up to `max` messages                  |
| `POST`   | `/v1/queues/{queue}/ack`                           | `{ leases: [...] }`                         |
| `POST`   | `/v1/queues/{queue}/nack`                          | `{ leases, delayMs?, deadLetter?, error? }` |
| `POST`   | `/v1/queues/{queue}/extend`                        | `{ leases, visibilityTimeoutMs? }`          |
| `POST`   | `/v1/queues/{queue}/requeue`                       | Move dead letters back `{ ids? }`           |
| `GET`    | `/v1/queues/{queue}/consumers`                     | List consumers                              |
| `POST`   | `/v1/queues/{queue}/consumers`                     | Create a consumer `{ name, from? }`         |
| `GET`    | `/v1/queues/{queue}/consumers/{consumer}`          | One consumer                                |
| `DELETE` | `/v1/queues/{queue}/consumers/{consumer}`          | Remove a consumer                           |
| `POST`   | `/v1/queues/{queue}/consumers/{consumer}/replay`   | Move its position `{ from }`                |
| `POST`   | `/v1/queues/{queue}/consumers/{consumer}/pause`    | Stop its deliveries                         |
| `POST`   | `/v1/queues/{queue}/consumers/{consumer}/resume`   | Resume its deliveries                       |
| `GET`    | `/v1/queues/{queue}/consumers/{consumer}/messages` | What it is processing, retrying, next       |
| `GET`    | `/v1/backup`                                       | A consistent copy of the database           |
| `GET`    | `/v1/stats`                                        | Totals, rates, last minute of history       |
| `GET`    | `/v1/events`                                       | Server-sent events: stats every second      |
| `GET`    | `/metrics`                                         | Prometheus metrics                          |
| `GET`    | `/healthz`                                         | Liveness (503 once storage has failed)      |

Everything else serves the dashboard.

## CLI

```
berthq start [--host 127.0.0.1] [--port 7331] [--data ./data] [--min-free-disk 1GB]
             [--shutdown-timeout 10s] [--api-key scope+scope:KEY] [--no-auth]
             [--tls-cert cert.pem --tls-key key.pem]
berthq queue create|update <name> [settings]
berthq queue list | get | delete | purge | pause | resume | requeue <name>
berthq publish <queue> <body|-> [--priority 5] [--delay 10s] [--at <time>] [--dedup-id id]
berthq consume <queue> [--consumer default] [--worker name] [--count 10] [--from start|end|<time>|<id>]
               [--auto-ack] [--exec "cmd"]
berthq consumer list <queue>
berthq consumer create <queue> <consumer> [--from start|end|<time>|<id>]
berthq consumer get | remove | pause | resume <queue> <consumer>
berthq consumer replay <queue> <consumer> --from start|end|<time>|<id>
berthq messages <queue> [--consumer name --state waiting|processing|retrying] [--id <id>]
berthq backup <file>
berthq stats
```

Client commands talk to `--url` (default `http://127.0.0.1:7331`, env `BERTHQ_URL`), send
`--key` (env `BERTHQ_API_KEY`) and print JSON with `--json`. `consume` prints and acks each
message; with `--exec` it pipes the body to the command and acks on exit code 0, nacks
otherwise. `publish -` publishes one message per stdin line, in batches. A line the broker
refuses as `invalid` is reported with its line number and skipped; any other error (the broker
gone, `queueFull`, `diskFull`) stops reading, and `publish` says how many messages were stored
and the first line that was not. The exit code is 1 when a line was not published.
`--dedup-id` needs the body on the command line. `consume` sends its acks with the next consume
call, and the rest on their own when it stops. While the broker restarts or is out of reach, it
logs a warning and goes on.

`consume --exec` runs each command in its own process group, so Ctrl-C reaches only `berthq`:
the running command and the rest of the messages already received (up to `--batch`, 10 by
default) are finished and acked or nacked, then `consume` stops. A second Ctrl-C stops at once
with exit code 130: it interrupts the running command, and messages not acked yet come again
once their lease runs out.

## SDKs

Rust, Node, Python, Go and PHP (with a Laravel queue driver), all over the HTTP API and with
the same method names (`queues_create`, `consume`, `consumers_replay`, …). Every SDK:

- keeps connections open and reuses them, and speaks HTTP/2 over TLS where the language makes
  that easy;
- has a **publisher** (`client.publisher(queue)`) that sends messages in batches of up to 1000
  (or 4 MiB). A message goes out right away; the messages that arrive while that batch is on its
  way form the next one (a `linger` option can make a batch wait for more). One batch is in
  flight at a time, so messages keep their order. Each `publish` returns that message's result
  once its batch is on disk. If a batch is rejected as `invalid`, it is split in half until the
  bad message is alone, so only it fails. PHP, which has no background work, sends the batch on
  `flush()`, when it is full, or when the publisher is destroyed;
- has a **worker loop** (`process`) that acks with its next consume call and sends the
  remaining acks on their own when it stops.

## Dashboard

Served by the broker at `/`. Live over `/v1/events`.

- **Overview**: messages/sec in and out, backlog, processing, failed (in DLQs), throughput chart
  for the last minute, queue table.
- **Queue**: stats and throughput; tabs for messages (newest first, with body viewer), consumers
  (waiting, processing, retrying, workers, last seen; replay, pause/resume, remove), dead
  letters and settings; actions: publish, purge, pause/resume, requeue dead letters, delete.
- A banner on every page while publishing is stopped because the disk is almost full.

## Deployment

- **Releases**: a `v*` tag publishes the image (amd64 and arm64) and Linux and macOS binaries
  to a draft GitHub release, each with a GitHub build attestation (`.github/workflows/release.yml`).
- **Install script**: `install.sh` (`curl -fsSL …/main/install.sh | sh`) downloads the latest
  release for macOS or Linux (glibc 2.35+, as built on Ubuntu 22.04), checks its SHA-256, runs
  it once and only then installs `berthq`, without sudo.
- **Docker**: the image (`ghcr.io/talovicnedim/berthq`) is the one binary on distroless,
  non-root, with `/data` as a volume. It listens on `0.0.0.0`, so it needs an API key
  (`BERTHQ_API_KEYS`) or an explicit `BERTHQ_NO_AUTH=true`.
- **Kubernetes**: `deploy/kubernetes/berthq.yaml`: a StatefulSet with exactly one replica and a
  persistent volume, a ClusterIP Service, API keys from a Secret, `/healthz` probes, and a
  NetworkPolicy so only labelled pods can connect.

## Roadmap

### v0.1 - single-node broker

Queues, delivery, retries, dead letters, delays, priorities, TTL, dedup, dashboard, CLI, Docker.

### v0.2 - integrations

Done: per-queue delivery rate limits, webhook consumers (HMAC-SHA256), API keys with scopes,
TLS, SDKs for Rust, PHP/Laravel (queue driver), Node, Python and Go. Topics with routing keys
were built here and replaced by consumers in v0.3.

### v0.3 - consumers and replay

Consumers with saved positions and replay, retention, auto-ack, backups, the format version,
safe-by-default auth, a Kubernetes manifest, [guarantees.md](guarantees.md) and a guard
against a full disk. Removed to keep Berthq small: webhook consumers, inbound webhooks (SNS),
rate limits, message TTLs, headers and seven queue settings (see the decisions below).

### Later

- Recurring schedules (cron) on top of `deliverAt`.
- `maxInFlight`: at most N messages of a queue being processed at once.
- `berthq compact`: shrink the data file after a big spike.
- Compressing large message bodies on disk (pure Rust, e.g. `lz4_flex`), invisible to clients.
- Homebrew.
- Order per key (all messages with the same key in order, one at a time).
- A standby copy on a second machine.
- A `concurrency` option in the Rust, Python and PHP worker loops (Node and Go have one) and an
  async Python client.
- Back only if people ask for them: webhooks (in and out), rate limits, TTLs, headers.

## Decisions

- **2026-10-11 redb over a hand-written log or SQLite.** Pure Rust, ACID, no C toolchain, and a
  single file. A custom WAL would be faster in theory but is the riskiest code to own.
- **2026-10-11 long-polling over WebSocket consumers.** Long-polling works with `curl`, every
  HTTP client and every proxy; acks are plain requests. SSE (not WebSocket) for monitoring
  because it is one-way and needs no client library.
- **2026-10-11 JSON bodies.** Messages carry JSON; plain text is a JSON string. Binary payloads
  can be base64-encoded by the producer.
- **2026-10-11 Leases as receipts.** Ack/nack take the lease id from the delivery, not the
  message id, so a slow worker whose lease expired cannot ack or nack someone else's delivery.
  A lease id names its consumer, so acks need no consumer field.
- **2026-10-11 clap for the CLI.** With a dozen subcommands and flags, clap's generated help and
  validation is worth the dependency.
- **2026-10-11 Hand-written TypeScript types for the dashboard.** The API needs `u64` counters and
  arbitrary JSON bodies, which specta refuses to export. The HTTP API is the contract (like for
  the SDKs); `apps/dashboard/src/lib/api/types.ts` mirrors it and the server's JSON tests pin it.
- **2026-10-12 Indexes on disk, not in RAM.** The first version kept message metadata and the
  delivery indexes in memory (a few hundred bytes per queued message). Moving them into redb
  tables keeps memory flat regardless of queue depth, at the cost of a write per delivery
  (absorbed by group commit). Measured: 250k queued messages, 39 MB RSS (was 189 MB).
- **2026-10-12 API keys are flags, not stored state.** `--api-key scope:KEY` (or
  `BERTHQ_API_KEYS`) keeps "no configuration files" true; keys rotate with a restart.
- **2026-10-14 One concept: queues with consumers, no topics.** RabbitMQ-style topics (bindings,
  routing-key patterns, a copy per queue) were replaced by consumers that each read the same
  queue at their own pace. Fan-out, replay and job queues are one model; nothing is copied.
- **2026-10-14 Consumer and worker.** The name with a position is the consumer (`billing`); the
  process is a worker. Kafka calls these group and consumer; NATS uses consumer like Berthq.
- **2026-10-14 Retention 24 hours, kept until acked.** Replay of the last day works out of the
  box, and a message someone still has to process is never deleted by age. `retentionMs: 0`
  gives a classic job queue.
- **2026-10-14 New consumers start at the oldest kept message.** That is what a job queue needs
  (the first worker gets the waiting jobs). Services that must not see old messages pass
  `from: "end"`.
- **2026-10-14 Priorities as lanes.** Each priority keeps its own order, and a consumer reads
  the highest lane first. Positions stay simple and replay works per lane.
- **2026-10-14 Delays join the queue when due.** A delayed message is invisible to every consumer
  until its time, so it never holds back positions or retention.
- **2026-10-14 Safe by default.** Berthq refuses a network address without API keys unless
  `--no-auth` is given; the Docker image listens on `0.0.0.0` and so needs a key.
- **2026-10-14 `+` joins scopes.** `publish,consume:KEY` clashed with the comma that separates
  keys in `BERTHQ_API_KEYS`; Berthq rejected the key. Scopes are now joined with `+`.
- **2026-10-14 HTTP stays the protocol.** Measured on a laptop: plain HTTP requests ~72,000/s;
  single publishes 11,000/s with 128 in flight (each waits for the disk) and 62,000/s with
  1,024, where group commit shares the flushes and a build without fsync was no faster. A binary protocol would only
  save the small HTTP part. Speed comes from batching instead: acks inside consume, a batching
  publisher in every SDK, reused connections and HTTP/2.
- **2026-10-14 Backups as a consistent copy.** Copying the file while Berthq writes isn't safe;
  `GET /v1/backup` copies every table inside one read transaction.
- **2026-10-14 Fewer features, fixed rules.** Webhook consumers, inbound webhooks (SNS), rate
  limits, message TTLs, headers, `X-Api-Key` and basic auth were removed, and seven queue
  settings became fixed rules: messages are kept until acked, dead letters are always on,
  backoff stops growing at 5 minutes, a full queue rejects publishes and a `dedupId` is
  remembered for one hour. Nothing was released yet, so nothing broke, and each can come back
  later without breaking clients. A worker can skip stale messages with `publishedAt`, and extra
  data goes in the body.
- **2026-10-14 Stop publishing before the disk is full.** On a completely full disk even
  deletes can fail, because the database needs free space to write them, and the broker is
  stuck. Berthq refuses publishes below `--min-free-disk` (default 1 GB) and keeps consuming,
  acking and trimming, like RabbitMQ's disk alarm. `fs4` reads the free space, which std can't.
- **2026-10-17 Only Berthq makes `.dlq` queues.** Berthq knows a dead-letter queue by its name.
  A hand-made `audit.dlq` silently dropped its own consumers' failures, and an `orders.dlq`
  made before `orders` collected its dead letters. Creating a queue whose name ends in `.dlq`
  now fails with `invalid`.
- **2026-10-17 Bodies stay JSON text.** Bodies were parsed into a tree on publish and again on
  every delivery, and a tree of many small values takes far more memory than its text: one
  consume of five 4 MB `[0,0,…]` messages peaked at 787 MB. Berthq now only checks that a body
  is valid JSON and keeps the text (88 MB for the same consume). A body is stored and delivered
  exactly as it was sent, and the 4 MB limit counts it as sent. The 16 MiB budget that made
  publishes wait their turn went too: each publish now holds about its own size either way.
