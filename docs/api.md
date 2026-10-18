# HTTP API

Everything the broker does is available over HTTP on one port (default `7331`), with HTTP/1.1
or HTTP/2 (over TLS, or without TLS with prior knowledge: `curl --http2-prior-knowledge`).
Request and response bodies are JSON with camelCase fields. The `Content-Type` header is optional, so
`curl -d '…'` works as is. Durations are milliseconds (`…Ms`); timestamps are RFC 3339 strings.

A request body may be up to 16 MiB for a publish or a batch and 1 MiB for everything else; a
larger one gets `400 invalid`. A message body is stored and delivered exactly as it was sent, and
its 4 MB limit counts it as sent, spaces and escapes like `\u00e9` included. A body that JSON
parsers can't read back gets `400 invalid` too: a number out of range (`1e400`), a lone
surrogate escape (`"\ud800"`) or nesting deeper than 128 levels.

A client has 30 seconds to send a request's headers, and an idle HTTP/1.1 connection is closed
after 30 seconds without a new request; clients open a new one. An HTTP/2 connection that is
quiet for 30 seconds gets a ping and is closed when the client doesn't answer within another
30 seconds. Request bodies and answers have no time limit, so slow uploads, long-polls
(`waitMs`) and `/v1/events` keep working.

## Authentication

Without API keys Berthq listens only on `127.0.0.1` (or with `--no-auth` on a private network)
and the API is open. With keys, every API request needs one:

```sh
curl -H "Authorization: Bearer $KEY" localhost:7331/v1/queues
curl "localhost:7331/v1/events?apiKey=$KEY"   # only GET /v1/events: EventSource can't set headers
```

The `Bearer` scheme is case-insensitive. In `?apiKey=`, `%XX` escapes are decoded and a `+` stays
a plus, so a key with `+` works as is; write a space as `%20`.

A key has one or more scopes, set when starting the broker (`--api-key publish:KEY`,
`--api-key publish+consume:KEY` for an app that both produces and works jobs):

| Scope     | Allows                                                                            |
| --------- | --------------------------------------------------------------------------------- |
| `admin`   | Everything (keys without a scope)                                                 |
| `publish` | `POST …/messages` and `…/batch`                                                   |
| `consume` | `POST …/consume`, `…/ack`, `…/nack`, `…/extend`                                   |
| `read`    | Every `GET` and `HEAD` except `/v1/backup`, including `/v1/events` and `/metrics` |

A missing or unknown key gets `401 unauthorized`, a key without the right scope `403
forbidden`. The consumer routes (`…/consumers…`: create, remove, replay, pause, resume) and
`/v1/backup` need `admin`. A `consume` key can still create a consumer: `POST …/consume` creates
the consumer it names on first use. The dashboard's files and `/healthz` stay public; the
dashboard asks for a key, also when the saved one has no `read` scope.

Without keys, browser requests from other web sites get `403 forbidden`, and so do host names
other than `localhost` and loopback addresses (`127.0.0.1`, `[::1]`) while Berthq listens on a
loopback address. This stops web pages from using the API through your browser. curl and the
SDKs are not affected.

## Errors

Failures return a status code and

```json
{ "error": { "kind": "notFound", "message": "queue emails not found" } }
```

| Kind           | Status | When                                                                            |
| -------------- | ------ | ------------------------------------------------------------------------------- |
| `invalid`      | 400    | Bad input: a name, priority, `from`, unknown field, bad JSON, a body too big    |
| `invalid`      | 405    | The path exists, but not with this method (`PUT …/consume`)                     |
| `unauthorized` | 401    | Missing or unknown API key                                                      |
| `forbidden`    | 403    | The key lacks the scope, or (without keys) a browser request from another site  |
| `notFound`     | 404    | The queue, consumer, message or API route doesn't exist                         |
| `conflict`     | 409    | Creating a queue or consumer that already exists, or a backup while one runs    |
| `queueFull`    | 507    | Publishing would take the backlog past `maxMessages`                            |
| `diskFull`     | 507    | Free disk space is below `--min-free-disk` (see below), or too low for a backup |
| `unavailable`  | 503    | The broker is shutting down; retry against the next start                       |
| `storage`      | 500    | Writing to disk failed                                                          |

Paths that can only be a mistyped API URL get the JSON `404 notFound` too, not the dashboard:
`/metrics/…`, `/healthz/…`, `/v2/…` (any `/v<digits>`) and paths starting with `//`.

`storage` with `writes stopped after an error, restart berthq: …`: the disk failed a commit, or
a bug made a write panic. Berthq then refuses every write until it is restarted, and
`/healthz` answers 503 with the same message.

`diskFull`: Berthq stops taking publishes while the disk holding its data has less than
`--min-free-disk` free (default 1 GB). Consumes, acks, nacks and deletes keep working, so
workers drain the backlog, retention deletes old messages and publishing works again by itself.
`GET /v1/stats` shows the free space as `disk`.

## Queues

### `POST /v1/queues` - create a queue

```sh
curl -d '{"name":"emails","maxAttempts":3}' localhost:7331/v1/queues
```

Body: `name` plus any [settings](#queue-settings). Returns `201` with a [queue](#queue).

Names use `a-z A-Z 0-9 . _ -`, start with a letter or digit and are up to 128 characters. A
name ending in `.dlq` fails with `invalid`: Berthq creates `<queue>.dlq` itself when the first
message is dead-lettered. Its settings can be changed with `PATCH` like any queue's.

### `GET /v1/queues` - list queues

Returns `{ "queues": [queue, …] }`.

### `GET /v1/queues/{queue}` - one queue

A [queue](#queue) plus `consumers` (`[consumer, …]`, see [Consumers](#consumers)) and
`history` (throughput per second for the last minute, `[{ at, published, delivered, acked,
failed }]`).

### `PATCH /v1/queues/{queue}` - change settings

Body: the [settings](#queue-settings) to change. `maxMessages: 0` removes the limit. Returns
the [queue](#queue).

### `DELETE /v1/queues/{queue}` - delete a queue

Deletes the queue, its consumers and all its messages. Its dead-letter queue stays. Returns
`204`.

### `POST /v1/queues/{queue}/purge`

Deletes every message, including ones being processed (their acks are then ignored) and ones
kept only for replay. Consumers keep their names and settings. Returns `{ "purged": 42 }`.

### `POST /v1/queues/{queue}/pause` and `/resume`

A paused queue accepts publishes but delivers nothing to any consumer. Returns the
[queue](#queue).

### Queue settings

| Field                 | Default  | Meaning                                                         |
| --------------------- | -------- | --------------------------------------------------------------- |
| `retentionMs`         | 86400000 | How long messages stay after they become visible (0 - 365 days) |
| `visibilityTimeoutMs` | 30000    | Lease length before an unacked message is retried               |
| `maxAttempts`         | 5        | Deliveries before dead-lettering (0 = retry forever)            |
| `retryDelayMs`        | 1000     | First retry delay, doubled per attempt up to 5 minutes          |
| `maxMessages`         | null     | Backlog limit (messages a consumer still has to process)        |

The rest is fixed: a message is never deleted before every consumer has acked it, exhausted
messages always move to `<queue>.dlq`, publishing to a full queue fails with `queueFull`, and a
`dedupId` is remembered for one hour. When `retryDelayMs` is over 5 minutes, every retry waits
that long.

### Queue

```json
{
	"name": "emails",
	"createdAt": "2026-10-12T09:00:00.000Z",
	"settings": { "retentionMs": 86400000, "maxAttempts": 5, "…": "…", "paused": false },
	"stats": {
		"messages": 131,
		"backlog": 120,
		"delayed": 3,
		"processing": 8,
		"bytes": 52400,
		"consumers": 2,
		"workers": 3,
		"counters": {
			"published": 9000,
			"delivered": 8900,
			"acked": 8870,
			"retried": 12,
			"deadLettered": 2,
			"dropped": 0
		},
		"publishRate": 140,
		"deliverRate": 138,
		"ackRate": 137
	},
	"deadLetterQueue": "emails.dlq",
	"deadLetterOf": null
}
```

- `messages`: everything stored, including messages kept only for replay and delayed ones.
- `backlog`: messages at least one consumer still has to process (including ones being
  processed or retried). Without consumers, every visible message.
- `delayed`: messages waiting for their `deliverAt`.
- `processing`: messages leased right now, across all consumers.
- `workers`: workers seen in the last minute, across all consumers.

`counters` count since the broker started; `dropped` counts messages that ran out of attempts in
a dead-letter queue, which has no dead-letter queue of its own. Rates are messages per second
over the last second.
`GET /v1/queues/{queue}` measures `backlog` when asked; lists, `/v1/events` and `/metrics` use a
value refreshed every second.
`deadLetterQueue` is `null` until the first message is dead-lettered (the DLQ is created then).

## Publishing

### `POST /v1/queues/{queue}/messages` - publish one message

```sh
curl -d '{"body":{"user":123},"priority":5,"delayMs":10000}' \
  localhost:7331/v1/queues/emails/messages
```

| Field       | Meaning                                                                                         |
| ----------- | ----------------------------------------------------------------------------------------------- |
| `body`      | Required. Any JSON value, kept exactly as sent; plain text is a JSON string. Up to 4 MB as sent |
| `priority`  | 0-9, higher is delivered first (default 0)                                                      |
| `delayMs`   | Hide the message for this long, up to 365 days                                                  |
| `deliverAt` | Hide it until this RFC 3339 time, up to 365 days ahead (overrides `delayMs`)                    |
| `dedupId`   | Ignore repeats of this id for one hour (up to 256 characters)                                   |

Returns `201 { "id": "01J…", "duplicate": false }`, or `200` with `duplicate: true` and the
original id when the `dedupId` was seen in the last hour. The response is sent only after the
message is on disk.

### `POST /v1/queues/{queue}/batch` - publish many

Body `{ "messages": [message, …] }` (up to 1000 and 16 MiB). All are stored in one transaction or none
are. Returns `201 { "messages": [{ id, duplicate }, …] }` in the same order.

## Consuming

### `POST /v1/queues/{queue}/consume` - get messages

```sh
curl -d '{"consumer":"billing","max":10,"waitMs":20000,"worker":"billing-1"}' \
  localhost:7331/v1/queues/orders/consume
```

| Field                 | Meaning                                                                                                                 |
| --------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| `consumer`            | Whose position to use (default `default`); created if it doesn't exist                                                  |
| `from`                | Where a new consumer starts: `start` (default), `end`, an RFC 3339 time or a message id. Ignored if the consumer exists |
| `max`                 | Most messages to return, 1-100 (default 1)                                                                              |
| `waitMs`              | Long-poll: wait up to this long for a message (default 0, max 60000)                                                    |
| `visibilityTimeoutMs` | Lease length for these messages (default: the queue's setting)                                                          |
| `autoAck`             | `true`: no lease, the position moves now, no retries (default `false`)                                                  |
| `worker`              | Name shown in the dashboard (default `anonymous`)                                                                       |
| `ack`                 | Leases to ack before getting new messages (up to 1000), instead of a separate `/ack` request                            |

Returns `{ "messages": [delivery, …], "settled": 0, "unknown": [] }`; `messages` may be empty.
`settled` and `unknown` report the `ack` leases, as in [`/ack`](#post-v1queuesqueueack---done).
The acks are saved first, even when the request then waits for new messages. A worker that
acks this way sends one request per batch instead of two:

```sh
curl -d '{"max":10,"waitMs":20000,"ack":["01J….1.k3x9.default"]}' localhost:7331/v1/queues/emails/consume
```

A delivery looks like this:

```json
{
	"id": "01J…",
	"lease": "01J….1.k3x9.billing",
	"queue": "orders",
	"consumer": "billing",
	"body": { "id": 1 },
	"priority": 0,
	"attempt": 1,
	"publishedAt": "2026-10-12T09:00:00.000Z",
	"leaseExpiresAt": "2026-10-12T09:00:30.000Z",
	"visibilityTimeoutMs": 30000,
	"dedupId": null,
	"lastError": null
}
```

The `lease` is an opaque string; with `autoAck` it is `null`, `leaseExpiresAt` is `null` and
`visibilityTimeoutMs` is 0.
`id` is the same on every redelivery and replay, so it works as an idempotency key (see
[guarantees.md](guarantees.md)).

Messages come highest priority first, then in the order they became visible, with the
consumer's due retries before newer messages of the same priority. Each delivery is leased
until `leaseExpiresAt`; settle it with its `lease` before then or it is retried. Long-running
workers call `extend` every `visibilityTimeoutMs / 2`.

### `POST /v1/queues/{queue}/ack` - done

Body `{ "leases": ["…", …] }` (up to 1000). The messages are done for the lease's consumer.
Returns `{ "settled": 1, "unknown": [] }`; `unknown` lists leases that no longer exist (already
settled, expired and redelivered, or dropped by a replay). An unknown lease is not an error.

### `POST /v1/queues/{queue}/nack` - failed

Body `{ "leases": [...], "error": "smtp timeout", "delayMs": 5000, "deadLetter": false }`
(up to 1000 leases). The message is retried for that consumer after `delayMs` (up to 365 days)
or the queue's backoff, or dead-lettered when it is out of attempts or `deadLetter` is true.
Returns `{ settled, unknown }`.

### `POST /v1/queues/{queue}/extend` - more time

Body `{ "leases": [...], "visibilityTimeoutMs": 60000 }` (up to 1000 leases). Restarts the lease
clock (default: the queue's visibility timeout). Returns `{ settled, unknown }`.

## Consumers

A consumer is a name with a saved position in a queue. Every consumer gets every message; its
workers share them. A queue can have up to 100 consumers; creating one more (also by
`consume`) is `invalid`. See [spec.md](spec.md#consumers).

### Consumer

```json
{
	"queue": "orders",
	"name": "billing",
	"createdAt": "2026-10-14T08:00:00.000Z",
	"paused": false,
	"stats": { "waiting": 1240, "processing": 8, "retrying": 2 },
	"idle": false,
	"lastSeen": "2026-10-14T09:30:12.000Z",
	"workers": [{ "name": "billing-1", "lastSeen": "2026-10-14T09:30:12.000Z", "waiting": true }]
}
```

- `stats.waiting`: messages after its position it hasn't received yet. Counted from
  positions, so a message deleted early (`DELETE`) can still be counted until the consumer
  passes its place.
- `stats.processing`: its messages leased right now.
- `stats.retrying`: messages it received before that wait for their next attempt, whether the
  backoff has ended or not.
- `idle`: no worker has consumed for it for at least an hour (counting from `createdAt` if it
  was never seen) while it still holds messages. Such a consumer keeps messages from being
  deleted; remove it if it is not coming back.
- `lastSeen`: the last time a worker consumed for it (`null` if never); `workers`: workers
  seen in the last minute (`waiting` is true while one is long-polling).

### `GET /v1/queues/{queue}/consumers`

Returns `{ "consumers": [consumer, …] }` sorted by name.

### `POST /v1/queues/{queue}/consumers` - create

Body `{ "name": "billing", "from": "end" }` (`from` as in consume, default `start`). Returns
`201` with the consumer, or `409 conflict` if it exists.

### `GET /v1/queues/{queue}/consumers/{consumer}` and `DELETE`

`DELETE` removes the consumer with its position, leases and retries. Messages it was the last
to hold back can then be deleted by retention. Returns `204`.

### `POST /v1/queues/{queue}/consumers/{consumer}/replay` - move the position

Body `{ "from": "start" }`: `start`, `end`, an RFC 3339 time, or a message id (that message and
everything after it; `invalid` if it is no longer kept). Drops what the consumer was
processing and retrying; leases from before become unknown. Requeued dead letters that the
new position doesn't bring back stay, as if they were just requeued. Returns the consumer.

### `POST /v1/queues/{queue}/consumers/{consumer}/pause` and `/resume`

A paused consumer gets nothing; others continue. Returns the consumer.

### `GET /v1/queues/{queue}/consumers/{consumer}/messages?state=processing&limit=50`

What the consumer is doing. `state` is `waiting` (not received yet, in delivery order),
`processing` or `retrying` (every retry: due ones first, then by `retryAt`; default
`processing`); `limit` up to 500 (default 50). Returns
`{ "messages": [consumerMessage, …] }` without bodies:

```json
{
	"id": "01J…",
	"queue": "orders",
	"consumer": "billing",
	"state": "processing",
	"priority": 0,
	"attempts": 2,
	"publishedAt": "…",
	"lastError": "timeout",
	"worker": "billing-1",
	"leaseExpiresAt": "…",
	"retryAt": null,
	"size": 14
}
```

`worker` and `leaseExpiresAt` are set while processing, `retryAt` while retrying.

## Inspecting

### `GET /v1/queues/{queue}/messages?limit=50`

Browses stored messages without consuming them. `state` is `visible` (default, newest first)
or `delayed` (latest `deliverAt` first); `limit` up to 500 (default 50); `before` (the id of
the last message on the previous page) pages on.
Returns `{ "messages": [message, …] }` without bodies:

```json
{
	"id": "01J…",
	"queue": "emails",
	"state": "visible",
	"priority": 0,
	"publishedAt": "…",
	"deliverAt": null,
	"dedupId": null,
	"size": 14,
	"attempts": 0,
	"lastError": null,
	"deadLetter": null,
	"body": null
}
```

In a dead-letter queue, `deadLetter` is `{ queue, consumer, messageId, reason, at }`:
`messageId` is the original message's id (the dead letter has its own `id`), `reason` is
`maxAttempts` or `rejected`, and `attempts` and `lastError` come from the consumer that failed.

### `GET /v1/queues/{queue}/messages/{id}` - one message with its body

### `DELETE /v1/queues/{queue}/messages/{id}` - delete one message

Deletes it for every consumer; leases on it become unknown. Returns `204`.

### `POST /v1/queues/{queue}/requeue` - replay dead letters

Only on dead-letter queues (`invalid` otherwise). Body `{ "ids": [...] }` (up to 1000) or `{}`
for all of them. Moves each message back to the queue and consumer that failed it, with attempts reset;
other consumers don't get it again. The consumer receives it with its original id
(`deadLetter.messageId`), so duplicate checks keep working. When the consumer's position will
bring the message back anyway (after a replay), the dead letter is only removed, so the
message comes once. Returns
`{ "requeued": 3, "skipped": 0 }` (`skipped`: the queue or consumer no longer exists, or the
consumer already has that message pending).

## Backups

### `GET /v1/backup`

Needs `admin`. Streams a consistent copy of the database (`application/octet-stream`), made
inside one read transaction while the broker keeps running. `berthq backup <file>` saves it.
To restore, stop Berthq and put the file in the data directory as `berthq.redb`.

The copy is written to the data directory first. It is deleted once it is sent, or right after
it is written if the client has gone.

- One backup runs at a time. While one is being copied, another request gets `409 conflict`.
- The disk needs room for the copy: with less free space than the size of the data file plus
  `--min-free-disk`, the request gets `507 diskFull` (`--min-free-disk 0` turns the check off).

## Monitoring

- `GET /v1/stats`: broker-wide totals, rates and the last minute of throughput (`version`,
  `startedAt`, `queues`, `messages`, `backlog`, `processing`, `failed` (messages in DLQs),
  `consumers`, `workers`, `publishRate`, `deliverRate`, `ackRate`, `counters`, `history`), and
  `disk`: `{ "free": 51234567890, "minFree": 1073741824, "full": false }` (bytes; `full` is true
  while publishes are refused, and `minFree` is 0 when the check is off).
- `GET /v1/events`: server-sent events. A `stats` event right away and then every second, with
  `{ "overview": stats, "queues": [queue, …] }`.
- `GET /metrics`: Prometheus metrics: `berthq_queue_messages`, `berthq_queue_backlog`,
  `berthq_queue_delayed`, `berthq_queue_processing`, `berthq_messages_published_total`, …
  labelled by `queue`, `berthq_consumer_waiting`, `berthq_consumer_processing`,
  `berthq_consumer_retrying` labelled by `queue` and `consumer`, and `berthq_disk_free_bytes`
  and `berthq_disk_full`.
- `GET /healthz`: `ok`, or `503` with the reason once storage has failed (see `storage` above).
  Restart Berthq then; the Kubernetes manifest's liveness probe does it on its own.
