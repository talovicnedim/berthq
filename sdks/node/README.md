# @berthq/client

Node.js and TypeScript client for Berthq, a message broker in one binary. No dependencies: it
uses the global `fetch` of Node 18+ (and works in Deno, Bun and browsers too).

The client is a thin layer over the [HTTP API](https://github.com/talovicnedim/berthq/blob/main/docs/api.md). Method names match the API and
the other SDKs: `queues.create`, `publish`, `consume`, `ack`, `nack`, `consumers.replay`, ...

## Install

```sh
npm install @berthq/client
```

## Quick start

Start a broker (`berthq start`), then publish:

```ts
import { Client } from '@berthq/client';

const client = new Client(); // http://127.0.0.1:7331
await client.queues.create('emails', { maxAttempts: 3 });
await client.publish('emails', { body: { user: 123 } });
```

And process messages with a worker:

```ts
import { Client } from '@berthq/client';

const client = new Client('http://127.0.0.1:7331', { apiKey: process.env.BERTHQ_API_KEY });
const controller = new AbortController();
process.on('SIGTERM', () => controller.abort());

await client.process<{ user: number }>(
	'emails',
	async (message) => {
		await sendEmail(message.body.user);
	},
	{ concurrency: 10, signal: controller.signal, worker: 'mailer-1' }
);
```

`process` long-polls the queue and runs the handler for each message:

- The handler returns: the message is acked (done for this consumer). The ack goes out with the
  next consume call, so a busy worker needs one request per batch; the lease stays extended
  until the broker has it, and the last acks are sent on their own when the worker stops.
- The handler throws: the message is nacked with the error's message, and retried after the
  queue's backoff or dead-lettered when it is out of attempts.
- The handler runs longer than half the lease: the lease is extended, so a slow handler doesn't
  get its message delivered to another worker.
- The broker is shutting down or can't be reached: the worker waits a second and asks again.
- The signal aborts: no new messages are leased, in-flight handlers finish and are settled, then
  the promise resolves.

It rejects on errors retrying can't fix, like an unknown queue or a missing api key.

| Option                | Default         | Meaning                                                                             |
| --------------------- | --------------- | ----------------------------------------------------------------------------------- |
| `concurrency`         | 1               | Handlers running at once                                                            |
| `batch`               | `concurrency`   | Most messages leased per request                                                    |
| `signal`              |                 | `AbortSignal` that stops the worker                                                 |
| `consumer`            | `default`       | Whose position to use; workers of one consumer share its messages                   |
| `from`                | `start`         | Where the consumer starts if it doesn't exist yet                                   |
| `worker`              | `anonymous`     | Name shown in the dashboard                                                         |
| `visibilityTimeoutMs` | queue's setting | Lease length; bounds how long a crashed worker holds a message                      |
| `autoAck`             | `false`         | No leases or retries: faster, but a crash loses the message                         |
| `onError`             | `console.error` | Called when an ack, nack or extend fails, and with handler errors in `autoAck` mode |

Delivery is at-least-once: a message can be handled twice (for example when a worker crashes
before acking), so handlers should be idempotent. `message.id` is the same on every
redelivery and replay, so it works as an idempotency key; see
[guarantees.md](https://github.com/talovicnedim/berthq/blob/main/docs/guarantees.md).

## Consumers and replay

A consumer is a name with a saved position in a queue. Every consumer gets every message, at
its own pace; calls without a consumer use `default`. A consumer is created by its first
`consume`, at the oldest kept message (or at `from`), and continues where it stopped after that.

```ts
// A second service gets every order too, and starts with new ones only.
await client.process('orders', handleBilling, { consumer: 'billing', from: 'end' });

// Billing had a bug since 9:00: process everything from then again.
await client.consumers.replay('orders', 'billing', '2026-10-14T09:00:00Z'); // or 'start', 'end', a message id
```

```ts
await client.consumers.list('orders'); // ConsumerInfo[]: stats, workers, last seen
await client.consumers.get('orders', 'billing');
await client.consumers.create('orders', 'audit', 'start'); // conflict if it exists
await client.consumers.pause('orders', 'billing');
await client.consumers.resume('orders', 'billing');
await client.consumers.messages('orders', 'billing', { state: 'retrying' }); // waiting, processing, retrying
await client.consumers.remove('orders', 'audit');
```

## API

```ts
const client = new Client(url?, { apiKey?, fetch? });
```

`apiKey` is sent as `Authorization: Bearer <key>`. `fetch` replaces the global `fetch`.

### Queues

Queue settings: `retentionMs` (how long messages stay for replay, default 1 day),
`visibilityTimeoutMs` (lease length, default 30 s), `maxAttempts` (default 5, 0 retries forever),
`retryDelayMs` (first retry delay, doubled per attempt up to 5 minutes, default 1 s) and
`maxMessages` (backlog limit, none by default; 0 removes it). A message is never deleted before
every consumer has acked it, and one that runs out of attempts moves to `<queue>.dlq`.

```ts
await client.queues.list(); // QueueInfo[]
await client.queues.get('emails'); // QueueDetails: settings, stats, consumers, history
await client.queues.create('emails', { visibilityTimeoutMs: 60_000, retentionMs: 86_400_000 });
await client.queues.update('emails', { maxMessages: 10_000, maxAttempts: 10 });
await client.queues.remove('emails');
await client.queues.purge('emails'); // number of deleted messages
await client.queues.pause('emails');
await client.queues.resume('emails');
```

### Publishing

```ts
await client.publish('emails', {
	body: { user: 123 },
	priority: 5, // 0-9, higher first
	delayMs: 10_000, // or deliverAt: '2026-10-12T09:00:00Z'
	dedupId: 'welcome-123' // repeats within an hour are ignored
}); // { id, duplicate }

await client.publishBatch('emails', [{ body: 1 }, { body: 2 }]); // up to 1000, atomically
```

For many messages, use a publisher. It sends them in batches of up to 1000 (or 4 MiB): a message
goes out right away, and the messages that arrive meanwhile form the next batch (`lingerMs` can
make a batch wait for more). One batch is in flight at a time, so messages keep their order, and
each `publish` resolves once its batch is on disk. If a batch is rejected as `invalid`, it is
split in half until the bad message is alone, so only it fails.

```ts
const publisher = client.publisher('emails', {
	maxBatch: 100,
	lingerMs: 5,
	maxBytes: 4 * 1024 * 1024
});
await Promise.all(users.map((user) => publisher.publish({ body: { user: user.id } })));
await publisher.flush(); // send what is waiting now
await publisher.close(); // flush, then refuse new messages
```

Await (or `.catch`) every `publish` promise: a failed message rejects its own promise.

### Consuming by hand

```ts
const { messages } = await client.consume('emails', {
	max: 10,
	waitMs: 20_000,
	worker: 'mailer-1'
});
const done = messages.map((message) => message.lease!);
await client.consume('emails', { max: 10, waitMs: 20_000, ack: done }); // ack and get the next ones
await client.ack('emails', done); // or ack on their own
await client.nack('emails', { leases: [lease], error: 'smtp timeout', delayMs: 5_000 });
await client.nack('emails', { leases: [lease], deadLetter: true }); // skip remaining attempts
await client.extend('emails', [lease], 60_000); // restart the lease clock

await client.consume('metrics', { consumer: 'dashboard', max: 100, autoAck: true }); // lease is null
```

`consume` takes `consumer`, `from`, `max`, `waitMs`, `visibilityTimeoutMs`, `autoAck`, `worker`
and `ack` (leases to ack first, up to 1000), and an `AbortSignal` as its third argument to
cancel a long-poll. It returns `{ messages, settled, unknown }`, where `settled` and `unknown`
report the `ack` leases. `ack`, `nack` and `extend` return `{ settled, unknown }`; a lease that
no longer exists is listed in `unknown`, not thrown.

### Messages and dead letters

```ts
await client.messages.list('emails', { limit: 50 }); // newest first, without bodies
await client.messages.list('emails', { state: 'delayed' });
await client.messages.list('emails', { before: id }); // older ones
await client.messages.get('emails', id); // with body
await client.messages.remove('emails', id); // for every consumer

await client.requeue('emails.dlq'); // every dead letter back to the consumer that failed it
await client.requeue('emails.dlq', [id]); // or some of them
```

### Monitoring

```ts
await client.stats(); // totals, rates, free disk space and the last minute of throughput
```

## Speed

- Each publish waits until its message is on disk, so one request per message is the slow way.
  A `publisher` batches them and is many times faster.
- `process` acks with its next consume call, so a worker needs one request per batch, not two.
- Connections stay open between requests (Node's `fetch` keeps them alive). HTTP/2 would need the
  `undici` package, so this client stays on HTTP/1.1 and has no dependencies.

## Errors

Every failed call rejects with a `BerthqError`:

```ts
import { BerthqError } from '@berthq/client';

try {
	await client.queues.create('emails');
} catch (err) {
	if (err instanceof BerthqError && err.kind === 'conflict') {
		// the queue already exists
	} else {
		throw err;
	}
}
```

| `kind`         | `status` | When                                                                 |
| -------------- | -------- | -------------------------------------------------------------------- |
| `invalid`      | 400      | Bad input: a name, priority, `from`, unknown field                   |
| `unauthorized` | 401      | Missing or unknown api key                                           |
| `forbidden`    | 403      | The api key's scope doesn't allow this                               |
| `notFound`     | 404      | The queue, consumer or message doesn't exist                         |
| `conflict`     | 409      | Creating a queue or consumer that already exists                     |
| `queueFull`    | 507      | Publishing to a queue whose backlog hit `maxMessages`                |
| `diskFull`     | 507      | Publishing while the broker's disk is below `--min-free-disk` (1 GB) |
| `unavailable`  | 503      | The broker is shutting down                                          |
| `storage`      | 500      | Writing to disk failed                                               |
| `http`         | 0        | The broker could not be reached or did not answer in time            |

## Development

The tests run against a real broker built from this repository:

```sh
cargo build -p berthq-cli   # from the repository root
npm install
npm run build
npm test                    # set BERTHQ_BINARY to use another binary
```
