# berthq-client (PHP)

PHP client and Laravel queue driver for Berthq, the single-binary message broker. PHP 8.2+,
no dependencies besides `ext-curl` and `ext-json`.

```sh
composer require talovicnedim/berthq-client
```

The package is not on Packagist yet. Until it is, install it from this repository: add
`{"type": "path", "url": "path/to/berthq/sdks/php"}` to the `repositories` of your
`composer.json`, then run `composer require talovicnedim/berthq-client:@dev`.

## Quick start (plain PHP)

```php
use Berthq\Client;
use Berthq\Delivery;

$client = new Client('http://127.0.0.1:7331'); // apiKey: '...', timeout: 30

$client->queuesCreate('emails', ['maxAttempts' => 3]);
$client->publish('emails', ['user' => 123], priority: 5, delayMs: 10_000);

// A worker: returning acks the message, throwing nacks it with the exception's message
// (retried with backoff, then dead-lettered to emails.dlq).
$stop = false;
pcntl_async_signals(true);
pcntl_signal(SIGTERM, function () use (&$stop) { $stop = true; });

$client->process('emails', function (Delivery $message) {
    sendEmail($message->body['user']);
}, batch: 10, shouldStop: function () use (&$stop) { return $stop; });
```

`process` long-polls (20 s by default), finishes the batch it leased before stopping, and waits
and retries (1 s up to 30 s) while the broker is shutting down or unreachable. Its acks travel
with the next consume call (one request per batch instead of two); an ack that has waited half
the lease is sent on its own, and the rest when the loop ends, also when it ends with an
exception. An ack or nack the broker can't take right now goes to `onError` and the loop goes
on: that message's lease runs out and it comes again.

`process` does not extend leases while a handler runs, and it handles a batch one message after
another, so the whole batch must fit in one lease: give the queue (or
`process(visibilityTimeoutMs: ...)`) a lease longer than `batch` times your slowest handler, or
call `$client->extend()` from the handler. A message whose lease ran out while it waited for its
turn is skipped and reported to `onError`, because the broker has already given it to another
worker.

Delivery is at-least-once, so handlers should be idempotent: `$delivery->id` is the same on every
redelivery and replay and works as an idempotency key (see
[guarantees.md](https://github.com/talovicnedim/berthq/blob/main/docs/guarantees.md)). With `autoAck: true` nothing is leased or retried:
the position moves on delivery, which is faster, but a crash or a throwing handler loses the
message for the consumer (handler errors go to `onError` too, by default `error_log()`).

Without `process`:

```php
foreach ($client->consume('emails', max: 10, waitMs: 20_000) as $delivery) {
    try {
        handle($delivery->body);
        $client->ack('emails', $delivery);
    } catch (Throwable $e) {
        $client->nack('emails', $delivery, error: $e->getMessage(), delayMs: 5_000);
    }
}
```

`ack`, `nack` and `extend` take a `Delivery`, a lease string, or a list of either. To save a
request, pass the last batch to the next consume instead of calling `ack`:
`$client->consume('emails', max: 10, waitMs: 20_000, ack: $done)`.

## Speed

Publishing many messages? Use a publisher: it collects them and sends batches of up to 1000
(`maxBatch`) or 4 MiB, many times faster than one request per message.

```php
$publisher = $client->publisher('emails');
foreach ($users as $user) {
    $publisher->publish(['user' => $user->id], dedupId: "welcome-{$user->id}");
}
$results = $publisher->flush(); // a Published per message, in order
```

PHP has no background work, so a batch goes out when it is full, on `flush()`, and when the
publisher is destroyed. If a batch is rejected as `invalid`, it is split in half until the bad
message is alone, so only it fails: its entry in `flush()`'s results is the `BerthqException`.
Other errors (the broker can't be reached) are thrown, and the messages wait for the next
`flush()`.

One `Client` keeps its connection open between requests, and uses HTTP/2 over HTTPS when the
broker offers it.

## Consumers and replay

A consumer is a name with a saved position in a queue. Every consumer gets every message, at
its own pace; calls without a consumer use `default`. A consumer is created by its first
`consume`, at the oldest kept message (or at `from`), and continues where it stopped after that.

```php
// A second service gets every order too, starting with new ones only.
$client->process('orders', $bill, consumer: 'billing', from: 'end', shouldStop: $stop);

// Billing had a bug since 9:00: process everything from then again.
$client->consumersReplay('orders', 'billing', '2026-10-14T09:00:00Z'); // or 'start', 'end', a message id

$client->consumersList('orders');
$client->consumersMessages('orders', 'billing', 'retrying'); // waiting, processing, retrying
```

## API

Method names match the [HTTP API](https://github.com/talovicnedim/berthq/blob/main/docs/api.md) and the other SDKs:

| Area       | Methods                                                                                                                                            |
| ---------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| Queues     | `queuesList`, `queuesCreate`, `queuesGet`, `queuesUpdate`, `queuesRemove`, `queuesPurge`, `queuesPause`, `queuesResume`                            |
| Publishing | `publish`, `publishBatch`, `publisher` (`publish`, `flush`)                                                                                        |
| Consuming  | `consume`, `ack`, `nack`, `extend`, `process`                                                                                                      |
| Consumers  | `consumersList`, `consumersGet`, `consumersCreate`, `consumersRemove`, `consumersReplay`, `consumersPause`, `consumersResume`, `consumersMessages` |
| Inspecting | `messagesList`, `messagesGet`, `messagesRemove`, `requeue`                                                                                         |
| Monitoring | `stats`                                                                                                                                            |

Responses are readonly objects: `Delivery`, `Published`, `Settled`, `Requeued`, `QueueInfo`,
`QueueDetails` (from `queuesGet`, with `consumers` and `history`), `ConsumerInfo`,
`ConsumerMessage`, `MessageInfo` and `Overview` (from `stats`). Queue settings use the
API's field names (`['maxAttempts' => 3, 'retentionMs' => 3_600_000]`). Message bodies are any
JSON value and come back decoded with objects as associative arrays; note that the broker
returns object keys sorted.

Publishing options are named arguments, or build a `Berthq\NewMessage` for batches:

```php
use Berthq\NewMessage;

$client->publishBatch('emails', [
    ['user' => 1],                                      // a plain body
    new NewMessage(['user' => 2], dedupId: 'welcome-2'),
]);

$client->messagesList('emails', limit: 50);             // newest first, without bodies
$client->requeue('emails.dlq');                         // dead letters back to the consumer that failed them
```

## Errors

Every failure throws `Berthq\BerthqException` with:

- `kind`: the broker's error kind, `invalid`, `unauthorized`, `forbidden`, `notFound`,
  `conflict`, `queueFull`, `diskFull` (the broker takes no new messages while its disk is almost
  full), `unavailable` or `storage`, or `http` when the broker could not be reached or answered
  something unexpected.
- `status`: the HTTP status, or `0` for transport errors (`isTransport()`).

```php
try {
    $client->queuesCreate('emails');
} catch (BerthqException $e) {
    if ($e->kind !== 'conflict') {
        throw $e;
    }
}
```

An `ack` or `nack` of a lease that no longer exists (already settled, or expired and
redelivered) is not an error: it is listed in `Settled::$unknown`.

## Laravel

Laravel 11 and 12. The service provider is auto-discovered and registers the `berthq` queue
driver. Add a connection to `config/queue.php`:

```php
'connections' => [
    'berthq' => [
        'driver' => 'berthq',
        'url' => env('BERTHQ_URL', 'http://127.0.0.1:7331'),
        'key' => env('BERTHQ_API_KEY'),
        'queue' => env('BERTHQ_QUEUE', 'default'),
        'wait' => 3,
    ],
],
```

Set `QUEUE_CONNECTION=berthq`, create the queue and start a worker:

```sh
berthq queue create default --max-attempts 0
php artisan queue:work berthq --sleep=0
```

Jobs, `dispatch()`, `->delay()`, `$this->release()`, `$tries`, `$backoff`,
`failed()` and `php artisan queue:clear berthq` work as with any other driver. Payloads are stored
as JSON objects, so the dashboard shows each job's class and data.

| Option          | Default       | Meaning                                                                            |
| --------------- | ------------- | ---------------------------------------------------------------------------------- |
| `url`, `key`    |               | The broker and its API key (see below)                                             |
| `queue`         | `default`     | Queue used when a job doesn't name one                                             |
| `wait`          | `3`           | Seconds a worker long-polls for a job before Laravel's loop runs again             |
| `retry_after`   | queue setting | Lease length in seconds. Keep it above the worker's `--timeout`, or jobs run twice |
| `timeout`       | `30`          | HTTP timeout in seconds (long-polls get `wait` on top)                             |
| `consumer`      | `default`     | The consumer all workers of this connection share (see below)                      |
| `worker`        | `host:pid`    | Worker name shown in the dashboard                                                 |
| `dead_letter`   | `true`        | Keep failed jobs in `<queue>.dlq` (`true`) or delete them (`false`)                |
| `create_queues` | `false`       | Create a missing berthq queue on first push or pop instead of throwing `notFound`  |
| `after_commit`  | `false`       | Dispatch jobs after the open database transaction commits                          |

**API keys.** A key can hold several scopes, joined with `+`. A typical app that dispatches and
works jobs uses `berthq start --api-key publish+consume+read:KEY`; a process that only
dispatches can use a `publish` key, a worker that only runs jobs a `consume` key. `size()` and
`queue:monitor` need `read`; `queue:clear` and `create_queues` need an admin key.

**Consumers.** All workers of a connection share one consumer (`default`), so each job runs once.
A second app that wants every job of the same queue too (an audit log, a search index) uses a
connection with its own `consumer`, for example `'consumer' => 'audit'`. A queue keeps acked jobs
for its retention (24 hours by default), so `berthq consumer replay default default --from
<time>` (or the dashboard) runs them again after a bug fix. `size()`, `pendingSize()` and the
other counts are the connection consumer's jobs; jobs kept only for replay don't count, and
released jobs waiting for their backoff count as delayed.

**Long-polling.** `pop` waits up to `wait` seconds for a job, so new jobs start right away and
the worker needs no `--sleep` between polls (hence `--sleep=0`). A worker that serves several
queues (`--queue=high,default`) long-polls each one in turn; use `'wait' => 0` or `1` there, so
an empty `high` queue doesn't hold up `default`.

### Retries: let Laravel decide

Both Laravel and the broker can retry, and they must not fight. The recommended setup is that
**Laravel owns retries and the broker queue retries forever (`maxAttempts` 0)**:

- A job that throws is released by Laravel's worker after its `$backoff`; the driver nacks
  it with exactly that delay, so the broker's own backoff is not used.
- `attempts()` is the broker's delivery count, so `$tries`, `$maxExceptions` and
  `retryUntil()` see every delivery, including ones whose worker crashed or timed out.
- When Laravel gives up, the job is recorded in `failed_jobs` as usual and dead-lettered to
  `<queue>.dlq` with the exception as its error (`dead_letter => false` deletes it instead).
  `berthq queue requeue default.dlq` (or the dashboard) replays dead letters with attempts
  reset; `php artisan queue:retry` works from `failed_jobs` as always.

With the broker's default `maxAttempts` of 5, the broker would dead-letter a job on its fifth
delivery even if the job has `$tries = 10`, without Laravel running `failed()`. Hence
`--max-attempts 0` when creating the queue.

### Speed

`Queue::bulk()` (and `Bus::batch()`, which uses it) publishes in batches of up to 1000 with one
request each. The worker acks every job right away instead of with its next poll: Laravel can
stop a worker between jobs (`--max-jobs`, `--max-time`, a deploy), and a buffered ack would then
be lost and the job run again.

### Creating queues

Berthq queues are created explicitly, like tables: `berthq queue create default
--max-attempts 0` (or `Client::queuesCreate()` in a migration or deploy script). Pushing to a
missing queue throws a `BerthqException` with kind `notFound`, so a typo in a queue name fails
loudly instead of silently creating a queue no worker listens to.

If you prefer queues to appear on first use, set `'create_queues' => true`: a push or pop that
gets `notFound` creates the queue with `maxAttempts` 0 and tries once more.

### Using the client in a Laravel app

`Berthq\Client` is bound in the container with the `berthq` connection's `url` and `key`, for
consumers, inspection and admin calls:

```php
app(Berthq\Client::class)->consumersReplay('default', 'default', '2026-10-14T09:00:00Z');
```

Messages a Laravel worker consumes must be Laravel job payloads; publish them with `dispatch()`.
Use separate queues for messages from other producers and consume those with the client.

## Development

The tests run against a real berthq binary (`target/debug/berthq`, or set `BERTHQ_BINARY`),
started on free ports with temporary data directories:

```sh
cargo build -p berthq-cli   # in the repository root
composer install
vendor/bin/phpunit
```
