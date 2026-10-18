# Delivery guarantees

What Berthq promises, what it can't promise, and how to make sure work is never done twice:
for a bank transfer, an email or anything in between.

## What Berthq promises

- **A confirmed publish is on disk.** `publish` answers only after the message is fsynced. A
  crash or power cut after that loses nothing.
- **Nothing is lost before it is done.** A message is done for a consumer only when a worker
  acks it. Crashes, restarts, timeouts and lost connections all lead to the message being
  delivered again (at-least-once).
- **One worker at a time.** While a lease is valid, no other worker of the same consumer gets
  that message.
- **Done stays done.** After an ack, that consumer never gets the message again, unless you
  replay it on purpose.
- **Stable ids.** A message keeps its `id` on every redelivery and replay, and when a dead
  letter is requeued. `attempt` tells a worker how many times its consumer has received it.

With `autoAck: true` the promise is the other way around: a message is never delivered twice to
that consumer, but a worker crash can lose it (at-most-once). A replay can bring it back while
the queue still keeps it.

## What no queue can promise

No broker can promise "exactly once" on its own. Look at what a worker does:

```
worker:  get message  →  do the work  →  ack
                                     ↑
                 crash here: the work happened, but Berthq never heard "done"
```

After that crash Berthq can't know whether the work happened, so it delivers the message
again. The same happens when a worker is slower than its lease, or when the network drops the
ack. Only the place where the work happened can tell whether it already happened, so that is
where "never twice" is solved: in your database, or with the service you call.

Kafka's "exactly once" has the same limit: it only covers results written back into Kafka.

## Recipe 1: work in your database

When the work is a change in your own database (a ledger entry, an order status), save the
message id in the same transaction, in a table with a unique key:

```sql
CREATE TABLE processed_messages (
    id         text PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT now()
);
```

```php
public function handle(): void
{
    DB::transaction(function () {
        $first = DB::table('processed_messages')->insertOrIgnore(['id' => $this->job->getJobId()]);
        if ($first === 0) {
            return; // done before: the job is acked and nothing happens twice
        }
        // the real work, in the same transaction
    });
}
```

If the worker crashes after the commit, the redelivery finds the id and skips. If it crashes
before the commit, nothing was saved and the redelivery does the work. Either way the work
happens exactly once.

**Use a business key when you have one.** The message id only catches redeliveries of the same
message. If a producer publishes the same payment twice by mistake, the two messages have
different ids, but the same `payment_id`. Save the business key instead, and publish with
`dedupId` set to it too (see recipe 3).

Delete old rows once they can't come back: older than the queue's retention plus the longest
time a message can wait for a retry.

## Recipe 2: emails and other outside services

An email, an SMS or a call to another company's API can't be part of your database
transaction. Claim the work first, then do it, then mark it done:

```sql
CREATE TABLE sent_emails (
    key        text PRIMARY KEY,           -- for example 'order-42-shipped'
    status     text NOT NULL,              -- 'sending' or 'sent'
    updated_at timestamptz NOT NULL DEFAULT now()
);
```

```php
public function handle(): void
{
    $key = "order-{$this->order->id}-shipped";

    $claimed = DB::table('sent_emails')->insertOrIgnore(['key' => $key, 'status' => 'sending']);
    if ($claimed === 0) {
        return; // sent before, or another worker is sending it right now
    }

    Mail::to($this->order->customer)->send(new OrderShipped($this->order));

    DB::table('sent_emails')->where('key', $key)->update(['status' => 'sent', 'updated_at' => now()]);
}
```

- A crash after sending: the redelivery finds the row and skips, so there is no second email.
- A slow send whose lease runs out: a second worker starts, but its insert fails, so it skips.
- **The one open case**: a crash in the middle of the send leaves the row at `sending`, and
  nobody can know whether the email went out. Decide per kind of email: a job that finds old
  `sending` rows can resend password resets, while an invoice may be better checked by hand.
- **If the service accepts an idempotency key, pass one** (for example Stripe's
  `Idempotency-Key` header, with the message id or business key). The service then drops the
  second request itself, and even the open case is covered.

Rule of thumb: for emails a rare duplicate is usually better than a lost email. For money,
never guess: use an idempotency key, or check with the service before you try again.

## Recipe 3: publishing without losing messages

A producer can fail too. If your app saves an order and then publishes `order created`, a crash
between the two loses the message. Publishing first and saving second is worse: a message for
an order that doesn't exist.

The fix is an **outbox**: save the message in your database in the same transaction as the
change, and publish it from there.

```sql
CREATE TABLE outbox (
    id         bigserial PRIMARY KEY,
    queue      text NOT NULL,
    body       jsonb NOT NULL,
    sent_at    timestamptz
);
```

1. In the transaction that saves the order, insert a row into `outbox`.
2. A small loop reads unsent rows, publishes each with `dedupId` set to the row id, and sets
   `sent_at`.
3. If the loop crashes after publishing but before `sent_at`, it publishes the row again, and
   Berthq drops the duplicate (a `dedupId` is remembered for one hour).

If the loop can be down for longer than an hour, keep recipe 1 on the consumer side as well:
the two together cover every case.

## Long jobs

A lease lasts the queue's `visibilityTimeoutMs` (30 seconds by default). A job that takes longer
looks like a crash, and another worker gets the message.

- Set `visibilityTimeoutMs` longer than your slowest job, per queue or per consume call, or
- call `extend` every `visibilityTimeoutMs / 2` while the job runs.

The claim from recipe 1 or 2 still protects you if a lease runs out anyway.

## Acks sent with the next consume

Workers can send their acks inside the next `consume` call (the SDK worker loops and the CLI do
this) to save a request per batch. An ack then waits until the worker asks for more, so:

- the lease must cover the work **and** the time until the next consume call, which is right
  after the batch in a normal loop;
- a worker that stops sends its remaining acks on its own; one that crashes loses them, and
  those messages are delivered again, which your duplicate check (recipe 1 or 2) already
  handles.

## Replay and duplicate checks

A replay sends messages to a consumer again **on purpose**. Your duplicate check decides what
happens:

- After an outage, or after dead letters were fixed, replay is safe: the check skips what was
  already done, so only the missing work runs.
- After a bug that did the work wrong, the check would skip the messages you want to redo.
  Delete the affected rows from `processed_messages` first, or replay to a new consumer whose
  code repairs the results.

## Summary

| You need                           | Berthq does                          | You add                                   |
| ---------------------------------- | ------------------------------------ | ----------------------------------------- |
| Never lose a message               | Disk before confirm, ack before done | An outbox when publishing from a database |
| Never do database work twice       | Stable ids, leases                   | A unique key in the same transaction      |
| Never send an email twice          | Stable ids, leases                   | Claim, send, mark sent; idempotency keys  |
| Never publish the same thing twice | `dedupId` within one hour            | The same business key as `dedupId`        |
| Speed over safety for one consumer | `autoAck`                            | Replay when something was lost            |
