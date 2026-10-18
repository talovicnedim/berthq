# berthq-client (Python)

Python client for Berthq, the single-binary message broker. Python 3.9+, standard library only.

```sh
pip install berthq-client
```

Or the latest code from GitHub:
`pip install "git+https://github.com/talovicnedim/berthq#subdirectory=sdks/python"`.

## Quick start

```python
import signal
import threading

from berthq_client import Client

client = Client("http://127.0.0.1:7331")  # api_key="..." when the broker requires one

client.queues_create("emails", max_attempts=3)
client.publish("emails", {"user": 123}, priority=5, delay_ms=1000, dedup_id="welcome-123")


def send_email(delivery):
    print("sending email to", delivery.body["user"])
    # Return to ack. Raise to nack: the message is retried with backoff, then dead-lettered.


# Runs until Ctrl-C, then finishes the messages it already received.
stop = threading.Event()
signal.signal(signal.SIGINT, lambda *_: stop.set())
client.process("emails", send_email, batch=10, worker="mailer-1", stop_event=stop)
```

`process()` long-polls the queue, calls the handler for each message one at a time, acks on
return and nacks with `str(exc)` when it raises. Acks go out with the next consume call, so a
busy worker needs one request per batch; the last ones are sent on their own when it stops. A
background thread extends the lease of a message until it is settled, so slow jobs are not
redelivered while they still run; `visibility_timeout_ms` sets the lease length (default: the
queue's setting), which bounds how long a crashed worker holds a message. When the broker is unavailable or unreachable it waits a
second and tries again. Run several workers (threads or processes) to handle messages in
parallel.

Delivery is at-least-once, so handlers should be idempotent: `delivery.id` is the same on every
redelivery and replay and works as an idempotency key (see
[guarantees.md](https://github.com/talovicnedim/berthq/blob/main/docs/guarantees.md)). With `auto_ack=True` nothing is leased or retried:
the position moves on delivery, which is faster, but a crash loses those messages for the
consumer.

## Consumers and replay

A consumer is a name with a saved position in a queue. Every consumer gets every message, at
its own pace; calls without a consumer use `default`. A consumer is created by its first
`consume`, at the oldest kept message (or at `from_`; `from` is a Python keyword), and continues
where it stopped after that.

```python
# A second service gets every order too, starting with new ones only.
client.process("orders", bill, consumer="billing", from_="end", stop_event=stop)

# Billing had a bug since 9:00: process everything from then again.
client.consumers_replay("orders", "billing", "2026-10-14T09:00:00Z")  # or "start", "end", a message id

client.consumers_list("orders")
client.consumers_messages("orders", "billing", state="retrying")  # waiting, processing, retrying
```

## Using the API directly

Method names match the HTTP API ([docs/api.md](https://github.com/talovicnedim/berthq/blob/main/docs/api.md)) and the other SDKs. Requests
take snake_case keywords; responses are dataclasses with snake_case fields.

```python
from berthq_client import NewMessage

client.publish_batch("emails", [{"user": 1}, NewMessage({"user": 2}, priority=9)])

done = []
while True:
    # Acks the finished leases and gets the next messages in one request.
    consumed = client.consume("emails", max=10, wait_ms=20_000, ack=done)
    done = []
    for delivery in consumed.messages:
        try:
            handle(delivery)
        except Exception as exc:
            client.nack("emails", delivery.lease, error=str(exc))
        else:
            done.append(delivery.lease)

# Browse messages, newest first (bodies with messages_get)
client.messages_list("emails", limit=50)
client.messages_list("emails", state="delayed")

# Dead letters go back to the consumer that failed them
dead = client.messages_list("emails.dlq")
client.requeue("emails.dlq")  # or requeue("emails.dlq", ids=[...])
```

| Area       | Methods                                                                                                                                                    |
| ---------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Queues     | `queues_list`, `queues_create`, `queues_get`, `queues_update`, `queues_remove`, `queues_purge`, `queues_pause`, `queues_resume`                            |
| Publishing | `publish`, `publish_batch`, `publisher`                                                                                                                    |
| Consuming  | `consume`, `ack`, `nack`, `extend`, `process`                                                                                                              |
| Consumers  | `consumers_list`, `consumers_get`, `consumers_create`, `consumers_remove`, `consumers_replay`, `consumers_pause`, `consumers_resume`, `consumers_messages` |
| Inspecting | `messages_list`, `messages_get`, `messages_remove`, `requeue`                                                                                              |
| Monitoring | `stats`                                                                                                                                                    |

`consume` returns `Consumed(messages, settled, unknown)`; `settled` and `unknown` report the
`ack` leases (up to 1000).

`queues_create` and `queues_update` take the queue settings as keywords: `retention_ms` (how
long messages stay for replay, default 1 day), `visibility_timeout_ms` (lease length, default
30 s), `max_attempts` (default 5, 0 retries forever), `retry_delay_ms` (first retry delay,
doubled per attempt up to 5 minutes, default 1 s) and `max_messages` (backlog limit, none by
default; 0 removes it). A message is never deleted before every consumer has acked it, and one
that runs out of attempts moves to `<queue>.dlq`. A `dedup_id` seen in the last hour is not
stored again.

## Publisher

For many messages, use a publisher. It sends them in batches of up to 1000 (or 4 MiB): a message
goes out right away, and the messages that arrive meanwhile form the next batch (`linger_ms` can
make a batch wait for more). A background thread sends one batch at a time, so messages keep
their order. `publish` returns a `concurrent.futures.Future` with the message's result once its
batch is on disk. If a batch is rejected as `invalid`, it is split in half until the bad message
is alone, so only it fails.

```python
with client.publisher("emails", max_batch=100, linger_ms=5, max_bytes=4 * 1024 * 1024) as publisher:
    futures = [publisher.publish({"user": user_id}, dedup_id=f"welcome-{user_id}") for user_id in user_ids]
    publisher.flush()  # send what is waiting now
for future in futures:
    future.result()  # the Published, or raises the message's BerthqError
```

Leaving the `with` block (or calling `close()`) sends the rest. An open publisher is closed at
interpreter exit, so its last messages aren't lost.

## Speed

- Each publish waits until its message is on disk, so one request per message is the slow way.
  A publisher batches them and is many times faster.
- `process()` acks with its next consume call, so a worker needs one request per batch, not two.
- Each thread keeps its connection open and reuses it (`client.close()` or a `with Client(...)`
  block closes them). The standard library has no HTTP/2, so this client uses HTTP/1.1.

## Errors

Every failure raises `BerthqError` with the API's error `kind`, the HTTP `status` and a
`message`:

```python
from berthq_client import BerthqError

try:
    client.publish("emails", {"user": 123})
except BerthqError as err:
    if err.kind == "notFound":
        client.queues_create("emails")
    elif err.kind in ("unavailable", "http"):
        ...  # broker restarting or unreachable: retry later
    else:
        raise
```

Kinds: `invalid` (400), `unauthorized` (401), `forbidden` (403), `notFound` (404), `conflict`
(409), `queueFull` (507, the queue's backlog hit `max_messages`), `diskFull` (507, the broker's
disk is below `--min-free-disk`, 1 GB by default), `unavailable` (503), `storage` (500), and
`http` when there was no usable answer (connection refused, timeout, a response that isn't
JSON); `status` is `None` when the broker could not be reached.

## Development

Tests start real brokers, so build the binary first:

```sh
cargo build -p berthq-cli                     # from the repository root
cd sdks/python
python3 -m unittest discover -s tests -v
```

Set `BERTHQ_BINARY` to test against another build.
