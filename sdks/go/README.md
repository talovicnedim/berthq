# Berthq Go SDK

Go client for [Berthq](https://github.com/talovicnedim/berthq), a message broker in one binary.
Standard library only, Go 1.22+.

## Install

```sh
go get github.com/talovicnedim/berthq/sdks/go@latest
```

Each Berthq release also tags the SDK as `sdks/go/<version>`, so
`go get github.com/talovicnedim/berthq/sdks/go@v0.1.0` gets the SDK from Berthq v0.1.0.

```go
import berthq "github.com/talovicnedim/berthq/sdks/go"
```

## Quick start

Start a broker (`berthq start`), then publish:

```go
ctx := context.Background()
client := berthq.New("http://127.0.0.1:7331") // berthq.WithAPIKey("...") when keys are on

if _, err := client.QueuesCreate(ctx, "emails", berthq.QueueSettingsPatch{
	MaxAttempts: berthq.Ptr(3),
}); err != nil && !berthq.IsConflict(err) {
	log.Fatal(err)
}

published, err := client.Publish(ctx, "emails", berthq.NewMessage{
	Body:     map[string]any{"user": 123},
	Priority: 5,
	DedupID:  "welcome-123",
})
if err != nil {
	log.Fatal(err)
}
fmt.Println("published", published.ID)
```

And run a worker:

```go
ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()

err := client.Process(ctx, "emails", func(ctx context.Context, d berthq.Delivery) error {
	var job struct{ User int }
	if err := d.Decode(&job); err != nil {
		return err
	}
	return sendWelcomeEmail(ctx, job.User) // nil acks, an error nacks
}, berthq.ProcessOptions{Concurrency: 8, Worker: "mailer-1"})
if err != nil {
	log.Fatal(err)
}
```

`Process` long-polls for messages and runs up to `Concurrency` handlers at once, leasing at most
`Batch` messages per request and never more than it has free handlers for. A handler that
returns `nil` acks its message; an error nacks it with `err.Error()` as the last error, so the
broker retries it with backoff and dead-letters it when it is out of attempts. A panic in a
handler is turned into a nack.

While a handler runs, its lease is extended every half lease, so slow jobs are not redelivered.
When the broker is unreachable or shutting down, `Process` waits a second and tries again;
other errors (an unknown queue, a bad API key) are returned. Errors it recovers from (a failed
ack, for example) go to `ProcessOptions.OnError`, which logs by default.

Cancelling `ctx` stops fetching new messages. Handlers already running get a context that is not
cancelled, so they can finish and be acked; `Process` returns `nil` once they are done.

Delivery is at-least-once, so handlers should be idempotent: `d.ID` is the same on every
redelivery and replay and works as an idempotency key (see
[guarantees.md](../../docs/guarantees.md)). With `AutoAck: true` nothing is leased or retried:
the position moves on delivery, which is faster, but a crash or a failing handler loses the
message for the consumer (handler errors go to `OnError`).

## Consumers and replay

A consumer is a name with a saved position in a queue. Every consumer gets every message, at
its own pace; calls without a consumer use `default`. A consumer is created by its first
`Consume`, at the oldest kept message (or at `From`), and continues where it stopped after that.

```go
// A second service gets every order too, starting with new ones only.
err := client.Process(ctx, "orders", bill, berthq.ProcessOptions{Consumer: "billing", From: berthq.End})

// Billing had a bug since 9:00: process everything from then again.
_, err = client.ConsumersReplay(ctx, "orders", "billing", "2026-10-14T09:00:00Z") // or berthq.Start, berthq.End, a message id

consumers, err := client.ConsumersList(ctx, "orders")
retrying, err := client.ConsumersMessages(ctx, "orders", "billing", berthq.ConsumerMessagesOptions{State: berthq.ConsumerRetrying})
```

## Consuming by hand

```go
var done []string
for {
	consumed, err := client.Consume(ctx, "emails", berthq.ConsumeOptions{Max: 10, WaitMs: 20_000, Ack: done})
	if err != nil {
		return err
	}
	done = nil
	for _, d := range consumed.Messages {
		if err := handle(d); err != nil {
			client.Nack(ctx, "emails", berthq.NackOptions{Leases: []string{d.Lease}, Error: err.Error()})
			continue
		}
		done = append(done, d.Lease) // acked with the next Consume: one request per batch
	}
}
```

Send `done` with `Ack` on its own before you stop, and before half a lease has passed if a
batch takes long. `Process` does all of that for you.

## Publishing fast

A `Publisher` sends messages in batches of up to 1000 (or 4 MiB). A message goes out right away,
and the messages that arrive meanwhile form the next batch (`Linger` can make a batch wait for
more). Never slower than one request per message, and up to about 13 times faster when many are
sent at once. It is safe for many
goroutines; one batch is in flight at a time, so messages keep their order.

```go
publisher := client.Publisher("emails", berthq.PublisherOptions{})
defer publisher.Close(ctx)

for _, user := range users {
	go func() {
		if _, err := publisher.Publish(ctx, berthq.NewMessage{Body: map[string]any{"user": user.ID}}); err != nil {
			log.Print(err)
		}
	}()
}
```

`Publish` returns once its batch is on disk. If a batch is rejected as invalid, it is split in
half until the bad message is alone, so only it fails. The client keeps up to 64 idle
connections open and uses HTTP/2 over HTTPS.

## API

Method names match the HTTP API ([docs/api.md](../../docs/api.md)) and the other SDKs. Every
method takes a `context.Context` first.

| Area      | Methods                                                                                                                                            |
| --------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| Queues    | `QueuesList`, `QueuesCreate`, `QueuesGet`, `QueuesUpdate`, `QueuesRemove`, `QueuesPurge`, `QueuesPause`, `QueuesResume`                            |
| Publish   | `Publish`, `PublishBatch`, `Publisher` (`Publish`, `Flush`, `Close`)                                                                               |
| Consume   | `Consume`, `Ack`, `Nack`, `Extend`, `Process`                                                                                                      |
| Consumers | `ConsumersList`, `ConsumersGet`, `ConsumersCreate`, `ConsumersRemove`, `ConsumersReplay`, `ConsumersPause`, `ConsumersResume`, `ConsumersMessages` |
| Inspect   | `MessagesList`, `MessagesGet`, `MessagesRemove`, `Requeue`, `Stats`                                                                                |

Each request times out after 30 seconds; `Consume` gets its `WaitMs` on top of that. Timeouts
are applied through the context, so if you pass your own client with `WithHTTPClient`, leave its
`Timeout` at 0.

## Errors

Every method returns a `*berthq.Error` with the API's error `Kind`, the HTTP `Status` and a
`Message`:

```go
_, err := client.Publish(ctx, "emails", msg)

var berr *berthq.Error
if errors.As(err, &berr) {
	switch berr.Kind {
	case berthq.KindQueueFull: // 507: the queue's backlog hit maxMessages
	case berthq.KindDiskFull: // 507: the broker's disk is almost full; try again later
	case berthq.KindNotFound: // 404: no such queue
	case berthq.KindUnauthorized, berthq.KindForbidden: // 401, 403: API key missing or not allowed
	case berthq.KindHTTP: // the broker could not be reached (Status 0)
	}
}
```

Kinds: `invalid` (400), `unauthorized` (401), `forbidden` (403), `notFound` (404), `conflict`
(409), `queueFull` (507), `diskFull` (507: the broker takes no new messages while its disk is
almost full), `unavailable` (503), `storage` (500), and `http` for transport errors and
unexpected answers. `berthq.IsNotFound(err)`, `berthq.IsConflict(err)` and
`berthq.ErrorKind(err)` are shortcuts. Transport errors wrap the underlying error, so
`errors.Is(err, context.DeadlineExceeded)` works.

## Development

Most tests run against real brokers and are skipped when these are not set:

- `BERTHQ_TEST_URL`: a broker started without API keys.
- `BERTHQ_TEST_AUTH_URL`: a broker started with
  `--api-key adminadminadmin1 --api-key publish:publishpublish12`.

```sh
berthq start --port 7441 --data /tmp/berthq-test &
berthq start --port 7442 --data /tmp/berthq-test-auth \
  --api-key adminadminadmin1 --api-key publish:publishpublish12 &

BERTHQ_TEST_URL=http://127.0.0.1:7441 BERTHQ_TEST_AUTH_URL=http://127.0.0.1:7442 \
  go test -race ./...
```

Without Go installed, run them in Docker against brokers on the host (started with
`--host 0.0.0.0`, plus `--no-auth` for the one without keys):

```sh
docker run --rm -v "$PWD":/src -w /src \
  -e BERTHQ_TEST_URL=http://host.docker.internal:7441 \
  -e BERTHQ_TEST_AUTH_URL=http://host.docker.internal:7442 \
  golang:1.25 sh -c 'gofmt -l . && go vet ./... && go test -race ./...'
```

Every test uses its own queue names, so the brokers can be reused between runs.
