package berthq

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

// Handler processes one delivery. Returning nil acks it; returning an error nacks it with the
// error's text, so it is retried (or dead-lettered when out of attempts).
type Handler func(ctx context.Context, d Delivery) error

// ProcessOptions configure Process. Zero values pick sensible defaults.
type ProcessOptions struct {
	// Concurrency is how many handlers run at once (default 1).
	Concurrency int
	// Batch is the most messages leased per request (default Concurrency, at most 100). Process
	// never leases more messages than it has free handlers for.
	Batch int
	// Consumer is whose position to use (default "default"). Workers of one consumer share its
	// messages; every consumer gets every message.
	Consumer string
	// From is where the consumer starts if it doesn't exist yet (default Start).
	From string
	// Worker is the name shown in the dashboard (default "anonymous").
	Worker string
	// VisibilityTimeoutMs is the lease length to ask for (default: the queue's setting).
	VisibilityTimeoutMs int64
	// AutoAck skips leases: the position moves on delivery and nothing is acked, nacked or
	// retried. Faster, but a crash or a failing handler loses the message for this consumer.
	AutoAck bool
	// OnError is called with errors Process recovers from: failed acks, nacks and extends,
	// consume errors it retries, and handler errors in AutoAck mode. Defaults to log.Printf.
	OnError func(err error)
}

// pollWaitMs is how long each consume request long-polls for messages.
const pollWaitMs = 20_000

// retryAfter is how long Process waits before consuming again after the broker was unreachable
// or shutting down.
const retryAfter = time.Second

// maxAcks is the most leases the broker takes in one request.
const maxAcks = 1000

// Process consumes queue and runs handler for every delivery until ctx is cancelled, then waits
// for running handlers to finish (and their messages to be settled) and returns nil.
//
// Finished messages are acked with the next Consume call, so a busy worker needs one request
// per batch; the last acks are sent on their own when Process stops. Handlers get a context that
// is not cancelled with ctx, so work in flight can finish cleanly. Until a message is settled,
// its lease is extended in the background every half lease.
// When the broker is unreachable or shutting down, Process waits a second and tries again; any
// other consume error (an unknown queue, a bad API key) is returned.
func (c *Client) Process(ctx context.Context, queue string, handler Handler, opts ProcessOptions) error {
	opts = processDefaults(opts)
	slots := make(chan struct{}, opts.Concurrency)
	acks := &pendingAcks{}
	var running sync.WaitGroup
	defer func() {
		running.Wait()
		c.flushAcks(queue, acks, opts)
	}()

	for {
		reserved := reserveSlots(ctx, slots, opts.Batch)
		if reserved == 0 {
			return nil
		}
		deliveries, err := c.nextBatch(ctx, queue, opts, acks, reserved)
		for i := len(deliveries); i < reserved; i++ {
			<-slots
		}
		if err != nil {
			if ctx.Err() != nil {
				// Messages leased by a request that was cut short are redelivered after their lease.
				return nil
			}
			kind := ErrorKind(err)
			if kind != KindUnavailable && kind != KindHTTP {
				return err
			}
			opts.OnError(err)
			select {
			case <-time.After(retryAfter):
			case <-ctx.Done():
				return nil
			}
			continue
		}
		for _, d := range deliveries {
			running.Add(1)
			go func(d Delivery) {
				defer running.Done()
				defer func() { <-slots }()
				c.handle(context.WithoutCancel(ctx), queue, d, handler, acks, opts)
			}(d)
		}
	}
}

// nextBatch gets up to max messages, sending the waiting acks along.
func (c *Client) nextBatch(ctx context.Context, queue string, opts ProcessOptions, acks *pendingAcks, max int) ([]Delivery, error) {
	ack := acks.next()
	consumed, err := c.Consume(ctx, queue, ConsumeOptions{
		Consumer:            opts.Consumer,
		From:                opts.From,
		Max:                 max,
		WaitMs:              pollWaitMs,
		VisibilityTimeoutMs: opts.VisibilityTimeoutMs,
		AutoAck:             opts.AutoAck,
		Worker:              opts.Worker,
		Ack:                 ack,
	})
	if err != nil {
		return nil, err
	}
	acks.sent(len(ack))
	return consumed.Messages, nil
}

func processDefaults(opts ProcessOptions) ProcessOptions {
	if opts.Concurrency < 1 {
		opts.Concurrency = 1
	}
	if opts.Batch < 1 {
		opts.Batch = opts.Concurrency
	}
	opts.Batch = min(opts.Batch, opts.Concurrency, 100)
	if opts.OnError == nil {
		opts.OnError = func(err error) { log.Printf("berthq: %v", err) }
	}
	return opts
}

// reserveSlots waits for one free handler slot, then takes up to limit-1 more that are free right
// now. It returns 0 when ctx is cancelled.
func reserveSlots(ctx context.Context, slots chan struct{}, limit int) int {
	select {
	case slots <- struct{}{}:
	case <-ctx.Done():
		return 0
	}
	reserved := 1
	for reserved < limit {
		select {
		case slots <- struct{}{}:
			reserved++
		default:
			return reserved
		}
	}
	return reserved
}

// handle runs the handler for one delivery, keeps its lease alive meanwhile, and queues the ack
// for the next Consume or nacks right away.
func (c *Client) handle(ctx context.Context, queue string, d Delivery, handler Handler, acks *pendingAcks, opts ProcessOptions) {
	if d.Lease == "" {
		// Auto-acked: the message is already done for this consumer, so a failure can only be reported.
		if err := runHandler(ctx, d, handler); err != nil {
			opts.OnError(fmt.Errorf("message %s (auto-acked, not retried): %w", d.ID, err))
		}
		return
	}
	stopExtending := c.keepLeased(ctx, queue, d.Lease, d.ID, d.VisibilityTimeoutMs, opts)
	err := runHandler(ctx, d, handler)
	if err == nil {
		acks.add(d.Lease, stopExtending)
		return
	}
	stopExtending()
	nack := NackOptions{Leases: []string{d.Lease}, Error: err.Error()}
	if _, err := c.Nack(ctx, queue, nack); err != nil {
		opts.OnError(fmt.Errorf("nack %s: %w", d.ID, err))
	}
}

// pendingAcks holds the leases of finished messages until they go out with the next Consume.
// Their leases stay extended until then, so a long-poll in between can't let them run out.
type pendingAcks struct {
	mu      sync.Mutex
	waiting []pendingAck
}

type pendingAck struct {
	lease         string
	stopExtending func()
}

func (a *pendingAcks) add(lease string, stopExtending func()) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.waiting = append(a.waiting, pendingAck{lease, stopExtending})
}

// next returns the leases for the next request, at most what the broker takes at once.
func (a *pendingAcks) next() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	leases := make([]string, min(len(a.waiting), maxAcks))
	for i := range leases {
		leases[i] = a.waiting[i].lease
	}
	return leases
}

// sent forgets the first count leases (the ones next returned), which the broker has acked, and
// stops extending them.
func (a *pendingAcks) sent(count int) {
	a.mu.Lock()
	done := a.waiting[:count]
	a.waiting = a.waiting[count:]
	a.mu.Unlock()
	for _, ack := range done {
		ack.stopExtending()
	}
}

// flushAcks sends the acks that didn't go out with a Consume. A lease that can't be acked runs
// out and its message is redelivered.
func (c *Client) flushAcks(queue string, acks *pendingAcks, opts ProcessOptions) {
	for {
		leases := acks.next()
		if len(leases) == 0 {
			return
		}
		if _, err := c.Ack(context.Background(), queue, leases); err != nil {
			opts.OnError(fmt.Errorf("ack %d messages: %w", len(leases), err))
		}
		acks.sent(len(leases))
	}
}

// runHandler turns a panic into an error, so one bad message nacks instead of crashing the
// worker.
func runHandler(ctx context.Context, d Delivery, handler Handler) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	return handler(ctx, d)
}

// keepLeased extends a lease every half lease until the returned stop function is called. It
// takes only the fields it needs, so the goroutine doesn't keep the message body alive.
func (c *Client) keepLeased(ctx context.Context, queue, lease, id string, visibilityMs int64, opts ProcessOptions) (stop func()) {
	interval := max(time.Duration(visibilityMs)*time.Millisecond/2, 50*time.Millisecond)

	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				settled, err := c.Extend(ctx, queue, []string{lease}, visibilityMs)
				if err != nil {
					opts.OnError(fmt.Errorf("extend %s: %w", id, err))
					continue
				}
				if settled.Settled == 0 {
					return // the lease is gone (expired and redelivered); nothing left to extend
				}
			}
		}
	}()
	return func() {
		close(done)
		<-finished
	}
}
