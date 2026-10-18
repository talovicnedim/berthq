package berthq

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// PublisherOptions configure a Publisher. Zero values pick the defaults.
type PublisherOptions struct {
	// MaxBatch is the most messages per batch, 1-1000 (default 1000).
	MaxBatch int
	// Linger is how long the first waiting message waits for others before its batch is sent.
	// The default, zero, sends right away: messages that arrive while a batch is on its way form
	// the next one.
	Linger time.Duration
	// MaxBytes is the most bytes of JSON per batch (default 4 MiB). A bigger message is sent
	// alone.
	MaxBytes int
}

// batchLimit is the most messages the broker takes per batch.
const batchLimit = 1000

// Publisher collects messages and publishes them in batches of up to MaxBatch (and MaxBytes).
// One batch is in flight at a time, so messages keep their order; what arrives meanwhile forms
// the next batch. It is safe for concurrent use. Make one with Client.Publisher and Close it to
// send the last messages.
type Publisher struct {
	client *Client
	queue  string
	opts   PublisherOptions
	// wake (buffered) tells the sending goroutine that something changed.
	wake    chan struct{}
	stopped chan struct{}

	mu           sync.Mutex
	waiting      []*pendingMessage
	waitingBytes int
	seq          uint64
	doneSeq      uint64
	closed       bool
	flushes      []flushWaiter
}

// pendingMessage is a message encoded once, and where its result goes.
type pendingMessage struct {
	seq      uint64
	data     json.RawMessage
	queuedAt time.Time
	// canceled is set when its Publish call gave up before the batch was sent.
	canceled bool
	result   chan publishResult
}

type publishResult struct {
	published Published
	err       error
}

type flushWaiter struct {
	seq  uint64
	done chan struct{}
}

// Publisher returns a publisher for queue. See PublisherOptions.
func (c *Client) Publisher(queue string, opts PublisherOptions) *Publisher {
	p := &Publisher{
		client:  c,
		queue:   queue,
		opts:    publisherDefaults(opts),
		wake:    make(chan struct{}, 1),
		stopped: make(chan struct{}),
	}
	go p.run()
	return p
}

func publisherDefaults(opts PublisherOptions) PublisherOptions {
	if opts.MaxBatch < 1 {
		opts.MaxBatch = batchLimit
	}
	opts.MaxBatch = min(opts.MaxBatch, batchLimit)
	opts.Linger = max(opts.Linger, 0)
	if opts.MaxBytes < 1 {
		opts.MaxBytes = 4 << 20
	}
	return opts
}

// Publish queues a message and returns its result once its batch is on disk, or the error that
// batch (or, after an invalid batch, this message alone) got. If ctx ends first, Publish returns
// ctx's error: the message is left out if its batch hasn't gone yet, and may still be published
// if it has.
func (p *Publisher) Publish(ctx context.Context, message NewMessage) (Published, error) {
	data, err := encodeJSON(message)
	if err != nil {
		return Published{}, &Error{Kind: KindInvalid, Message: "could not encode the message: " + err.Error(), Err: err}
	}
	entry := &pendingMessage{data: data, queuedAt: time.Now(), result: make(chan publishResult, 1)}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return Published{}, &Error{Kind: KindInvalid, Message: fmt.Sprintf("the publisher for %s is closed", p.queue)}
	}
	p.seq++
	entry.seq = p.seq
	p.waiting = append(p.waiting, entry)
	p.waitingBytes += len(entry.data)
	p.mu.Unlock()
	p.signal()

	select {
	case result := <-entry.result:
		return result.published, result.err
	case <-ctx.Done():
		// Left out if its batch hasn't been taken yet; once taken, the flag isn't read.
		p.mu.Lock()
		entry.canceled = true
		p.mu.Unlock()
		return Published{}, ctx.Err()
	}
}

// Flush sends every message queued so far without waiting for the linger, and returns once
// they are all settled (or ctx ends). Failures go to their own Publish calls.
func (p *Publisher) Flush(ctx context.Context) error {
	p.mu.Lock()
	target := p.seq
	if p.doneSeq >= target {
		p.mu.Unlock()
		return nil
	}
	done := make(chan struct{})
	p.flushes = append(p.flushes, flushWaiter{seq: target, done: done})
	p.mu.Unlock()
	p.signal()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close flushes and stops the publisher. Publish fails with KindInvalid afterwards.
func (p *Publisher) Close(ctx context.Context) error {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	p.signal()
	if err := p.Flush(ctx); err != nil {
		return err
	}
	select {
	case <-p.stopped:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Publisher) signal() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// run sends one batch at a time until the publisher is closed and nothing is left.
func (p *Publisher) run() {
	defer close(p.stopped)
	for {
		batch, last, wait, stop := p.next()
		if stop {
			return
		}
		if last == 0 {
			p.sleep(wait)
			continue
		}
		p.send(batch)
		p.settled(last)
	}
}

// next takes the next batch if it is due, with the seq of its last message (canceled messages
// are left out of the batch but count as settled). Otherwise it says how long to wait: -1 means
// until woken.
func (p *Publisher) next() (batch []*pendingMessage, last uint64, wait time.Duration, stop bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.waiting) == 0 {
		return nil, 0, -1, p.closed
	}
	if wait := p.lingerLeft(); wait > 0 {
		return nil, 0, wait, false
	}
	taken := p.take()
	for _, entry := range taken {
		if !entry.canceled {
			batch = append(batch, entry)
		}
	}
	return batch, taken[len(taken)-1].seq, 0, false
}

// lingerLeft is 0 when the next batch should go now: it is full, a flush or Close asked for it,
// or its first message has waited long enough.
func (p *Publisher) lingerLeft() time.Duration {
	first := p.waiting[0]
	full := len(p.waiting) >= p.opts.MaxBatch || p.waitingBytes >= p.opts.MaxBytes
	flushing := len(p.flushes) > 0 && first.seq <= p.flushes[len(p.flushes)-1].seq
	if full || p.closed || flushing {
		return 0
	}
	return max(0, time.Until(first.queuedAt.Add(p.opts.Linger)))
}

func (p *Publisher) take() []*pendingMessage {
	count, bytes := 0, 0
	for _, entry := range p.waiting {
		if count == p.opts.MaxBatch || (count > 0 && bytes+len(entry.data) > p.opts.MaxBytes) {
			break
		}
		count++
		bytes += len(entry.data)
	}
	taken := p.waiting[:count:count]
	p.waiting = p.waiting[count:]
	p.waitingBytes -= bytes
	return taken
}

func (p *Publisher) sleep(wait time.Duration) {
	if wait < 0 {
		<-p.wake
		return
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-p.wake:
	case <-timer.C:
	}
}

// send publishes a batch. One bad message fails the whole batch, so a batch rejected as invalid
// is split in half until the bad messages are alone: only they fail, and the rest still go in
// order.
func (p *Publisher) send(batch []*pendingMessage) {
	parts := [][]*pendingMessage{batch} // still to send, the next one last
	for len(parts) > 0 {
		part := parts[len(parts)-1]
		parts = parts[:len(parts)-1]
		if len(part) == 0 {
			continue
		}
		messages := make([]json.RawMessage, len(part))
		for i, entry := range part {
			messages[i] = entry.data
		}
		results, err := p.client.publishEncoded(context.Background(), p.queue, messages)
		switch {
		case err == nil:
			for i, entry := range part {
				entry.result <- publishResult{published: results[i]}
			}
		case ErrorKind(err) == KindInvalid && len(part) > 1:
			half := len(part) / 2
			parts = append(parts, part[half:], part[:half])
		default:
			for _, entry := range part {
				entry.result <- publishResult{err: err}
			}
		}
	}
}

func (p *Publisher) settled(seq uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.doneSeq = seq
	kept := p.flushes[:0]
	for _, flush := range p.flushes {
		if flush.seq <= seq {
			close(flush.done)
			continue
		}
		kept = append(kept, flush)
	}
	p.flushes = kept
}
