package berthq

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

// ConsumeOptions control a Consume call. Zero values use the broker's defaults.
type ConsumeOptions struct {
	// Consumer is whose position to use (default "default"); it is created if it doesn't exist.
	Consumer string `json:"consumer,omitempty"`
	// From is where a new consumer starts: Start (default), End, an RFC 3339 time or a message
	// id. It is ignored if the consumer exists.
	From string `json:"from,omitempty"`
	// Max is the most messages to return, 1-100 (default 1).
	Max int `json:"max,omitempty"`
	// WaitMs long-polls: wait up to this long for a message (default 0, at most 60000).
	WaitMs int64 `json:"waitMs,omitempty"`
	// VisibilityTimeoutMs is the lease length (default: the queue's setting).
	VisibilityTimeoutMs int64 `json:"visibilityTimeoutMs,omitempty"`
	// AutoAck skips the lease: the position moves right away and nothing is retried.
	AutoAck bool `json:"autoAck,omitempty"`
	// Worker is the name shown in the dashboard (default "anonymous").
	Worker string `json:"worker,omitempty"`
	// Ack lists leases to ack before getting new messages (up to 1000), saving an Ack call.
	Ack []string `json:"ack,omitempty"`
}

// Consumed is what Consume returns: the messages, and how the Ack leases were settled.
type Consumed struct {
	Messages []Delivery `json:"messages"`
	Settled  int        `json:"settled"`
	// Unknown lists Ack leases that no longer exist (already settled, expired and redelivered,
	// or dropped by a replay).
	Unknown []string `json:"unknown"`
}

// Delivery is a delivered message. Settle it with its Lease before LeaseExpiresAt, or it is
// retried.
type Delivery struct {
	// ID is the same on every redelivery and replay, so it works as an idempotency key.
	ID string `json:"id"`
	// Lease is what Ack, Nack and Extend take. It is empty with AutoAck.
	Lease    string `json:"lease"`
	Queue    string `json:"queue"`
	Consumer string `json:"consumer"`
	// Body is the raw JSON body; use Decode to unmarshal it.
	Body     json.RawMessage `json:"body"`
	Priority int             `json:"priority"`
	// Attempt is 1 on the first delivery.
	Attempt     int       `json:"attempt"`
	PublishedAt time.Time `json:"publishedAt"`
	// LeaseExpiresAt is nil with AutoAck.
	LeaseExpiresAt *time.Time `json:"leaseExpiresAt"`
	// VisibilityTimeoutMs is the lease length; Extend restarts a lease clock of this length.
	// It is 0 with AutoAck.
	VisibilityTimeoutMs int64   `json:"visibilityTimeoutMs"`
	DedupID             *string `json:"dedupId"`
	// LastError is the error from the previous failed attempt, if any.
	LastError *string `json:"lastError"`
}

// Decode unmarshals the delivery's JSON body into v.
func (d Delivery) Decode(v any) error {
	return json.Unmarshal(d.Body, v)
}

// NackOptions control a Nack call.
type NackOptions struct {
	Leases []string `json:"leases"`
	// DelayMs retries after this long instead of the queue's backoff (Ptr(int64(0)) retries
	// right away).
	DelayMs *int64 `json:"delayMs,omitempty"`
	// DeadLetter skips the remaining attempts and dead-letters the messages.
	DeadLetter bool `json:"deadLetter,omitempty"`
	// Error is kept on the message and shown in the dashboard.
	Error string `json:"error,omitempty"`
}

// Settled is the result of Ack, Nack and Extend.
type Settled struct {
	Settled int `json:"settled"`
	// Unknown lists leases that no longer exist (already settled, expired and redelivered, or
	// dropped by a replay).
	Unknown []string `json:"unknown"`
}

// Consume acks opts.Ack, then gets up to opts.Max messages for opts.Consumer (default
// "default"), creating the consumer if it doesn't exist. With opts.WaitMs it waits for a
// message to arrive (long-polling); the request may take that long plus 30 seconds before it
// times out.
func (c *Client) Consume(ctx context.Context, queue string, opts ConsumeOptions) (*Consumed, error) {
	timeout := time.Duration(opts.WaitMs)*time.Millisecond + requestTimeout
	var out Consumed
	if err := c.callTimeout(ctx, timeout, http.MethodPost, queuePath(queue, "/consume"), opts, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Ack marks deliveries as done for their consumer. Leases that no longer exist are listed in
// Settled.Unknown; that is not an error.
func (c *Client) Ack(ctx context.Context, queue string, leases []string) (*Settled, error) {
	return c.settle(ctx, queue, "/ack", map[string]any{"leases": leases})
}

// Nack marks deliveries as failed: they are retried after a backoff, or dead-lettered when out
// of attempts.
func (c *Client) Nack(ctx context.Context, queue string, opts NackOptions) (*Settled, error) {
	return c.settle(ctx, queue, "/nack", opts)
}

// Extend restarts the lease clock of deliveries that take long to process. A
// visibilityTimeoutMs of 0 uses the queue's visibility timeout.
func (c *Client) Extend(ctx context.Context, queue string, leases []string, visibilityTimeoutMs int64) (*Settled, error) {
	body := struct {
		Leases              []string `json:"leases"`
		VisibilityTimeoutMs int64    `json:"visibilityTimeoutMs,omitempty"`
	}{leases, visibilityTimeoutMs}
	return c.settle(ctx, queue, "/extend", body)
}

func (c *Client) settle(ctx context.Context, queue, action string, body any) (*Settled, error) {
	var out Settled
	if err := c.call(ctx, http.MethodPost, queuePath(queue, action), body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
