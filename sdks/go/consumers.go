package berthq

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// Start and End are the named positions a consumer can start at or replay from. Any RFC 3339
// time or message id works too.
const (
	// Start is the oldest kept message.
	Start = "start"
	// End means only messages published from now on.
	End = "end"
)

// ConsumerStats are what a consumer holds.
type ConsumerStats struct {
	// Waiting counts messages after its position it hasn't received yet.
	Waiting int64 `json:"waiting"`
	// Processing counts its messages leased right now.
	Processing int64 `json:"processing"`
	// Retrying counts messages it received before that wait for their next attempt, backoff
	// over or not.
	Retrying int64 `json:"retrying"`
}

// WorkerInfo is a process seen consuming for a consumer in the last minute.
type WorkerInfo struct {
	Name     string    `json:"name"`
	LastSeen time.Time `json:"lastSeen"`
	// Waiting is true while it is long-polling.
	Waiting bool `json:"waiting"`
}

// ConsumerInfo is a name with a saved position in a queue. Every consumer gets every message; its
// workers share them.
type ConsumerInfo struct {
	Queue     string        `json:"queue"`
	Name      string        `json:"name"`
	CreatedAt time.Time     `json:"createdAt"`
	Paused    bool          `json:"paused"`
	Stats     ConsumerStats `json:"stats"`
	// Idle is true when no worker consumed for it for an hour or more while it still holds
	// messages back from deletion. Remove it if it is not coming back.
	Idle bool `json:"idle"`
	// LastSeen is the last time a worker consumed for it; nil if never.
	LastSeen *time.Time   `json:"lastSeen"`
	Workers  []WorkerInfo `json:"workers"`
}

// ConsumerMessageState is where a message is for one consumer.
type ConsumerMessageState string

const (
	// ConsumerWaiting messages haven't been received yet.
	ConsumerWaiting ConsumerMessageState = "waiting"
	// ConsumerProcessing messages are leased right now.
	ConsumerProcessing ConsumerMessageState = "processing"
	// ConsumerRetrying messages wait for their next attempt.
	ConsumerRetrying ConsumerMessageState = "retrying"
)

// ConsumerMessagesOptions filter ConsumersMessages. Zero values use the broker's defaults
// (processing, limit 50).
type ConsumerMessagesOptions struct {
	State ConsumerMessageState
	// Limit is up to 500.
	Limit int
}

// ConsumerMessage is a message as one consumer sees it.
type ConsumerMessage struct {
	ID          string               `json:"id"`
	Queue       string               `json:"queue"`
	Consumer    string               `json:"consumer"`
	State       ConsumerMessageState `json:"state"`
	Priority    int                  `json:"priority"`
	Attempts    int                  `json:"attempts"`
	PublishedAt time.Time            `json:"publishedAt"`
	LastError   *string              `json:"lastError"`
	// Worker and LeaseExpiresAt are set while processing.
	Worker         *string    `json:"worker"`
	LeaseExpiresAt *time.Time `json:"leaseExpiresAt"`
	// RetryAt is set while retrying.
	RetryAt *time.Time `json:"retryAt"`
	Size    int64      `json:"size"`
}

// ConsumersList returns a queue's consumers, sorted by name.
func (c *Client) ConsumersList(ctx context.Context, queue string) ([]ConsumerInfo, error) {
	var out struct {
		Consumers []ConsumerInfo `json:"consumers"`
	}
	if err := c.call(ctx, http.MethodGet, queuePath(queue, "/consumers"), nil, &out); err != nil {
		return nil, err
	}
	return out.Consumers, nil
}

// ConsumersGet returns one consumer.
func (c *Client) ConsumersGet(ctx context.Context, queue, consumer string) (*ConsumerInfo, error) {
	return c.consumerCall(ctx, http.MethodGet, consumerPath(queue, consumer, ""), nil)
}

// ConsumersCreate creates a consumer starting at from: Start (or ""), End, an RFC 3339 time or
// a message id. Fails with KindConflict when it exists. Consume also creates consumers on
// first use.
func (c *Client) ConsumersCreate(ctx context.Context, queue, name, from string) (*ConsumerInfo, error) {
	body := struct {
		Name string `json:"name"`
		From string `json:"from,omitempty"`
	}{name, from}
	return c.consumerCall(ctx, http.MethodPost, queuePath(queue, "/consumers"), body)
}

// ConsumersRemove removes a consumer with its position, leases and retries.
func (c *Client) ConsumersRemove(ctx context.Context, queue, consumer string) error {
	return c.call(ctx, http.MethodDelete, consumerPath(queue, consumer, ""), nil, nil)
}

// ConsumersReplay moves a consumer's position to from: Start, End, an RFC 3339 time or a
// message id. It drops what the consumer was processing and retrying.
func (c *Client) ConsumersReplay(ctx context.Context, queue, consumer, from string) (*ConsumerInfo, error) {
	body := map[string]string{"from": from}
	return c.consumerCall(ctx, http.MethodPost, consumerPath(queue, consumer, "/replay"), body)
}

// ConsumersPause stops deliveries to one consumer; other consumers continue.
func (c *Client) ConsumersPause(ctx context.Context, queue, consumer string) (*ConsumerInfo, error) {
	return c.consumerCall(ctx, http.MethodPost, consumerPath(queue, consumer, "/pause"), nil)
}

// ConsumersResume restarts deliveries to a paused consumer.
func (c *Client) ConsumersResume(ctx context.Context, queue, consumer string) (*ConsumerInfo, error) {
	return c.consumerCall(ctx, http.MethodPost, consumerPath(queue, consumer, "/resume"), nil)
}

// ConsumersMessages lists what a consumer is processing (the default), retrying, or hasn't
// received yet, without bodies.
func (c *Client) ConsumersMessages(ctx context.Context, queue, consumer string, opts ConsumerMessagesOptions) ([]ConsumerMessage, error) {
	query := url.Values{}
	if opts.State != "" {
		query.Set("state", string(opts.State))
	}
	if opts.Limit > 0 {
		query.Set("limit", strconv.Itoa(opts.Limit))
	}
	path := consumerPath(queue, consumer, "/messages")
	if len(query) > 0 {
		path += "?" + query.Encode()
	}
	var out struct {
		Messages []ConsumerMessage `json:"messages"`
	}
	if err := c.call(ctx, http.MethodGet, path, nil, &out); err != nil {
		return nil, err
	}
	return out.Messages, nil
}

func (c *Client) consumerCall(ctx context.Context, method, path string, body any) (*ConsumerInfo, error) {
	var out ConsumerInfo
	if err := c.call(ctx, method, path, body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
