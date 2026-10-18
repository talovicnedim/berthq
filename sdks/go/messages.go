package berthq

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// NewMessage is a message to publish. Only Body is required; zero values of the other fields
// are left out of the request, so the broker's defaults apply.
type NewMessage struct {
	// Body is any value that encodes to JSON. Plain text is a JSON string. Up to 4 MB.
	Body any `json:"body"`
	// Priority is 0-9, higher is delivered first.
	Priority int `json:"priority,omitempty"`
	// DelayMs hides the message for this long.
	DelayMs int64 `json:"delayMs,omitempty"`
	// DeliverAt hides the message until this time; it overrides DelayMs.
	DeliverAt *time.Time `json:"deliverAt,omitempty"`
	// DedupID makes repeats on the same queue within an hour a no-op.
	DedupID string `json:"dedupId,omitempty"`
}

// Published is the result of publishing one message.
type Published struct {
	ID string `json:"id"`
	// Duplicate is true when the dedup id was already seen: nothing was stored and ID is the
	// original message's id.
	Duplicate bool `json:"duplicate"`
}

// MessageState is whether a stored message can be delivered yet.
type MessageState string

const (
	StateVisible MessageState = "visible"
	StateDelayed MessageState = "delayed"
)

// MessagesListOptions filter MessagesList. Zero values use the broker's defaults (visible
// messages, limit 50).
type MessagesListOptions struct {
	State MessageState
	// Limit is up to 500.
	Limit int
	// Before is a message id: list messages older than it (the list is newest first).
	Before string
}

// MessageDeadLetter says why, where and when a message was dead-lettered.
type MessageDeadLetter struct {
	// Queue is the queue the message failed in.
	Queue string `json:"queue"`
	// Consumer is the consumer that failed it; Requeue moves it back to this consumer.
	Consumer string `json:"consumer"`
	// MessageID is the original message's id (the dead letter has its own ID). A requeued
	// message is delivered with this id again.
	MessageID string `json:"messageId"`
	// Reason is "maxAttempts" or "rejected".
	Reason string    `json:"reason"`
	At     time.Time `json:"at"`
}

// MessageInfo is a stored message as seen when browsing a queue.
type MessageInfo struct {
	ID          string       `json:"id"`
	Queue       string       `json:"queue"`
	State       MessageState `json:"state"`
	Priority    int          `json:"priority"`
	PublishedAt time.Time    `json:"publishedAt"`
	DeliverAt   *time.Time   `json:"deliverAt"`
	DedupID     *string      `json:"dedupId"`
	Size        int64        `json:"size"`
	// Attempts and LastError come from the consumer that failed a dead letter; 0 and nil
	// otherwise.
	Attempts   int                `json:"attempts"`
	LastError  *string            `json:"lastError"`
	DeadLetter *MessageDeadLetter `json:"deadLetter"`
	// Body is only set by MessagesGet; MessagesList returns JSON null.
	Body json.RawMessage `json:"body"`
}

// Requeued is the result of Requeue.
type Requeued struct {
	Requeued int `json:"requeued"`
	// Skipped counts dead letters whose queue or consumer no longer exists, or whose consumer
	// already has the message pending.
	Skipped int `json:"skipped"`
}

// Publish stores one message. It returns after the message is on disk.
func (c *Client) Publish(ctx context.Context, queue string, message NewMessage) (*Published, error) {
	var out Published
	if err := c.call(ctx, http.MethodPost, queuePath(queue, "/messages"), message, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// PublishBatch stores up to 1000 messages atomically: all of them or none. Results are in the
// same order as messages.
func (c *Client) PublishBatch(ctx context.Context, queue string, messages []NewMessage) ([]Published, error) {
	body := map[string]any{"messages": messages}
	var out struct {
		Messages []Published `json:"messages"`
	}
	if err := c.call(ctx, http.MethodPost, queuePath(queue, "/batch"), body, &out); err != nil {
		return nil, err
	}
	return out.Messages, nil
}

// publishEncoded publishes messages that are already JSON as one atomic batch, for Publisher.
// Raw messages are copied into the request as they are, not encoded again.
func (c *Client) publishEncoded(ctx context.Context, queue string, messages []json.RawMessage) ([]Published, error) {
	body := struct {
		Messages []json.RawMessage `json:"messages"`
	}{messages}
	var out struct {
		Messages []Published `json:"messages"`
	}
	if err := c.call(ctx, http.MethodPost, queuePath(queue, "/batch"), body, &out); err != nil {
		return nil, err
	}
	if len(out.Messages) != len(messages) {
		return nil, &Error{Kind: KindHTTP, Message: fmt.Sprintf("the broker answered %d results for %d messages", len(out.Messages), len(messages))}
	}
	return out.Messages, nil
}

// MessagesList browses stored messages, newest first, without consuming them. Bodies are not
// included; use MessagesGet for that.
func (c *Client) MessagesList(ctx context.Context, queue string, opts MessagesListOptions) ([]MessageInfo, error) {
	query := url.Values{}
	if opts.State != "" {
		query.Set("state", string(opts.State))
	}
	if opts.Limit > 0 {
		query.Set("limit", strconv.Itoa(opts.Limit))
	}
	if opts.Before != "" {
		query.Set("before", opts.Before)
	}
	path := queuePath(queue, "/messages")
	if len(query) > 0 {
		path += "?" + query.Encode()
	}
	var out struct {
		Messages []MessageInfo `json:"messages"`
	}
	if err := c.call(ctx, http.MethodGet, path, nil, &out); err != nil {
		return nil, err
	}
	return out.Messages, nil
}

// MessagesGet returns one message including its body.
func (c *Client) MessagesGet(ctx context.Context, queue, id string) (*MessageInfo, error) {
	var out MessageInfo
	if err := c.call(ctx, http.MethodGet, queuePath(queue, "/messages/"+url.PathEscape(id)), nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// MessagesRemove deletes one message for every consumer.
func (c *Client) MessagesRemove(ctx context.Context, queue, id string) error {
	return c.call(ctx, http.MethodDelete, queuePath(queue, "/messages/"+url.PathEscape(id)), nil, nil)
}

// Requeue moves dead letters from a dead-letter queue back to the queue and consumer that
// failed them, with their attempts reset. A nil ids moves all of them.
func (c *Client) Requeue(ctx context.Context, queue string, ids []string) (*Requeued, error) {
	body := map[string]any{}
	if ids != nil {
		body["ids"] = ids
	}
	var out Requeued
	if err := c.call(ctx, http.MethodPost, queuePath(queue, "/requeue"), body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
