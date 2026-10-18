package berthq

import (
	"context"
	"net/http"
	"time"
)

// QueueSettings are a queue's current settings.
type QueueSettings struct {
	// RetentionMs is how long messages stay after they become visible, for replay.
	RetentionMs         int64 `json:"retentionMs"`
	VisibilityTimeoutMs int64 `json:"visibilityTimeoutMs"`
	// MaxAttempts is the deliveries before a message is dead-lettered; 0 retries forever.
	MaxAttempts int `json:"maxAttempts"`
	// RetryDelayMs is the first retry delay. It doubles per attempt, up to 5 minutes or
	// RetryDelayMs itself, whichever is longer.
	RetryDelayMs int64 `json:"retryDelayMs"`
	// MaxMessages limits the backlog; nil means no limit.
	MaxMessages *int `json:"maxMessages"`
	Paused      bool `json:"paused"`
}

// QueueSettingsPatch holds settings to set when creating or updating a queue. Nil fields keep
// the current (or default) value. MaxMessages of 0 removes the limit.
// Use Ptr to fill the fields: QueueSettingsPatch{MaxAttempts: berthq.Ptr(3)}.
type QueueSettingsPatch struct {
	RetentionMs         *int64 `json:"retentionMs,omitempty"`
	VisibilityTimeoutMs *int64 `json:"visibilityTimeoutMs,omitempty"`
	MaxAttempts         *int   `json:"maxAttempts,omitempty"`
	RetryDelayMs        *int64 `json:"retryDelayMs,omitempty"`
	MaxMessages         *int   `json:"maxMessages,omitempty"`
}

// Ptr returns a pointer to v, for the optional fields of QueueSettingsPatch and NackOptions.
func Ptr[T any](v T) *T {
	return &v
}

// Counters count events since the broker started.
type Counters struct {
	Published    int64 `json:"published"`
	Delivered    int64 `json:"delivered"`
	Acked        int64 `json:"acked"`
	Retried      int64 `json:"retried"`
	DeadLettered int64 `json:"deadLettered"`
	// Dropped counts messages that ran out of attempts inside a dead-letter queue, which has no
	// dead-letter queue of its own.
	Dropped int64 `json:"dropped"`
}

// QueueStats are a queue's message counts and rates (messages per second over the last second).
type QueueStats struct {
	// Messages counts everything stored, including messages kept only for replay and delayed ones.
	Messages int64 `json:"messages"`
	// Backlog counts messages at least one consumer still has to process.
	Backlog int64 `json:"backlog"`
	// Delayed counts messages waiting for their deliverAt.
	Delayed int64 `json:"delayed"`
	// Processing counts messages leased right now, across all consumers.
	Processing int64 `json:"processing"`
	Bytes      int64 `json:"bytes"`
	Consumers  int64 `json:"consumers"`
	// Workers counts workers seen in the last minute, across all consumers.
	Workers     int64    `json:"workers"`
	Counters    Counters `json:"counters"`
	PublishRate int64    `json:"publishRate"`
	DeliverRate int64    `json:"deliverRate"`
	AckRate     int64    `json:"ackRate"`
}

// QueueInfo is a queue with its settings and stats.
type QueueInfo struct {
	Name      string        `json:"name"`
	CreatedAt time.Time     `json:"createdAt"`
	Settings  QueueSettings `json:"settings"`
	Stats     QueueStats    `json:"stats"`
	// DeadLetterQueue is the name of this queue's dead-letter queue, if it has one.
	DeadLetterQueue *string `json:"deadLetterQueue"`
	// DeadLetterOf is set on a dead-letter queue: the queue its messages failed in.
	DeadLetterOf *string `json:"deadLetterOf"`
}

// QueueDetails is a queue plus its consumers and the last minute of throughput.
type QueueDetails struct {
	QueueInfo
	Consumers []ConsumerInfo `json:"consumers"`
	History   []StatsPoint   `json:"history"`
}

// StatsPoint is one second of throughput.
type StatsPoint struct {
	At        time.Time `json:"at"`
	Published int64     `json:"published"`
	Delivered int64     `json:"delivered"`
	Acked     int64     `json:"acked"`
	Failed    int64     `json:"failed"`
}

// Overview holds broker-wide totals, rates and the last minute of throughput.
type Overview struct {
	Version   string    `json:"version"`
	StartedAt time.Time `json:"startedAt"`
	Queues    int64     `json:"queues"`
	Messages  int64     `json:"messages"`
	// Backlog counts messages at least one consumer still has to process.
	Backlog int64 `json:"backlog"`
	// Processing counts messages leased to workers.
	Processing int64 `json:"processing"`
	// Failed counts messages in dead-letter queues.
	Failed      int64        `json:"failed"`
	Consumers   int64        `json:"consumers"`
	Workers     int64        `json:"workers"`
	PublishRate int64        `json:"publishRate"`
	DeliverRate int64        `json:"deliverRate"`
	AckRate     int64        `json:"ackRate"`
	Counters    Counters     `json:"counters"`
	History     []StatsPoint `json:"history"`
	Disk        DiskStats    `json:"disk"`
}

// DiskStats are the free space on the disk that holds the broker's data, in bytes.
type DiskStats struct {
	Free int64 `json:"free"`
	// MinFree is the broker's --min-free-disk; 0 turns the check off.
	MinFree int64 `json:"minFree"`
	// Full is true while Free is below MinFree: publishes then fail with KindDiskFull, while
	// consuming and acking go on.
	Full bool `json:"full"`
}

// QueuesList returns every queue with its settings and stats.
func (c *Client) QueuesList(ctx context.Context) ([]QueueInfo, error) {
	var out struct {
		Queues []QueueInfo `json:"queues"`
	}
	if err := c.call(ctx, http.MethodGet, "/v1/queues", nil, &out); err != nil {
		return nil, err
	}
	return out.Queues, nil
}

// QueuesCreate creates a queue. Settings left nil in the patch get their defaults. Fails with
// KindConflict when the queue exists.
func (c *Client) QueuesCreate(ctx context.Context, name string, settings QueueSettingsPatch) (*QueueInfo, error) {
	body := struct {
		Name string `json:"name"`
		QueueSettingsPatch
	}{name, settings}
	var out QueueInfo
	if err := c.call(ctx, http.MethodPost, "/v1/queues", body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// QueuesGet returns a queue plus its consumers and throughput.
func (c *Client) QueuesGet(ctx context.Context, queue string) (*QueueDetails, error) {
	var out QueueDetails
	if err := c.call(ctx, http.MethodGet, queuePath(queue, ""), nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// QueuesUpdate changes the settings that are set in the patch.
func (c *Client) QueuesUpdate(ctx context.Context, queue string, settings QueueSettingsPatch) (*QueueInfo, error) {
	return c.queueAction(ctx, http.MethodPatch, queue, "", settings)
}

// QueuesRemove deletes a queue, its consumers and all its messages. Its dead-letter queue
// stays.
func (c *Client) QueuesRemove(ctx context.Context, queue string) error {
	return c.call(ctx, http.MethodDelete, queuePath(queue, ""), nil, nil)
}

// QueuesPurge deletes every message, including ones being processed, and returns how many.
func (c *Client) QueuesPurge(ctx context.Context, queue string) (int64, error) {
	var out struct {
		Purged int64 `json:"purged"`
	}
	if err := c.call(ctx, http.MethodPost, queuePath(queue, "/purge"), nil, &out); err != nil {
		return 0, err
	}
	return out.Purged, nil
}

// QueuesPause stops deliveries to every consumer; publishing still works.
func (c *Client) QueuesPause(ctx context.Context, queue string) (*QueueInfo, error) {
	return c.queueAction(ctx, http.MethodPost, queue, "/pause", nil)
}

// QueuesResume restarts deliveries of a paused queue.
func (c *Client) QueuesResume(ctx context.Context, queue string) (*QueueInfo, error) {
	return c.queueAction(ctx, http.MethodPost, queue, "/resume", nil)
}

// Stats returns broker-wide totals, rates, the last minute of throughput and the free disk
// space.
func (c *Client) Stats(ctx context.Context) (*Overview, error) {
	var out Overview
	if err := c.call(ctx, http.MethodGet, "/v1/stats", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) queueAction(ctx context.Context, method, queue, rest string, body any) (*QueueInfo, error) {
	var out QueueInfo
	if err := c.call(ctx, method, queuePath(queue, rest), body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
