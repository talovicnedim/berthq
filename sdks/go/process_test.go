package berthq_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	berthq "github.com/talovicnedim/berthq/sdks/go"
)

func TestProcessAcks(t *testing.T) {
	client := testClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queue := createQueue(t, client, "process-ack", berthq.QueueSettingsPatch{})
	const count = 10
	for i := 0; i < count; i++ {
		must(client.Publish(ctx, queue, berthq.NewMessage{Body: i}))(t)
	}

	var mu sync.Mutex
	seen := map[int]bool{}
	result := make(chan error, 1)
	go func() {
		result <- client.Process(ctx, queue, func(ctx context.Context, d berthq.Delivery) error {
			var n int
			if err := d.Decode(&n); err != nil {
				return err
			}
			mu.Lock()
			defer mu.Unlock()
			seen[n] = true
			if len(seen) == count {
				cancel()
			}
			return nil
		}, berthq.ProcessOptions{Concurrency: 3, Batch: 2, Worker: "go-worker"})
	}()

	waitFor(t, result)
	if len(seen) != count {
		t.Fatalf("want %d handled messages, got %d", count, len(seen))
	}
	stats := must(client.QueuesGet(context.Background(), queue))(t).Stats
	if stats.Backlog != 0 || stats.Counters.Acked != count {
		t.Fatalf("all messages should be acked: %+v", stats)
	}
}

func TestProcessNacks(t *testing.T) {
	client := testClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queue := createQueue(t, client, "process-nack", berthq.QueueSettingsPatch{MaxAttempts: berthq.Ptr(1)})
	must(client.Publish(ctx, queue, berthq.NewMessage{Body: "fails"}))(t)

	result := make(chan error, 1)
	go func() {
		result <- client.Process(ctx, queue, func(ctx context.Context, d berthq.Delivery) error {
			defer cancel()
			return errors.New("boom")
		}, berthq.ProcessOptions{})
	}()
	waitFor(t, result)

	dead := must(client.MessagesList(context.Background(), queue+".dlq", berthq.MessagesListOptions{}))(t)
	if len(dead) != 1 || dead[0].LastError == nil || *dead[0].LastError != "boom" {
		t.Fatalf("want one dead letter with lastError boom, got %+v", dead)
	}
}

func TestProcessExtendsLongHandlers(t *testing.T) {
	client := testClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queue := createQueue(t, client, "process-extend", berthq.QueueSettingsPatch{VisibilityTimeoutMs: berthq.Ptr(int64(500))})
	must(client.Publish(ctx, queue, berthq.NewMessage{Body: "slow"}))(t)

	result := make(chan error, 1)
	go func() {
		result <- client.Process(ctx, queue, func(ctx context.Context, d berthq.Delivery) error {
			defer cancel()
			time.Sleep(2500 * time.Millisecond) // five times the lease
			return nil
		}, berthq.ProcessOptions{})
	}()
	waitFor(t, result)

	stats := must(client.QueuesGet(context.Background(), queue))(t).Stats
	if stats.Backlog != 0 || stats.Counters.Retried != 0 || stats.Counters.Acked != 1 {
		t.Fatalf("the lease should have been kept alive: %+v", stats)
	}
}

func TestProcessAutoAckReportsHandlerErrors(t *testing.T) {
	client := testClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queue := createQueue(t, client, "process-autoack", berthq.QueueSettingsPatch{})
	must(client.Publish(ctx, queue, berthq.NewMessage{Body: "fails"}))(t)

	var mu sync.Mutex
	var reported []error
	result := make(chan error, 1)
	go func() {
		result <- client.Process(ctx, queue, func(ctx context.Context, d berthq.Delivery) error {
			defer cancel()
			return errors.New("boom")
		}, berthq.ProcessOptions{AutoAck: true, Consumer: "metrics", OnError: func(err error) {
			mu.Lock()
			defer mu.Unlock()
			reported = append(reported, err)
		}})
	}()
	waitFor(t, result)

	if len(reported) != 1 || !strings.Contains(reported[0].Error(), "boom") {
		t.Fatalf("want the handler error reported once, got %v", reported)
	}
	stats := must(client.ConsumersGet(context.Background(), queue, "metrics"))(t).Stats
	if stats != (berthq.ConsumerStats{}) {
		t.Fatalf("nothing should be retried in auto-ack mode: %+v", stats)
	}
}

func TestProcessReturnsConsumeErrors(t *testing.T) {
	client := testClient(t)
	err := client.Process(context.Background(), uniqueName("missing"), func(context.Context, berthq.Delivery) error {
		return nil
	}, berthq.ProcessOptions{})
	assertKind(t, err, berthq.KindNotFound, 404)
}
