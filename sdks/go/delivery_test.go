package berthq_test

import (
	"context"
	"testing"

	berthq "github.com/talovicnedim/berthq/sdks/go"
)

func TestRoundTrip(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()
	queue := createQueue(t, client, "roundtrip", berthq.QueueSettingsPatch{MaxAttempts: berthq.Ptr(3)})

	_, err := client.QueuesCreate(ctx, queue, berthq.QueueSettingsPatch{})
	assertKind(t, err, berthq.KindConflict, 409)
	if !berthq.IsConflict(err) {
		t.Fatal("IsConflict should match")
	}

	published := must(client.Publish(ctx, queue, berthq.NewMessage{
		Body:     map[string]any{"user": 123},
		Priority: 5,
		DedupID:  "user-123",
	}))(t)
	if published.ID == "" || published.Duplicate {
		t.Fatalf("unexpected publish result %+v", published)
	}

	info := must(client.MessagesGet(ctx, queue, published.ID))(t)
	if info.State != berthq.StateVisible || string(info.Body) != `{"user":123}` {
		t.Fatalf("unexpected message %+v body %s", info, info.Body)
	}

	deliveries := must(client.Consume(ctx, queue, berthq.ConsumeOptions{Max: 10, Worker: "go-test"}))(t).Messages
	if len(deliveries) != 1 {
		t.Fatalf("want 1 delivery, got %d", len(deliveries))
	}
	d := deliveries[0]
	var body struct{ User int }
	if err := d.Decode(&body); err != nil || body.User != 123 {
		t.Fatalf("decode: %v %+v", err, body)
	}
	if d.ID != published.ID || d.Consumer != "default" || d.Attempt != 1 || d.VisibilityTimeoutMs != 30_000 || d.Priority != 5 || d.LastError != nil {
		t.Fatalf("unexpected delivery %+v", d)
	}
	if d.Lease == "" || d.LeaseExpiresAt == nil || d.DedupID == nil || *d.DedupID != "user-123" {
		t.Fatalf("unexpected lease or dedup id %+v", d)
	}

	extended := must(client.Extend(ctx, queue, []string{d.Lease}, 60_000))(t)
	if extended.Settled != 1 {
		t.Fatalf("extend: %+v", extended)
	}
	acked := must(client.Ack(ctx, queue, []string{d.Lease}))(t)
	if acked.Settled != 1 || len(acked.Unknown) != 0 {
		t.Fatalf("ack: %+v", acked)
	}
	again := must(client.Ack(ctx, queue, []string{d.Lease}))(t)
	if again.Settled != 0 || len(again.Unknown) != 1 {
		t.Fatalf("second ack: %+v", again)
	}

	details := must(client.QueuesGet(ctx, queue))(t)
	if details.Name != queue || details.Stats.Backlog != 0 || details.Stats.Messages != 1 || details.Stats.Counters.Acked != 1 {
		t.Fatalf("unexpected queue %+v", details.QueueInfo)
	}
	if len(details.Consumers) != 1 || details.Consumers[0].Name != "default" {
		t.Fatalf("want the default consumer, got %+v", details.Consumers)
	}
	if details.Settings.MaxAttempts != 3 || details.DeadLetterQueue != nil {
		t.Fatalf("unexpected settings %+v", details.Settings)
	}
}

func TestNackRetryDeadLetterRequeue(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()
	queue := createQueue(t, client, "retry", berthq.QueueSettingsPatch{
		MaxAttempts:  berthq.Ptr(2),
		RetryDelayMs: berthq.Ptr(int64(0)),
	})
	dlq := queue + ".dlq"
	published := must(client.Publish(ctx, queue, berthq.NewMessage{Body: "job"}))(t)

	first := consumeOne(t, client, queue)
	if first.ID != published.ID {
		t.Fatalf("want %s, got %+v", published.ID, first)
	}
	must(client.Nack(ctx, queue, berthq.NackOptions{Leases: []string{first.Lease}, Error: "smtp timeout"}))(t)

	second := consumeOne(t, client, queue)
	if second.Attempt != 2 || second.LastError == nil || *second.LastError != "smtp timeout" {
		t.Fatalf("unexpected retry %+v", second)
	}
	must(client.Nack(ctx, queue, berthq.NackOptions{Leases: []string{second.Lease}, Error: "smtp timeout again"}))(t)

	// A dead letter is a copy, so it is looked up in the DLQ rather than by the original id.
	dead := must(client.MessagesList(ctx, dlq, berthq.MessagesListOptions{}))(t)
	if len(dead) != 1 || dead[0].Attempts != 2 || dead[0].LastError == nil || *dead[0].LastError != "smtp timeout again" {
		t.Fatalf("want the message in %s, got %+v", dlq, dead)
	}
	if dl := dead[0].DeadLetter; dl == nil || dl.Reason != "maxAttempts" || dl.Queue != queue || dl.Consumer != "default" || dl.MessageID != published.ID {
		t.Fatalf("unexpected dead letter %+v", dead[0].DeadLetter)
	}

	requeued := must(client.Requeue(ctx, dlq, nil))(t)
	if requeued.Requeued != 1 || requeued.Skipped != 0 {
		t.Fatalf("requeue: %+v", requeued)
	}
	back := consumeOne(t, client, queue)
	if back.ID != published.ID || string(back.Body) != `"job"` || back.Attempt != 1 {
		t.Fatalf("unexpected requeued delivery %+v", back)
	}
	must(client.Nack(ctx, queue, berthq.NackOptions{Leases: []string{back.Lease}, DeadLetter: true}))(t)
	dead = must(client.MessagesList(ctx, dlq, berthq.MessagesListOptions{}))(t)
	if len(dead) != 1 || dead[0].DeadLetter.Reason != "rejected" {
		t.Fatalf("want a rejected dead letter, got %+v", dead)
	}
}

func TestAutoAck(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()
	queue := createQueue(t, client, "autoack", berthq.QueueSettingsPatch{})
	must(client.PublishBatch(ctx, queue, []berthq.NewMessage{{Body: 1}, {Body: 2}}))(t)

	deliveries := must(client.Consume(ctx, queue, berthq.ConsumeOptions{Max: 10, AutoAck: true}))(t).Messages
	if len(deliveries) != 2 || deliveries[0].Lease != "" || deliveries[0].LeaseExpiresAt != nil || deliveries[0].VisibilityTimeoutMs != 0 {
		t.Fatalf("want 2 deliveries without leases, got %+v", deliveries)
	}
	stats := must(client.ConsumersGet(ctx, queue, "default"))(t).Stats
	if stats != (berthq.ConsumerStats{}) {
		t.Fatalf("an auto-acked consumer holds nothing: %+v", stats)
	}
}
