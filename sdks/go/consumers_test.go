package berthq_test

import (
	"context"
	"testing"

	berthq "github.com/talovicnedim/berthq/sdks/go"
)

func TestConsumersFanOutAndReplay(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()
	queue := createQueue(t, client, "fanout", berthq.QueueSettingsPatch{})
	first := must(client.Publish(ctx, queue, berthq.NewMessage{Body: "first"}))(t)
	must(client.Publish(ctx, queue, berthq.NewMessage{Body: "second"}))(t)

	if got := drain(t, client, queue, "billing"); got != `"first","second"` {
		t.Fatalf("billing got %s", got)
	}
	if got := drain(t, client, queue, "audit"); got != `"first","second"` {
		t.Fatalf("audit should get every message too, got %s", got)
	}
	if got := drain(t, client, queue, "billing"); got != "" {
		t.Fatalf("billing should be done, got %s", got)
	}
	consumers := must(client.ConsumersList(ctx, queue))(t)
	if len(consumers) != 2 || consumers[0].Name != "audit" || consumers[1].Name != "billing" {
		t.Fatalf("want audit and billing, got %+v", consumers)
	}

	none := must(client.Consume(ctx, queue, berthq.ConsumeOptions{Consumer: "late", From: berthq.End}))(t).Messages
	if len(none) != 0 {
		t.Fatalf("a consumer starting at the end should get nothing, got %d", len(none))
	}
	replayed := must(client.ConsumersReplay(ctx, queue, "late", berthq.Start))(t)
	if replayed.Stats.Waiting != 2 {
		t.Fatalf("want 2 waiting after a replay, got %+v", replayed.Stats)
	}
	if got := drain(t, client, queue, "late"); got != `"first","second"` {
		t.Fatalf("replay from start got %s", got)
	}
	must(client.ConsumersReplay(ctx, queue, "late", first.ID))(t)
	if got := drain(t, client, queue, "late"); got != `"first","second"` {
		t.Fatalf("replay from a message id got %s", got)
	}
}

func TestConsumerAdmin(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()
	queue := createQueue(t, client, "consumers", berthq.QueueSettingsPatch{})

	created := must(client.ConsumersCreate(ctx, queue, "billing", berthq.End))(t)
	if created.Name != "billing" || created.Queue != queue || created.Paused || created.Idle || created.LastSeen != nil {
		t.Fatalf("unexpected consumer %+v", created)
	}
	_, err := client.ConsumersCreate(ctx, queue, "billing", "")
	assertKind(t, err, berthq.KindConflict, 409)

	if paused := must(client.ConsumersPause(ctx, queue, "billing"))(t); !paused.Paused {
		t.Fatal("consumer should be paused")
	}
	must(client.Publish(ctx, queue, berthq.NewMessage{Body: "held"}))(t)
	if held := must(client.Consume(ctx, queue, berthq.ConsumeOptions{Consumer: "billing"}))(t).Messages; len(held) != 0 {
		t.Fatal("a paused consumer should get nothing")
	}
	must(client.ConsumersResume(ctx, queue, "billing"))(t)
	deliveries := must(client.Consume(ctx, queue, berthq.ConsumeOptions{Consumer: "billing", Worker: "b1"}))(t).Messages
	if len(deliveries) != 1 || deliveries[0].Consumer != "billing" {
		t.Fatalf("want the held message, got %+v", deliveries)
	}
	got := must(client.ConsumersGet(ctx, queue, "billing"))(t)
	if got.LastSeen == nil || !hasWorker(got.Workers, "b1") || got.Stats.Processing != 1 {
		t.Fatalf("unexpected consumer %+v", got)
	}
	processing := must(client.ConsumersMessages(ctx, queue, "billing", berthq.ConsumerMessagesOptions{State: berthq.ConsumerProcessing}))(t)
	if len(processing) != 1 || processing[0].Worker == nil || *processing[0].Worker != "b1" || processing[0].LeaseExpiresAt == nil {
		t.Fatalf("unexpected processing messages %+v", processing)
	}

	if err := client.ConsumersRemove(ctx, queue, "billing"); err != nil {
		t.Fatal(err)
	}
	_, err = client.ConsumersGet(ctx, queue, "billing")
	assertKind(t, err, berthq.KindNotFound, 404)
	acked := must(client.Ack(ctx, queue, []string{deliveries[0].Lease}))(t)
	if len(acked.Unknown) != 1 {
		t.Fatalf("a lease from before the removal should be unknown, got %+v", acked)
	}
}
