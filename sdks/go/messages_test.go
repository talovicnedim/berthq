package berthq_test

import (
	"context"
	"testing"

	berthq "github.com/talovicnedim/berthq/sdks/go"
)

func TestDedup(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()
	queue := createQueue(t, client, "dedup", berthq.QueueSettingsPatch{})

	first := must(client.Publish(ctx, queue, berthq.NewMessage{Body: 1, DedupID: "order-1"}))(t)
	second := must(client.Publish(ctx, queue, berthq.NewMessage{Body: 2, DedupID: "order-1"}))(t)
	if first.Duplicate || !second.Duplicate || first.ID != second.ID {
		t.Fatalf("want a duplicate of %+v, got %+v", first, second)
	}
	if stored := must(client.QueuesGet(ctx, queue))(t).Stats.Messages; stored != 1 {
		t.Fatalf("want 1 stored message, got %d", stored)
	}
}

func TestBatch(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()
	queue := createQueue(t, client, "batch", berthq.QueueSettingsPatch{})

	published := must(client.PublishBatch(ctx, queue, []berthq.NewMessage{
		{Body: 1}, {Body: 2, Priority: 9}, {Body: 3},
	}))(t)
	if len(published) != 3 {
		t.Fatalf("want 3 results, got %d", len(published))
	}
	deliveries := must(client.Consume(ctx, queue, berthq.ConsumeOptions{Max: 10}))(t).Messages
	if len(deliveries) != 3 || deliveries[0].ID != published[1].ID {
		t.Fatalf("want 3 deliveries, priority 9 first; got %+v", deliveries)
	}
	leases := make([]string, len(deliveries))
	for i, d := range deliveries {
		leases[i] = d.Lease
	}
	if acked := must(client.Ack(ctx, queue, leases))(t); acked.Settled != 3 {
		t.Fatalf("ack: %+v", acked)
	}
}
