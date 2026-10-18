package berthq_test

import (
	"context"
	"testing"

	berthq "github.com/talovicnedim/berthq/sdks/go"
)

func TestQueueAdmin(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()
	queue := createQueue(t, client, "admin", berthq.QueueSettingsPatch{})

	updated := must(client.QueuesUpdate(ctx, queue, berthq.QueueSettingsPatch{
		RetentionMs: berthq.Ptr(int64(3_600_000)),
		MaxMessages: berthq.Ptr(100),
	}))(t)
	if updated.Settings.RetentionMs != 3_600_000 || updated.Settings.MaxMessages == nil || *updated.Settings.MaxMessages != 100 {
		t.Fatalf("update: %+v", updated.Settings)
	}
	unlimited := must(client.QueuesUpdate(ctx, queue, berthq.QueueSettingsPatch{MaxMessages: berthq.Ptr(0)}))(t)
	if unlimited.Settings.MaxMessages != nil {
		t.Fatalf("maxMessages 0 should remove the limit: %+v", unlimited.Settings)
	}
	paused := must(client.QueuesPause(ctx, queue))(t)
	if !paused.Settings.Paused {
		t.Fatal("queue should be paused")
	}
	must(client.Publish(ctx, queue, berthq.NewMessage{Body: "a"}))(t)
	none := must(client.Consume(ctx, queue, berthq.ConsumeOptions{}))(t).Messages
	if len(none) != 0 {
		t.Fatal("a paused queue should deliver nothing")
	}
	must(client.QueuesResume(ctx, queue))(t)
	second := must(client.Publish(ctx, queue, berthq.NewMessage{Body: "b"}))(t)

	visible := must(client.MessagesList(ctx, queue, berthq.MessagesListOptions{State: berthq.StateVisible, Limit: 10}))(t)
	if len(visible) != 2 || visible[0].ID != second.ID {
		t.Fatalf("want 2 visible messages, newest first, got %+v", visible)
	}
	older := must(client.MessagesList(ctx, queue, berthq.MessagesListOptions{Before: second.ID}))(t)
	if len(older) != 1 || older[0].ID == second.ID {
		t.Fatalf("want the older message before %s, got %+v", second.ID, older)
	}
	if err := client.MessagesRemove(ctx, queue, second.ID); err != nil {
		t.Fatal(err)
	}
	_, err := client.MessagesGet(ctx, queue, second.ID)
	if !berthq.IsNotFound(err) {
		t.Fatalf("want notFound, got %v", err)
	}
	if purged := must(client.QueuesPurge(ctx, queue))(t); purged != 1 {
		t.Fatalf("want 1 purged, got %d", purged)
	}

	queues := must(client.QueuesList(ctx))(t)
	if !containsQueue(queues, queue) {
		t.Fatal("QueuesList should include the queue")
	}
	stats := must(client.Stats(ctx))(t)
	if stats.Version == "" || stats.Queues < 1 || stats.Disk.Free <= 0 || stats.Disk.Full {
		t.Fatalf("stats: %+v", stats)
	}

	if err := client.QueuesRemove(ctx, queue); err != nil {
		t.Fatal(err)
	}
	_, err = client.QueuesGet(ctx, queue)
	assertKind(t, err, berthq.KindNotFound, 404)
}

func TestInvalidInput(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()
	_, err := client.QueuesCreate(ctx, "not a valid name!", berthq.QueueSettingsPatch{})
	assertKind(t, err, berthq.KindInvalid, 400)

	queue := createQueue(t, client, "invalid", berthq.QueueSettingsPatch{})
	_, err = client.Publish(ctx, queue, berthq.NewMessage{Body: 1, Priority: 10})
	assertKind(t, err, berthq.KindInvalid, 400)
}
