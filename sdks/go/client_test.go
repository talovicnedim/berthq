package berthq_test

import (
	"context"
	"os"
	"testing"

	berthq "github.com/talovicnedim/berthq/sdks/go"
)

func TestAuth(t *testing.T) {
	url := os.Getenv("BERTHQ_TEST_AUTH_URL")
	if url == "" {
		t.Skip("BERTHQ_TEST_AUTH_URL is not set")
	}
	ctx := context.Background()
	admin := berthq.New(url, berthq.WithAPIKey(adminKey))
	publisher := berthq.New(url, berthq.WithAPIKey(publishKey))
	queue := uniqueName("auth")

	_, err := berthq.New(url).QueuesList(ctx)
	assertKind(t, err, berthq.KindUnauthorized, 401)
	_, err = berthq.New(url, berthq.WithAPIKey("wrongwrongwrong1")).QueuesList(ctx)
	assertKind(t, err, berthq.KindUnauthorized, 401)

	must(admin.QueuesCreate(ctx, queue, berthq.QueueSettingsPatch{}))(t)
	must(publisher.Publish(ctx, queue, berthq.NewMessage{Body: "hi"}))(t)
	_, err = publisher.QueuesCreate(ctx, uniqueName("auth"), berthq.QueueSettingsPatch{})
	assertKind(t, err, berthq.KindForbidden, 403)
	_, err = publisher.Consume(ctx, queue, berthq.ConsumeOptions{})
	assertKind(t, err, berthq.KindForbidden, 403)

	_, err = publisher.ConsumersCreate(ctx, queue, "billing", "")
	assertKind(t, err, berthq.KindForbidden, 403)

	deliveries := must(admin.Consume(ctx, queue, berthq.ConsumeOptions{}))(t).Messages
	if len(deliveries) != 1 {
		t.Fatalf("want 1 delivery, got %d", len(deliveries))
	}
}

func TestTransportError(t *testing.T) {
	client := berthq.New("http://127.0.0.1:1")
	_, err := client.QueuesList(context.Background())
	assertKind(t, err, berthq.KindHTTP, 0)
}
