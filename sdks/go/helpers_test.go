package berthq_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	berthq "github.com/talovicnedim/berthq/sdks/go"
)

// Most tests run against a real broker. BERTHQ_TEST_URL points at one started without API
// keys; BERTHQ_TEST_AUTH_URL at one started with
// --api-key adminadminadmin1 --api-key publish:publishpublish12.

const (
	adminKey   = "adminadminadmin1"
	publishKey = "publishpublish12"
)

func testClient(t *testing.T) *berthq.Client {
	t.Helper()
	url := os.Getenv("BERTHQ_TEST_URL")
	if url == "" {
		t.Skip("BERTHQ_TEST_URL is not set")
	}
	return berthq.New(url)
}

var counter atomic.Int64

// uniqueName keeps tests (and repeated runs against the same broker) from sharing queues.
func uniqueName(prefix string) string {
	return fmt.Sprintf("go-%s-%d-%d", prefix, time.Now().UnixNano(), counter.Add(1))
}

func createQueue(t *testing.T, client *berthq.Client, prefix string, settings berthq.QueueSettingsPatch) string {
	t.Helper()
	name := uniqueName(prefix)
	if _, err := client.QueuesCreate(context.Background(), name, settings); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	return name
}

// must turns a (value, error) result into the value, failing the test on an error:
// must(client.Publish(...))(t).
func must[T any](v T, err error) func(t *testing.T) T {
	return func(t *testing.T) T {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
}

func assertKind(t *testing.T, err error, kind string, status int) {
	t.Helper()
	var berr *berthq.Error
	if !errors.As(err, &berr) {
		t.Fatalf("want *berthq.Error %s, got %v", kind, err)
	}
	if berr.Kind != kind || berr.Status != status {
		t.Fatalf("want %s/%d, got %s/%d (%s)", kind, status, berr.Kind, berr.Status, berr.Message)
	}
}

// consumeOne long-polls for exactly one delivery.
func consumeOne(t *testing.T, client *berthq.Client, queue string) berthq.Delivery {
	t.Helper()
	deliveries := must(client.Consume(context.Background(), queue, berthq.ConsumeOptions{WaitMs: 5_000}))(t).Messages
	if len(deliveries) != 1 {
		t.Fatalf("want 1 delivery from %s, got %d", queue, len(deliveries))
	}
	return deliveries[0]
}

// drain consumes and acks everything a consumer has right now and returns the bodies, joined
// with commas.
func drain(t *testing.T, client *berthq.Client, queue, consumer string) string {
	t.Helper()
	ctx := context.Background()
	deliveries := must(client.Consume(ctx, queue, berthq.ConsumeOptions{Consumer: consumer, Max: 100}))(t).Messages
	bodies := make([]string, len(deliveries))
	leases := make([]string, len(deliveries))
	for i, d := range deliveries {
		bodies[i] = string(d.Body)
		leases[i] = d.Lease
	}
	if len(leases) > 0 {
		must(client.Ack(ctx, queue, leases))(t)
	}
	return strings.Join(bodies, ",")
}

func containsQueue(queues []berthq.QueueInfo, name string) bool {
	for _, q := range queues {
		if q.Name == name {
			return true
		}
	}
	return false
}

func hasWorker(workers []berthq.WorkerInfo, name string) bool {
	for _, w := range workers {
		if w.Name == name {
			return true
		}
	}
	return false
}

func waitFor(t *testing.T, result chan error) {
	t.Helper()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Process did not return")
	}
}
