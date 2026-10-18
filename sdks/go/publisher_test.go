package berthq_test

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	berthq "github.com/talovicnedim/berthq/sdks/go"
)

const long = time.Minute

// publishAll starts one Publish per body, one after the other, and returns a function that
// waits for their results.
func publishAll(t *testing.T, p *berthq.Publisher, bodies ...any) func() []error {
	t.Helper()
	start := p.Queued()
	errs := make([]error, len(bodies))
	var wg sync.WaitGroup
	for i, body := range bodies {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = p.Publish(context.Background(), berthq.NewMessage{Body: body})
		}()
		waitQueued(t, p, start+uint64(i)+1)
	}
	return func() []error {
		wg.Wait()
		return errs
	}
}

// waitQueued waits until n messages have been queued, so the order is known.
func waitQueued(t *testing.T, p *berthq.Publisher, n uint64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for p.Queued() < n {
		if time.Now().After(deadline) {
			t.Fatalf("message %d was not queued", n)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestPublisherSendsWhenMaxBatchMessagesWait(t *testing.T) {
	broker := newFakeBroker(t)
	p := broker.client().Publisher("jobs", berthq.PublisherOptions{MaxBatch: 3, Linger: long})
	wait := publishAll(t, p, 0, 1, 2, 3, 4, 5, 6)
	if err := p.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, err := range wait() {
		if err != nil {
			t.Fatal(err)
		}
	}
	want := [][]any{{0.0, 1.0, 2.0}, {3.0, 4.0, 5.0}, {6.0}}
	if got := broker.batches(); !reflect.DeepEqual(got, want) {
		t.Fatalf("batches %v, want %v", got, want)
	}
}

func TestPublisherWaitsLingerForMoreMessages(t *testing.T) {
	broker := newFakeBroker(t)
	p := broker.client().Publisher("jobs", berthq.PublisherOptions{Linger: 40 * time.Millisecond})
	defer p.Close(context.Background())
	started := time.Now()
	wait := publishAll(t, p, "a", "b")
	if got := broker.batches(); len(got) != 0 {
		t.Fatalf("nothing should be sent before the linger, got %v", got)
	}
	wait()
	if elapsed := time.Since(started); elapsed < 35*time.Millisecond {
		t.Fatalf("sent after %v, before the linger", elapsed)
	}
	if got := broker.batches(); !reflect.DeepEqual(got, [][]any{{"a", "b"}}) {
		t.Fatalf("batches %v", got)
	}
}

func TestPublisherCutsBatchesBeforeMaxBytes(t *testing.T) {
	broker := newFakeBroker(t)
	// {"body":"s1"} is 13 bytes, so two small messages fit in 30 and a third does not.
	p := broker.client().Publisher("jobs", berthq.PublisherOptions{MaxBytes: 30, Linger: long})
	big := strings.Repeat("x", 100)
	wait := publishAll(t, p, "s1", "s2", "s3", big, "s4")
	if err := p.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	wait()
	want := [][]any{{"s1", "s2"}, {"s3"}, {big}, {"s4"}}
	if got := broker.batches(); !reflect.DeepEqual(got, want) {
		t.Fatalf("batches %v, want %v", got, want)
	}
}

func TestPublisherKeepsOneBatchInFlightFromManyGoroutines(t *testing.T) {
	broker := newFakeBroker(t)
	broker.latency = 2 * time.Millisecond
	p := broker.client().Publisher("jobs", berthq.PublisherOptions{MaxBatch: 10, Linger: -1})
	var wg sync.WaitGroup
	for i := range 200 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			published, err := p.Publish(context.Background(), berthq.NewMessage{Body: i})
			if err != nil || published.ID != fmt.Sprintf("id-%d", i) {
				t.Errorf("message %d: %+v %v", i, published, err)
			}
		}()
	}
	wg.Wait()
	if err := p.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if inFlight := broker.maxInFlight(); inFlight != 1 {
		t.Fatalf("want one batch in flight, saw %d", inFlight)
	}
}

func TestPublisherSplitsAnInvalidBatchUntilTheBadMessageIsAlone(t *testing.T) {
	broker := newFakeBroker(t)
	p := broker.client().Publisher("jobs", berthq.PublisherOptions{Linger: long})
	wait := publishAll(t, p, "a", "bad", "c")
	if err := p.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	errs := wait()
	if errs[0] != nil || errs[2] != nil {
		t.Fatalf("good messages failed: %v", errs)
	}
	if berthq.ErrorKind(errs[1]) != berthq.KindInvalid {
		t.Fatalf("want invalid for the bad message, got %v", errs[1])
	}
	want := [][]any{{"a", "bad", "c"}, {"a"}, {"bad", "c"}, {"bad"}, {"c"}}
	if got := broker.batches(); !reflect.DeepEqual(got, want) {
		t.Fatalf("batches %v, want %v", got, want)
	}
}

func TestPublisherFailsTheWholeBatchOnOtherErrors(t *testing.T) {
	broker := newFakeBroker(t)
	p := broker.client().Publisher("jobs", berthq.PublisherOptions{Linger: long})
	wait := publishAll(t, p, "x", "full")
	if err := p.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, err := range wait() {
		if berthq.ErrorKind(err) != berthq.KindQueueFull {
			t.Fatalf("want queueFull, got %v", err)
		}
	}
	if got := broker.batches(); !reflect.DeepEqual(got, [][]any{{"x", "full"}}) {
		t.Fatalf("batches %v", got)
	}
}

func TestPublisherCloseFlushesAndRefusesMessages(t *testing.T) {
	broker := newFakeBroker(t)
	p := broker.client().Publisher("jobs", berthq.PublisherOptions{Linger: long})
	wait := publishAll(t, p, "last")
	if err := p.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if errs := wait(); errs[0] != nil {
		t.Fatal(errs[0])
	}
	_, err := p.Publish(context.Background(), berthq.NewMessage{Body: "late"})
	if berthq.ErrorKind(err) != berthq.KindInvalid {
		t.Fatalf("want invalid after Close, got %v", err)
	}
}

func TestPublisherLeavesOutAMessageWhoseContextEnded(t *testing.T) {
	broker := newFakeBroker(t)
	p := broker.client().Publisher("jobs", berthq.PublisherOptions{Linger: long})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := p.Publish(ctx, berthq.NewMessage{Body: "dropped"})
		done <- err
	}()
	waitQueued(t, p, 1)
	cancel()
	if err := <-done; err != context.Canceled {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	wait := publishAll(t, p, "kept")
	if err := p.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	wait()
	if got := broker.batches(); !reflect.DeepEqual(got, [][]any{{"kept"}}) {
		t.Fatalf("batches %v", got)
	}
}

func TestBodiesArePlainUTF8WithoutNeedlessEscapes(t *testing.T) {
	broker := newFakeBroker(t)
	client := broker.client()
	ctx := context.Background()
	message := berthq.NewMessage{Body: map[string]string{"text": "é</>&"}}
	must(client.Publish(ctx, "jobs", message))(t)
	p := client.Publisher("jobs", berthq.PublisherOptions{})
	defer p.Close(ctx)
	must(p.Publish(ctx, message))(t)

	broker.mu.Lock()
	defer broker.mu.Unlock()
	var got []string
	for _, req := range broker.requests {
		got = append(got, req.JSON)
	}
	want := []string{`{"body":{"text":"é</>&"}}`, `{"messages":[{"body":{"text":"é</>&"}}]}`}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("sent %q, want %q", got, want)
	}
}
