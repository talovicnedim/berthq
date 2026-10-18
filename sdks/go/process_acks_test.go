package berthq_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"

	berthq "github.com/talovicnedim/berthq/sdks/go"
)

func TestProcessAcksWithTheNextConsume(t *testing.T) {
	broker := newFakeBroker(t, []berthq.Delivery{fakeDelivery("L1")}, []berthq.Delivery{fakeDelivery("L2")})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	seen := 0
	err := broker.client().Process(ctx, "jobs", func(ctx context.Context, d berthq.Delivery) error {
		seen++
		if seen == 2 {
			cancel()
		}
		return nil
	}, berthq.ProcessOptions{})
	if err != nil {
		t.Fatal(err)
	}

	consumes := broker.sent("/consume")
	if len(consumes) < 2 || consumes[0].Ack != nil || !reflect.DeepEqual(consumes[1].Ack, []string{"L1"}) {
		t.Fatalf("the second consume should carry L1's ack: %+v", consumes)
	}
	acks := broker.sent("/ack")
	if len(acks) != 1 || !reflect.DeepEqual(acks[0].Leases, []string{"L2"}) {
		t.Fatalf("L2 should be acked on its own when Process stops: %+v", acks)
	}
}

func TestProcessNacksFailuresRightAway(t *testing.T) {
	broker := newFakeBroker(t, []berthq.Delivery{fakeDelivery("L1")})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := broker.client().Process(ctx, "jobs", func(ctx context.Context, d berthq.Delivery) error {
		cancel()
		return errors.New("boom")
	}, berthq.ProcessOptions{})
	if err != nil {
		t.Fatal(err)
	}

	nacks := broker.sent("/nack")
	if len(nacks) != 1 || !reflect.DeepEqual(nacks[0].Leases, []string{"L1"}) {
		t.Fatalf("want L1 nacked: %+v", nacks)
	}
	if acks := broker.sent("/ack"); len(acks) != 0 {
		t.Fatalf("a failed message must not be acked: %+v", acks)
	}
}

func TestProcessHandlesABatchThatArrivesAsItStops(t *testing.T) {
	broker := newFakeBroker(t, []berthq.Delivery{fakeDelivery("L1")})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stopOnAnswer := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		resp, err := http.DefaultTransport.RoundTrip(req)
		if err != nil || !strings.HasSuffix(req.URL.Path, "/consume") {
			return resp, err
		}
		// Read the answer before stopping, so the stop can't cut it short.
		data, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		resp.Body = io.NopCloser(bytes.NewReader(data))
		cancel()
		return resp, err
	})
	client := berthq.New(broker.server.URL, berthq.WithHTTPClient(&http.Client{Transport: stopOnAnswer}))
	var handled []string
	err := client.Process(ctx, "jobs", func(ctx context.Context, d berthq.Delivery) error {
		handled = append(handled, d.Lease)
		return nil
	}, berthq.ProcessOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(handled, []string{"L1"}) {
		t.Fatalf("a batch received before the stop must be handled, handled %v", handled)
	}
	acks := broker.sent("/ack")
	if len(acks) != 1 || !reflect.DeepEqual(acks[0].Leases, []string{"L1"}) {
		t.Fatalf("want L1 acked when Process stops: %+v", acks)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
