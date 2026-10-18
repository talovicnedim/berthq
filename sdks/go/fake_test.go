package berthq_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	berthq "github.com/talovicnedim/berthq/sdks/go"
)

// fakeBroker answers like the broker for the calls these tests make. A batch with a "bad" body
// is invalid; one with a "full" body hits queueFull.
type fakeBroker struct {
	server   *httptest.Server
	latency  time.Duration
	mu       sync.Mutex
	requests []fakeRequest
	consumes [][]berthq.Delivery
	inFlight int
	maxIn    int
}

type fakeRequest struct {
	Path     string
	Messages []struct{ Body any }
	Ack      []string
	Leases   []string
	// JSON is the body as it was sent.
	JSON string `json:"-"`
}

func newFakeBroker(t *testing.T, consumes ...[]berthq.Delivery) *fakeBroker {
	t.Helper()
	f := &fakeBroker{consumes: consumes}
	f.server = httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.server.Close)
	return f
}

func (f *fakeBroker) client() *berthq.Client {
	return berthq.New(f.server.URL)
}

func (f *fakeBroker) handle(w http.ResponseWriter, r *http.Request) {
	var req fakeRequest
	data, _ := io.ReadAll(r.Body)
	_ = json.Unmarshal(data, &req)
	req.Path = r.URL.Path
	req.JSON = string(data)
	f.mu.Lock()
	f.requests = append(f.requests, req)
	f.inFlight++
	f.maxIn = max(f.maxIn, f.inFlight)
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		f.inFlight--
		f.mu.Unlock()
	}()
	time.Sleep(f.latency)

	switch {
	case strings.HasSuffix(req.Path, "/batch"):
		f.batch(w, req)
	case strings.HasSuffix(req.Path, "/messages"):
		reply(w, http.StatusCreated, berthq.Published{ID: "id-1"})
	case strings.HasSuffix(req.Path, "/consume"):
		f.mu.Lock()
		var messages []berthq.Delivery
		if len(f.consumes) > 0 {
			messages, f.consumes = f.consumes[0], f.consumes[1:]
		}
		f.mu.Unlock()
		reply(w, http.StatusOK, map[string]any{"messages": messages, "settled": len(req.Ack), "unknown": []string{}})
	default:
		reply(w, http.StatusOK, map[string]any{"settled": len(req.Leases), "unknown": []string{}})
	}
}

func (f *fakeBroker) batch(w http.ResponseWriter, req fakeRequest) {
	results := make([]berthq.Published, 0, len(req.Messages))
	for _, m := range req.Messages {
		switch m.Body {
		case "bad":
			reply(w, http.StatusBadRequest, apiError("invalid", "body bad is not allowed"))
			return
		case "full":
			reply(w, 507, apiError("queueFull", "queue is full"))
			return
		}
		results = append(results, berthq.Published{ID: fmt.Sprintf("id-%v", m.Body)})
	}
	reply(w, http.StatusCreated, map[string]any{"messages": results})
}

// batches returns the bodies of every batch request, in order.
func (f *fakeBroker) batches() [][]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out [][]any
	for _, req := range f.requests {
		if !strings.HasSuffix(req.Path, "/batch") {
			continue
		}
		bodies := make([]any, len(req.Messages))
		for i, m := range req.Messages {
			bodies[i] = m.Body
		}
		out = append(out, bodies)
	}
	return out
}

func (f *fakeBroker) maxInFlight() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.maxIn
}

// sent returns the requests made to paths ending in suffix.
func (f *fakeBroker) sent(suffix string) []fakeRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []fakeRequest
	for _, req := range f.requests {
		if strings.HasSuffix(req.Path, suffix) {
			out = append(out, req)
		}
	}
	return out
}

func reply(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func apiError(kind, message string) map[string]any {
	return map[string]any{"error": map[string]string{"kind": kind, "message": message}}
}

func fakeDelivery(lease string) berthq.Delivery {
	return berthq.Delivery{
		ID:                  "id-" + lease,
		Lease:               lease,
		Queue:               "jobs",
		Consumer:            "default",
		Body:                json.RawMessage(fmt.Sprintf("%q", lease)),
		Attempt:             1,
		PublishedAt:         time.Now(),
		VisibilityTimeoutMs: 30_000,
	}
}
