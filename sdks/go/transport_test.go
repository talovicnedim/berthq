package berthq

import (
	"net/http"
	"testing"
)

func TestNewKeepsConnectionsOpenAndSpeaksHTTP2(t *testing.T) {
	transport, ok := New("http://127.0.0.1:7331").http.Transport.(*http.Transport)
	if !ok {
		t.Fatal("want an *http.Transport")
	}
	if transport.MaxIdleConnsPerHost < 64 || !transport.ForceAttemptHTTP2 {
		t.Fatalf("idle per host %d, http2 %v", transport.MaxIdleConnsPerHost, transport.ForceAttemptHTTP2)
	}
}
