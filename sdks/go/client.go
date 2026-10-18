// Package berthq is the Go client for Berthq (https://github.com/talovicnedim/berthq).
//
//	client := berthq.New("http://127.0.0.1:7331")
//	_, err := client.Publish(ctx, "emails", berthq.NewMessage{Body: map[string]any{"user": 123}})
//
// Method names match the HTTP API and the other SDKs: QueuesCreate, Publish, Consume, Ack,
// Nack, ConsumersReplay, Requeue, ... Process runs a worker loop on top of Consume. Types
// mirror the API's request and response bodies (see docs/api.md); durations are milliseconds.
package berthq

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// requestTimeout bounds every request except Consume, which may legitimately wait longer.
const requestTimeout = 30 * time.Second

// idleConnsPerHost keeps enough connections open for many goroutines publishing and consuming
// at once; Go's default of 2 would open and close a connection for most requests.
const idleConnsPerHost = 64

// Client talks to one broker. It is safe for concurrent use.
type Client struct {
	base   string
	http   *http.Client
	apiKey string
}

// Option configures a Client.
type Option func(*Client)

// WithAPIKey sends key with every request (Authorization: Bearer <key>).
func WithAPIKey(key string) Option {
	return func(c *Client) { c.apiKey = key }
}

// WithHTTPClient uses hc instead of the default client, which keeps up to 64 idle connections
// open and speaks HTTP/2 over TLS. Timeouts are set per request through the context, so
// hc.Timeout should be 0 (or longer than the longest Consume wait).
func WithHTTPClient(hc *http.Client) Option {
	return func(c *Client) { c.http = hc }
}

// New returns a client for the broker at url, e.g. "http://127.0.0.1:7331".
func New(url string, opts ...Option) *Client {
	c := &Client{base: strings.TrimRight(url, "/"), http: &http.Client{Transport: newTransport()}}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// newTransport is http.DefaultTransport with more idle connections per host.
func newTransport() *http.Transport {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = idleConnsPerHost
	return transport
}

// URL returns the broker's base URL.
func (c *Client) URL() string {
	return c.base
}

// call sends a JSON request and decodes a JSON answer into out (unless out is nil).
func (c *Client) call(ctx context.Context, method, path string, body, out any) error {
	return c.callTimeout(ctx, requestTimeout, method, path, body, out)
}

func (c *Client) callTimeout(ctx context.Context, timeout time.Duration, method, path string, body, out any) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var reader io.Reader
	if body != nil {
		data, err := encodeJSON(body)
		if err != nil {
			return &Error{Kind: KindHTTP, Message: "could not encode the request: " + err.Error(), Err: err}
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, reader)
	if err != nil {
		return &Error{Kind: KindHTTP, Message: err.Error(), Err: err}
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return c.transportError(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return apiError(resp)
	}
	if out == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return &Error{Kind: KindHTTP, Status: resp.StatusCode, Message: "invalid response from the broker: " + err.Error(), Err: err}
	}
	return nil
}

// encodeJSON is json.Marshal without its HTML escapes: the broker counts a message body's size
// as it was sent, and "<" sent as "\u003c" takes six bytes instead of one.
func encodeJSON(v any) ([]byte, error) {
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(v); err != nil {
		return nil, err
	}
	// Encode ends the value with a newline, which isn't part of it.
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

func (c *Client) transportError(err error) *Error {
	message := fmt.Sprintf("could not reach the broker at %s: %v", c.base, err)
	if errors.Is(err, context.DeadlineExceeded) {
		message = fmt.Sprintf("the broker at %s did not answer in time", c.base)
	}
	return &Error{Kind: KindHTTP, Message: message, Err: err}
}

func apiError(resp *http.Response) *Error {
	var envelope struct {
		Error struct {
			Kind    string `json:"kind"`
			Message string `json:"message"`
		} `json:"error"`
	}
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	_ = json.Unmarshal(data, &envelope)
	e := &Error{Kind: envelope.Error.Kind, Status: resp.StatusCode, Message: envelope.Error.Message}
	if e.Kind == "" {
		e.Kind = KindHTTP
	}
	if e.Message == "" {
		e.Message = "the broker answered " + resp.Status
	}
	return e
}

func queuePath(queue, rest string) string {
	return "/v1/queues/" + url.PathEscape(queue) + rest
}

func consumerPath(queue, consumer, rest string) string {
	return queuePath(queue, "/consumers/"+url.PathEscape(consumer)+rest)
}
