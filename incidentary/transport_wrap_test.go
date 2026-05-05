package incidentary

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func newTestClient() *Client {
	cfg := DefaultConfig("test-key", "test-service")
	cfg.BaseURL = "http://localhost:9999" // unused, just to avoid warnings
	return New(cfg)
}

func TestWrapTransportInjectsHeadersWithContext(t *testing.T) {
	var gotTraceID, gotParentCe string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotTraceID = r.Header.Get(TraceIDHeader)
		gotParentCe = r.Header.Get(ParentCeHeader)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := newTestClient()
	wrapped := WrapTransport(client, http.DefaultTransport)

	ctx := setTraceContext(context.Background(), "trace-abc", "parent-ce-xyz")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/api/test", nil)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := wrapped.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if gotTraceID != "trace-abc" {
		t.Fatalf("expected trace-id header 'trace-abc', got %q", gotTraceID)
	}
	if gotParentCe == "" {
		t.Fatal("expected parent-ce header to be set (new ceID), got empty")
	}
	// parentCe should be a newly generated UUID, not the parent from context
	if gotParentCe == "parent-ce-xyz" {
		t.Fatal("expected parent-ce header to be a new ceID, not the context ceID")
	}
}

func TestWrapTransportNoHeadersWithoutContext(t *testing.T) {
	var gotTraceID, gotParentCe string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotTraceID = r.Header.Get(TraceIDHeader)
		gotParentCe = r.Header.Get(ParentCeHeader)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := newTestClient()
	wrapped := WrapTransport(client, http.DefaultTransport)

	// No trace context on this request.
	req, err := http.NewRequest(http.MethodGet, server.URL+"/api/test", nil)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := wrapped.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if gotTraceID != "" {
		t.Fatalf("expected no trace-id header without context, got %q", gotTraceID)
	}
	if gotParentCe != "" {
		t.Fatalf("expected no parent-ce header without context, got %q", gotParentCe)
	}
}

func TestWrapTransportRecordsResponseStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot) // 418
	}))
	defer server.Close()

	client := newTestClient()
	wrapped := WrapTransport(client, http.DefaultTransport)

	ctx := setTraceContext(context.Background(), "trace-status", "ce-status")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/status", nil)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := wrapped.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	// Flush the buffer to check recorded events.
	events := client.buffer.Flush(time.Now().UnixMilli())
	if len(events) == 0 {
		t.Fatal("expected at least one event in the buffer")
	}

	last := events[len(events)-1]
	if last.StatusCode != http.StatusTeapot {
		t.Fatalf("expected status 418, got %d", last.StatusCode)
	}
	if last.Kind != KindHTTPOut {
		t.Fatalf("expected kind HTTP_OUT, got %s", last.Kind)
	}
	if last.TraceID != "trace-status" {
		t.Fatalf("expected trace-id 'trace-status', got %q", last.TraceID)
	}
}

func TestWrapTransportRecordsStatusZeroOnError(t *testing.T) {
	// Use a transport that always errors.
	failTransport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return nil, errors.New("simulated network failure")
	})

	client := newTestClient()
	wrapped := WrapTransport(client, failTransport)

	ctx := setTraceContext(context.Background(), "trace-err", "ce-err")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://unreachable.test/fail", nil)
	if err != nil {
		t.Fatal(err)
	}

	resp, roundTripErr := wrapped.RoundTrip(req)
	if roundTripErr == nil {
		t.Fatal("expected an error from RoundTrip")
	}
	if resp != nil {
		t.Fatal("expected nil response on error")
	}

	events := client.buffer.Flush(time.Now().UnixMilli())
	if len(events) == 0 {
		t.Fatal("expected at least one event in the buffer on error")
	}

	last := events[len(events)-1]
	if last.StatusCode != 0 {
		t.Fatalf("expected status 0 on error, got %d", last.StatusCode)
	}
}

func TestWrapTransportPassthrough(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Test-Marker", "present")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := newTestClient()
	wrapped := WrapTransport(client, http.DefaultTransport)

	ctx := setTraceContext(context.Background(), "trace-pass", "ce-pass")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/passthrough", nil)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := wrapped.RoundTrip(req)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if resp.Header.Get("X-Test-Marker") != "present" {
		t.Fatal("expected response header to pass through")
	}
}

func TestWrapTransportNilBase(t *testing.T) {
	client := newTestClient()
	wrapped := WrapTransport(client, nil)

	// Should not panic; the base defaults to http.DefaultTransport.
	if wrapped == nil {
		t.Fatal("expected non-nil transport")
	}
}

func TestWrapTransportNilClient(t *testing.T) {
	base := http.DefaultTransport
	wrapped := WrapTransport(nil, base)

	// With nil client, should return the base transport directly.
	if wrapped != base {
		t.Fatal("expected WrapTransport(nil, base) to return base")
	}
}

// roundTripFunc adapts a function to the http.RoundTripper interface.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
