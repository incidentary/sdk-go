package incidentary

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestWrapLambdaHandlerProvidesTraceContext(t *testing.T) {
	client := newTestClient()

	var capturedTraceID, capturedCeID string
	handler := func(ctx context.Context, event json.RawMessage) (interface{}, error) {
		capturedTraceID, capturedCeID = ContextTrace(ctx)
		return map[string]string{"ok": "true"}, nil
	}

	wrapped := WrapLambdaHandler(client, handler)
	result, err := wrapped(context.Background(), json.RawMessage(`{"test":true}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}

	if capturedTraceID == "" {
		t.Fatal("expected traceID to be set in context")
	}
	if capturedCeID == "" {
		t.Fatal("expected ceID to be set in context")
	}
}

func TestWrapLambdaHandlerFlushCalledOnSuccess(t *testing.T) {
	client := newTestClient()

	// Write an event so the buffer is non-empty before the handler runs.
	client.WriteEvent(&SkeletonCe{
		CeID:       "pre-event",
		TraceID:    "trace-pre",
		ServiceID:  "test-service",
		WallTsNs:   1_000_000_000,
		Kind:       KindHTTPIn,
		StatusCode: 200,
	})

	handler := func(ctx context.Context, event json.RawMessage) (interface{}, error) {
		return "done", nil
	}

	wrapped := WrapLambdaHandler(client, handler)
	_, err := wrapped(context.Background(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// After WrapLambdaHandler completes, buffer should have been flushed.
	// The buffer's count should be 0 since FlushToBackend drains it.
	client.mu.Lock()
	count := client.buffer.count
	client.mu.Unlock()
	if count != 0 {
		t.Fatalf("expected buffer to be flushed (count=0), got count=%d", count)
	}
}

func TestWrapLambdaHandlerFlushCalledOnError(t *testing.T) {
	client := newTestClient()

	// Write an event so the buffer is non-empty.
	client.WriteEvent(&SkeletonCe{
		CeID:       "pre-event-err",
		TraceID:    "trace-pre-err",
		ServiceID:  "test-service",
		WallTsNs:   1_000_000_000,
		Kind:       KindHTTPIn,
		StatusCode: 500,
	})

	handlerErr := errors.New("lambda handler failed")
	handler := func(ctx context.Context, event json.RawMessage) (interface{}, error) {
		return nil, handlerErr
	}

	wrapped := WrapLambdaHandler(client, handler)
	_, err := wrapped(context.Background(), json.RawMessage(`{}`))
	if err != handlerErr {
		t.Fatalf("expected handler error to be returned, got %v", err)
	}

	// Buffer should still have been flushed despite the error.
	client.mu.Lock()
	count := client.buffer.count
	client.mu.Unlock()
	if count != 0 {
		t.Fatalf("expected buffer to be flushed on error (count=0), got count=%d", count)
	}
}

func TestWrapLambdaHandlerNilClientDoesNotPanic(t *testing.T) {
	handler := func(ctx context.Context, event json.RawMessage) (interface{}, error) {
		return "ok", nil
	}

	wrapped := WrapLambdaHandler(nil, handler)
	result, err := wrapped(context.Background(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "ok" {
		t.Fatalf("expected 'ok', got %v", result)
	}
}
