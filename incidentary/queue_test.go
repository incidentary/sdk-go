package incidentary

import (
	"context"
	"errors"
	"testing"
	"time"
)

// --- InjectQueueContext ---

func TestInjectQueueContextAddsTraceHeadersToEmptyMap(t *testing.T) {
	traceID := "trace-abc"
	ceID := "ce-xyz"
	ctx := setTraceContext(context.Background(), traceID, ceID)

	out := InjectQueueContext(ctx, QueueHeaders{})

	if out[QueueHeaderTraceID] != traceID {
		t.Fatalf("expected trace header %q, got %q", traceID, out[QueueHeaderTraceID])
	}
	if out[QueueHeaderParentCe] != ceID {
		t.Fatalf("expected parent-ce header %q, got %q", ceID, out[QueueHeaderParentCe])
	}
}

func TestInjectQueueContextAddsTraceHeadersToExistingMap(t *testing.T) {
	ctx := setTraceContext(context.Background(), "trace-123", "ce-456")
	input := QueueHeaders{
		"x-custom-header": "custom-value",
		"another":         "header",
	}

	out := InjectQueueContext(ctx, input)

	// Trace headers injected.
	if out[QueueHeaderTraceID] != "trace-123" {
		t.Fatalf("expected trace-id header, got %q", out[QueueHeaderTraceID])
	}
	if out[QueueHeaderParentCe] != "ce-456" {
		t.Fatalf("expected parent-ce header, got %q", out[QueueHeaderParentCe])
	}
	// Existing headers preserved.
	if out["x-custom-header"] != "custom-value" {
		t.Fatal("expected existing headers to be preserved")
	}
	if out["another"] != "header" {
		t.Fatal("expected existing headers to be preserved")
	}
}

func TestInjectQueueContextDoesNotMutateInput(t *testing.T) {
	ctx := setTraceContext(context.Background(), "trace-mut", "ce-mut")
	input := QueueHeaders{"existing": "value"}

	_ = InjectQueueContext(ctx, input)

	// Input must not be mutated.
	if _, ok := input[QueueHeaderTraceID]; ok {
		t.Fatal("expected InjectQueueContext to not mutate the input map")
	}
}

func TestInjectQueueContextWithNoTraceContextReturnsInputUnchanged(t *testing.T) {
	// Context has no trace info — both IDs will be empty.
	ctx := context.Background()
	input := QueueHeaders{"key": "val"}

	out := InjectQueueContext(ctx, input)

	// Should not add empty-string headers.
	if out[QueueHeaderTraceID] != "" {
		t.Fatalf("expected no trace header when context has no trace, got %q", out[QueueHeaderTraceID])
	}
	if out[QueueHeaderParentCe] != "" {
		t.Fatalf("expected no parent-ce header when context has no trace, got %q", out[QueueHeaderParentCe])
	}
	// Existing key still present.
	if out["key"] != "val" {
		t.Fatal("expected existing headers to remain when no trace context")
	}
}

func TestInjectQueueContextWithNilInputUsesEmptyMap(t *testing.T) {
	ctx := setTraceContext(context.Background(), "t1", "c1")

	// nil input must not panic.
	out := InjectQueueContext(ctx, nil)

	if out[QueueHeaderTraceID] != "t1" {
		t.Fatalf("expected trace-id 't1', got %q", out[QueueHeaderTraceID])
	}
}

// --- ExtractQueueContext ---

func TestExtractQueueContextSetsTraceContextOnReturnedContext(t *testing.T) {
	headers := QueueHeaders{
		QueueHeaderTraceID:  "extracted-trace",
		QueueHeaderParentCe: "extracted-ce",
	}

	ctx := ExtractQueueContext(context.Background(), headers)

	gotTrace, gotCe := ContextTrace(ctx)
	if gotTrace != "extracted-trace" {
		t.Fatalf("expected traceID 'extracted-trace', got %q", gotTrace)
	}
	if gotCe != "extracted-ce" {
		t.Fatalf("expected ceID 'extracted-ce', got %q", gotCe)
	}
}

func TestExtractQueueContextWithMissingHeadersReturnsOriginalContext(t *testing.T) {
	// No trace headers in the map.
	headers := QueueHeaders{"unrelated": "header"}

	base := context.Background()
	ctx := ExtractQueueContext(base, headers)

	gotTrace, gotCe := ContextTrace(ctx)
	if gotTrace != "" {
		t.Fatalf("expected empty traceID with missing headers, got %q", gotTrace)
	}
	if gotCe != "" {
		t.Fatalf("expected empty ceID with missing headers, got %q", gotCe)
	}
}

func TestExtractQueueContextWithNilHeadersDoesNotPanic(t *testing.T) {
	// Must not panic with nil input.
	ctx := ExtractQueueContext(context.Background(), nil)

	gotTrace, _ := ContextTrace(ctx)
	if gotTrace != "" {
		t.Fatalf("expected empty traceID with nil headers, got %q", gotTrace)
	}
}

func TestExtractQueueContextDoesNotMutateParentContext(t *testing.T) {
	headers := QueueHeaders{
		QueueHeaderTraceID:  "trace-new",
		QueueHeaderParentCe: "ce-new",
	}

	parent := context.Background()
	_ = ExtractQueueContext(parent, headers)

	// Parent context must be unchanged.
	gotTrace, _ := ContextTrace(parent)
	if gotTrace != "" {
		t.Fatal("expected parent context to be unaffected by ExtractQueueContext")
	}
}

// --- WrapQueueConsumer ---

func TestWrapQueueConsumerRunsHandlerWithTraceContextFromHeaders(t *testing.T) {
	client := newTestClient()

	var capturedTraceID, capturedCeID string
	handler := func(ctx context.Context, headers QueueHeaders, body []byte) error {
		capturedTraceID, capturedCeID = ContextTrace(ctx)
		return nil
	}

	wrapped := WrapQueueConsumer(client, handler)

	headers := QueueHeaders{
		QueueHeaderTraceID:  "queue-trace",
		QueueHeaderParentCe: "queue-ce",
	}
	err := wrapped(context.Background(), headers, []byte("payload"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if capturedTraceID != "queue-trace" {
		t.Fatalf("expected traceID 'queue-trace', got %q", capturedTraceID)
	}
	if capturedCeID != "queue-ce" {
		t.Fatalf("expected ceID 'queue-ce', got %q", capturedCeID)
	}
}

func TestWrapQueueConsumerRecordsQueueConsumeEvent(t *testing.T) {
	client := newTestClient()

	handler := func(ctx context.Context, headers QueueHeaders, body []byte) error {
		return nil
	}

	wrapped := WrapQueueConsumer(client, handler)
	headers := QueueHeaders{
		QueueHeaderTraceID:  "event-trace",
		QueueHeaderParentCe: "event-ce",
	}
	if err := wrapped(context.Background(), headers, []byte("data")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	events := client.buffer.Flush(time.Now().UnixMilli())
	if len(events) == 0 {
		t.Fatal("expected at least one event recorded")
	}

	last := events[len(events)-1]
	if last.Kind != KindQueueConsume {
		t.Fatalf("expected QUEUE_CONSUME kind, got %q", last.Kind)
	}
	if last.TraceID != "event-trace" {
		t.Fatalf("expected traceID 'event-trace', got %q", last.TraceID)
	}
}

func TestWrapQueueConsumerRecordsNonZeroStatusOnHandlerError(t *testing.T) {
	client := newTestClient()

	handlerErr := errors.New("processing failed")
	handler := func(ctx context.Context, headers QueueHeaders, body []byte) error {
		return handlerErr
	}

	wrapped := WrapQueueConsumer(client, handler)
	headers := QueueHeaders{
		QueueHeaderTraceID:  "err-trace",
		QueueHeaderParentCe: "err-ce",
	}
	err := wrapped(context.Background(), headers, []byte("bad"))
	if err != handlerErr {
		t.Fatalf("expected handlerErr to be returned, got %v", err)
	}

	events := client.buffer.Flush(time.Now().UnixMilli())
	if len(events) == 0 {
		t.Fatal("expected at least one event recorded on handler error")
	}

	last := events[len(events)-1]
	if last.Kind != KindQueueConsume {
		t.Fatalf("expected QUEUE_CONSUME kind, got %q", last.Kind)
	}
	if last.Status != 500 {
		t.Fatalf("expected status 500 on error, got %d", last.Status)
	}
}

func TestWrapQueueConsumerNilClientDoesNotPanic(t *testing.T) {
	handler := func(ctx context.Context, headers QueueHeaders, body []byte) error {
		return nil
	}

	wrapped := WrapQueueConsumer(nil, handler)
	// Must not panic.
	err := wrapped(context.Background(), QueueHeaders{}, []byte("ok"))
	if err != nil {
		t.Fatalf("unexpected error with nil client: %v", err)
	}
}

func TestWrapQueueConsumerPassesThroughBody(t *testing.T) {
	client := newTestClient()

	var capturedBody []byte
	handler := func(ctx context.Context, headers QueueHeaders, body []byte) error {
		capturedBody = body
		return nil
	}

	wrapped := WrapQueueConsumer(client, handler)
	expectedBody := []byte("hello world")
	if err := wrapped(context.Background(), QueueHeaders{}, expectedBody); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if string(capturedBody) != string(expectedBody) {
		t.Fatalf("expected body %q, got %q", expectedBody, capturedBody)
	}
}

// --- RecordQueuePublish ---

func TestRecordQueuePublishRecordsCorrectEvent(t *testing.T) {
	client := newTestClient()
	ctx := setTraceContext(context.Background(), "pub-trace", "pub-ce")
	start := time.Now().Add(-50 * time.Millisecond)

	RecordQueuePublish(ctx, client, "orders", start)

	events := client.buffer.Flush(time.Now().UnixMilli())
	if len(events) == 0 {
		t.Fatal("expected at least one event after RecordQueuePublish")
	}

	last := events[len(events)-1]
	if last.Kind != KindQueuePublish {
		t.Fatalf("expected QUEUE_PUBLISH kind, got %q", last.Kind)
	}
	if last.TraceID != "pub-trace" {
		t.Fatalf("expected traceID 'pub-trace', got %q", last.TraceID)
	}
	if last.DurationNs <= 0 {
		t.Fatal("expected positive DurationNs")
	}
}

func TestRecordQueuePublishNilClientDoesNotPanic(t *testing.T) {
	ctx := setTraceContext(context.Background(), "trace", "ce")
	// Must not panic.
	RecordQueuePublish(ctx, nil, "topic", time.Now())
}

// --- RecordQueueConsume ---

func TestRecordQueueConsumeRecordsCorrectEventWithoutError(t *testing.T) {
	client := newTestClient()
	ctx := setTraceContext(context.Background(), "con-trace", "con-ce")
	start := time.Now().Add(-30 * time.Millisecond)

	RecordQueueConsume(ctx, client, "payments", start, nil)

	events := client.buffer.Flush(time.Now().UnixMilli())
	if len(events) == 0 {
		t.Fatal("expected at least one event after RecordQueueConsume")
	}

	last := events[len(events)-1]
	if last.Kind != KindQueueConsume {
		t.Fatalf("expected QUEUE_CONSUME kind, got %q", last.Kind)
	}
	if last.TraceID != "con-trace" {
		t.Fatalf("expected traceID 'con-trace', got %q", last.TraceID)
	}
	if last.Status != 0 {
		t.Fatalf("expected status 0 on success, got %d", last.Status)
	}
}

func TestRecordQueueConsumeRecordsErrorStatus(t *testing.T) {
	client := newTestClient()
	ctx := setTraceContext(context.Background(), "con-err-trace", "con-err-ce")
	start := time.Now().Add(-10 * time.Millisecond)

	RecordQueueConsume(ctx, client, "notifications", start, errors.New("consumer failed"))

	events := client.buffer.Flush(time.Now().UnixMilli())
	if len(events) == 0 {
		t.Fatal("expected at least one event after RecordQueueConsume with error")
	}

	last := events[len(events)-1]
	if last.Status != 500 {
		t.Fatalf("expected status 500 on error, got %d", last.Status)
	}
}

func TestRecordQueueConsumeNilClientDoesNotPanic(t *testing.T) {
	ctx := setTraceContext(context.Background(), "trace", "ce")
	// Must not panic.
	RecordQueueConsume(ctx, nil, "topic", time.Now(), nil)
}
