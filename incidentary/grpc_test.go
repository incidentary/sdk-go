package incidentary

import (
	"context"
	"errors"
	"testing"
	"time"
)

// --- InjectGRPCContext ---

func TestInjectGRPCContextAddsTraceHeaders(t *testing.T) {
	ctx := setTraceContext(context.Background(), "trace-grpc-inject", "ce-grpc-inject")
	md := GRPCMetadataCarrier{}

	result := InjectGRPCContext(ctx, md)

	if result[TraceIDHeader] != "trace-grpc-inject" {
		t.Fatalf("expected trace header 'trace-grpc-inject', got %q", result[TraceIDHeader])
	}
	if result[ParentCeHeader] != "ce-grpc-inject" {
		t.Fatalf("expected parent-ce header 'ce-grpc-inject', got %q", result[ParentCeHeader])
	}
}

func TestInjectGRPCContextWithNoTraceContextReturnsInputUnchanged(t *testing.T) {
	// Context carries no trace info — traceID is empty.
	ctx := context.Background()
	md := GRPCMetadataCarrier{"existing": "value"}

	result := InjectGRPCContext(ctx, md)

	// No trace headers should be injected.
	if result[TraceIDHeader] != "" {
		t.Fatalf("expected no trace header when context has no trace, got %q", result[TraceIDHeader])
	}
	// Existing keys must still be present.
	if result["existing"] != "value" {
		t.Fatal("expected existing keys to be preserved when no trace context")
	}
}

func TestInjectGRPCContextDoesNotMutateInput(t *testing.T) {
	ctx := setTraceContext(context.Background(), "trace-mut", "ce-mut")
	original := GRPCMetadataCarrier{"key": "val"}

	_ = InjectGRPCContext(ctx, original)

	if _, ok := original[TraceIDHeader]; ok {
		t.Fatal("expected InjectGRPCContext to not mutate the original metadata map")
	}
}

func TestInjectGRPCContextWithNilInputCreatesNewMap(t *testing.T) {
	ctx := setTraceContext(context.Background(), "trace-nil-md", "ce-nil-md")

	// Must not panic with nil input.
	result := InjectGRPCContext(ctx, nil)

	if result == nil {
		t.Fatal("expected non-nil result when input is nil")
	}
	if result[TraceIDHeader] != "trace-nil-md" {
		t.Fatalf("expected trace header 'trace-nil-md', got %q", result[TraceIDHeader])
	}
}

func TestInjectGRPCContextPreservesExistingKeys(t *testing.T) {
	ctx := setTraceContext(context.Background(), "trace-preserve", "ce-preserve")
	md := GRPCMetadataCarrier{
		"x-custom-header": "custom-value",
		"x-other":         "other-value",
	}

	result := InjectGRPCContext(ctx, md)

	if result["x-custom-header"] != "custom-value" {
		t.Fatal("expected existing keys to be preserved")
	}
	if result["x-other"] != "other-value" {
		t.Fatal("expected existing keys to be preserved")
	}
}

// --- ExtractGRPCContext ---

func TestExtractGRPCContextSetsContextValues(t *testing.T) {
	md := GRPCMetadataCarrier{
		TraceIDHeader:  "trace-extract",
		ParentCeHeader: "ce-extract",
	}

	ctx := ExtractGRPCContext(context.Background(), md)

	gotTrace, gotCe := ContextTrace(ctx)
	if gotTrace != "trace-extract" {
		t.Fatalf("expected traceID 'trace-extract', got %q", gotTrace)
	}
	if gotCe != "ce-extract" {
		t.Fatalf("expected ceID 'ce-extract', got %q", gotCe)
	}
}

func TestExtractGRPCContextWithMissingHeadersReturnsOriginalContext(t *testing.T) {
	md := GRPCMetadataCarrier{"unrelated": "header"}
	base := context.Background()

	ctx := ExtractGRPCContext(base, md)

	gotTrace, _ := ContextTrace(ctx)
	if gotTrace != "" {
		t.Fatalf("expected empty traceID with missing headers, got %q", gotTrace)
	}
}

func TestExtractGRPCContextWithNilMetadataDoesNotPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("unexpected panic with nil metadata: %v", r)
		}
	}()

	ctx := ExtractGRPCContext(context.Background(), nil)

	gotTrace, _ := ContextTrace(ctx)
	if gotTrace != "" {
		t.Fatalf("expected empty traceID with nil metadata, got %q", gotTrace)
	}
}

func TestExtractGRPCContextDoesNotMutateParentContext(t *testing.T) {
	md := GRPCMetadataCarrier{
		TraceIDHeader:  "trace-new",
		ParentCeHeader: "ce-new",
	}

	parent := context.Background()
	_ = ExtractGRPCContext(parent, md)

	gotTrace, _ := ContextTrace(parent)
	if gotTrace != "" {
		t.Fatal("expected parent context to be unaffected by ExtractGRPCContext")
	}
}

// --- RecordGRPCCall ---

func TestRecordGRPCCallRecordsGRPCInEvent(t *testing.T) {
	client := newTestClient()
	ctx := setTraceContext(context.Background(), "trace-grpc-in", "ce-grpc-in")

	RecordGRPCCall(ctx, client, KindHTTPIn, "/MyService/MyMethod", time.Now(), nil)

	events := drainEvents(client)
	if len(events) == 0 {
		t.Fatal("expected event from RecordGRPCCall")
	}

	ce := events[len(events)-1]
	if ce.Kind != KindHTTPIn {
		t.Fatalf("expected kind %q, got %q", KindHTTPIn, ce.Kind)
	}
	if ce.EventType != string(EventGRPCIn) {
		t.Fatalf("expected event_type %q, got %q", EventGRPCIn, ce.EventType)
	}
	if ce.TraceID != "trace-grpc-in" {
		t.Fatalf("expected traceID 'trace-grpc-in', got %q", ce.TraceID)
	}
}

func TestRecordGRPCCallRecordsGRPCOutEvent(t *testing.T) {
	client := newTestClient()
	ctx := setTraceContext(context.Background(), "trace-grpc-out", "ce-grpc-out")

	RecordGRPCCall(ctx, client, KindHTTPOut, "/MyService/GetFoo", time.Now(), nil)

	events := drainEvents(client)
	if len(events) == 0 {
		t.Fatal("expected event from RecordGRPCCall")
	}

	ce := events[len(events)-1]
	if ce.Kind != KindHTTPOut {
		t.Fatalf("expected kind %q, got %q", KindHTTPOut, ce.Kind)
	}
	if ce.EventType != string(EventGRPCOut) {
		t.Fatalf("expected event_type %q, got %q", EventGRPCOut, ce.EventType)
	}
}

func TestRecordGRPCCallSetsStatus500OnError(t *testing.T) {
	client := newTestClient()
	RecordGRPCCall(context.Background(), client, KindHTTPIn, "/svc/Method", time.Now(), errors.New("rpc error"))

	events := drainEvents(client)
	if len(events) == 0 {
		t.Fatal("expected event to be recorded")
	}
	if events[len(events)-1].Status != 500 {
		t.Fatalf("expected status 500, got %d", events[len(events)-1].Status)
	}
}

func TestRecordGRPCCallSetsStatus0OnSuccess(t *testing.T) {
	client := newTestClient()
	RecordGRPCCall(context.Background(), client, KindHTTPIn, "/svc/Method", time.Now(), nil)

	events := drainEvents(client)
	if len(events) == 0 {
		t.Fatal("expected event to be recorded")
	}
	if events[len(events)-1].Status != 0 {
		t.Fatalf("expected status 0, got %d", events[len(events)-1].Status)
	}
}

func TestRecordGRPCCallNilClientDoesNotPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("unexpected panic with nil client: %v", r)
		}
	}()

	RecordGRPCCall(context.Background(), nil, KindHTTPIn, "/svc/Method", time.Now(), nil)
}

func TestRecordGRPCCallDurationIsNonNegative(t *testing.T) {
	client := newTestClient()
	start := time.Now().Add(-10 * time.Millisecond)
	RecordGRPCCall(context.Background(), client, KindHTTPIn, "/svc/Method", start, nil)

	events := drainEvents(client)
	if len(events) == 0 {
		t.Fatal("expected event to be recorded")
	}
	if events[len(events)-1].DurationNs < 0 {
		t.Fatalf("expected non-negative DurationNs, got %d", events[len(events)-1].DurationNs)
	}
}

// --- WrapGRPCHandler ---

func TestWrapGRPCHandlerRunsHandlerWithExtractedTraceContext(t *testing.T) {
	client := newTestClient()
	md := GRPCMetadataCarrier{
		TraceIDHeader:  "trace-wrap-handler",
		ParentCeHeader: "ce-wrap-handler",
	}

	var capturedTrace, capturedCe string
	handler := func(ctx context.Context) error {
		capturedTrace, capturedCe = ContextTrace(ctx)
		return nil
	}

	wrapped := WrapGRPCHandler(client, md, handler)
	if err := wrapped(context.Background()); err != nil {
		t.Fatalf("unexpected error from wrapped handler: %v", err)
	}

	if capturedTrace != "trace-wrap-handler" {
		t.Fatalf("expected traceID 'trace-wrap-handler', got %q", capturedTrace)
	}
	if capturedCe != "ce-wrap-handler" {
		t.Fatalf("expected ceID 'ce-wrap-handler', got %q", capturedCe)
	}
}

func TestWrapGRPCHandlerRecordsGRPCInEvent(t *testing.T) {
	client := newTestClient()
	md := GRPCMetadataCarrier{
		TraceIDHeader:  "trace-wrap-event",
		ParentCeHeader: "ce-wrap-event",
	}

	handler := func(ctx context.Context) error {
		return nil
	}

	wrapped := WrapGRPCHandler(client, md, handler)
	if err := wrapped(context.Background()); err != nil {
		t.Fatalf("unexpected error from wrapped handler: %v", err)
	}

	events := drainEvents(client)
	if len(events) == 0 {
		t.Fatal("expected at least one event recorded by WrapGRPCHandler")
	}

	ce := events[len(events)-1]
	if ce.EventType != string(EventGRPCIn) {
		t.Fatalf("expected event_type %q, got %q", EventGRPCIn, ce.EventType)
	}
}

func TestWrapGRPCHandlerPropagatesHandlerError(t *testing.T) {
	client := newTestClient()
	handlerErr := errors.New("handler failed")

	handler := func(ctx context.Context) error {
		return handlerErr
	}

	wrapped := WrapGRPCHandler(client, GRPCMetadataCarrier{}, handler)
	err := wrapped(context.Background())
	if !errors.Is(err, handlerErr) {
		t.Fatalf("expected error %v, got %v", handlerErr, err)
	}
}

func TestWrapGRPCHandlerErrorRecordsStatus500(t *testing.T) {
	client := newTestClient()

	handler := func(ctx context.Context) error {
		return errors.New("rpc failed")
	}

	wrapped := WrapGRPCHandler(client, GRPCMetadataCarrier{}, handler)
	_ = wrapped(context.Background())

	events := drainEvents(client)
	if len(events) == 0 {
		t.Fatal("expected event from WrapGRPCHandler on error")
	}
	if events[len(events)-1].Status != 500 {
		t.Fatalf("expected status 500, got %d", events[len(events)-1].Status)
	}
}

func TestWrapGRPCHandlerWithNilClientStillRunsHandler(t *testing.T) {
	var handlerCalled bool
	handler := func(ctx context.Context) error {
		handlerCalled = true
		return nil
	}

	wrapped := WrapGRPCHandler(nil, GRPCMetadataCarrier{}, handler)

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("unexpected panic with nil client: %v", r)
		}
	}()

	_ = wrapped(context.Background())

	if !handlerCalled {
		t.Fatal("expected handler to be called even with nil client")
	}
}

// --- GRPCIntegration ---

func TestGRPCIntegrationImplementsInterface(t *testing.T) {
	var _ Integration = (*GRPCIntegration)(nil)
}

func TestGRPCIntegrationName(t *testing.T) {
	g := &GRPCIntegration{}
	if g.Name() != "grpc" {
		t.Fatalf("expected name 'grpc', got %q", g.Name())
	}
}

func TestGRPCIntegrationSetupStoresClient(t *testing.T) {
	client := newTestClient()
	g := &GRPCIntegration{}

	cleanup, err := g.Setup(client)
	if err != nil {
		t.Fatalf("unexpected Setup error: %v", err)
	}
	if cleanup != nil {
		cleanup()
	}
	if g.client != client {
		t.Fatal("expected GRPCIntegration to store client reference")
	}
}

func TestGRPCIntegrationSetupWithNilClientDoesNotError(t *testing.T) {
	g := &GRPCIntegration{}
	cleanup, err := g.Setup(nil)
	if err != nil {
		t.Fatalf("expected no error with nil client, got: %v", err)
	}
	if cleanup != nil {
		cleanup()
	}
}

func TestGRPCIntegrationInjectMetadataAddsHeaders(t *testing.T) {
	client := newTestClient()
	g := &GRPCIntegration{}
	_, _ = g.Setup(client)

	ctx := setTraceContext(context.Background(), "trace-inject-intg", "ce-inject-intg")
	result := g.InjectMetadata(ctx, GRPCMetadataCarrier{})

	if result[TraceIDHeader] != "trace-inject-intg" {
		t.Fatalf("expected trace header 'trace-inject-intg', got %q", result[TraceIDHeader])
	}
}

func TestGRPCIntegrationExtractAndHandleRunsHandlerWithContext(t *testing.T) {
	client := newTestClient()
	g := &GRPCIntegration{}
	_, _ = g.Setup(client)

	md := GRPCMetadataCarrier{
		TraceIDHeader:  "trace-intg-extract",
		ParentCeHeader: "ce-intg-extract",
	}

	var capturedTrace string
	handler := func(ctx context.Context) error {
		capturedTrace, _ = ContextTrace(ctx)
		return nil
	}

	wrapped := g.ExtractAndHandle(md, handler)
	_ = wrapped(context.Background())

	if capturedTrace != "trace-intg-extract" {
		t.Fatalf("expected traceID 'trace-intg-extract', got %q", capturedTrace)
	}
}

func TestDefaultIntegrationsContainsGRPC(t *testing.T) {
	integrations := DefaultIntegrations()
	for _, i := range integrations {
		if i.Name() == "grpc" {
			return
		}
	}
	t.Fatal("expected DefaultIntegrations to contain the 'grpc' integration")
}
