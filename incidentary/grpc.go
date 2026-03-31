package incidentary

import (
	"context"
	"time"
)

// GRPCMetadataCarrier is a map[string]string that mirrors gRPC metadata
// propagation conventions. Users create gRPC unary interceptors (or streaming
// interceptors) that call InjectGRPCContext / ExtractGRPCContext to propagate
// Incidentary trace context across service boundaries without requiring a
// direct import of google.golang.org/grpc.
type GRPCMetadataCarrier map[string]string

// InjectGRPCContext injects Incidentary trace context from ctx into a new copy
// of md and returns it. The original md map is never mutated. If ctx carries
// no trace context (empty traceID), md is copied as-is with no additions.
func InjectGRPCContext(ctx context.Context, md GRPCMetadataCarrier) GRPCMetadataCarrier {
	traceID, parentCe := ContextTrace(ctx)
	if traceID == "" {
		// No trace in context — return a copy of the input without adding headers.
		out := make(GRPCMetadataCarrier, len(md))
		for k, v := range md {
			out[k] = v
		}
		return out
	}

	out := make(GRPCMetadataCarrier, len(md)+2)
	for k, v := range md {
		out[k] = v
	}
	out[TraceIDHeader] = traceID
	out[ParentCeHeader] = parentCe
	return out
}

// ExtractGRPCContext reads Incidentary trace headers from md and returns a new
// context.Context derived from ctx with the trace context set. If the required
// TraceIDHeader is absent, ctx is returned unchanged. Never panics with nil md.
func ExtractGRPCContext(ctx context.Context, md GRPCMetadataCarrier) context.Context {
	if len(md) == 0 {
		return ctx
	}
	traceID := md[TraceIDHeader]
	if traceID == "" {
		return ctx
	}
	parentCe := md[ParentCeHeader]
	return setTraceContext(ctx, traceID, parentCe)
}

// RecordGRPCCall records a gRPC call event (incoming or outgoing). Use
// KindHTTPIn for server-side (incoming) calls with EventGRPCIn, and
// KindHTTPOut for client-side (outgoing) calls with EventGRPCOut. The event
// type is inferred from kind: KindHTTPIn maps to EventGRPCIn, everything else
// maps to EventGRPCOut. If client is nil this is a no-op. Never panics.
func RecordGRPCCall(ctx context.Context, client *Client, kind CeKind, method string, startTime time.Time, err error) {
	if client == nil {
		return
	}

	duration := time.Since(startTime)
	if duration < 0 {
		duration = 0
	}

	traceID, parentCe := ContextTrace(ctx)
	if traceID == "" {
		traceID = randomUUID()
	}

	eventType := EventGRPCOut
	if kind == KindHTTPIn {
		eventType = EventGRPCIn
	}

	status := 0
	if err != nil {
		status = 500
	}

	ce := &SkeletonCe{
		CeID:       randomUUID(),
		TraceID:    traceID,
		ParentCeID: parentCe,
		ServiceID:  client.ServiceName,
		WallTsNs:   startTime.UnixNano(),
		Kind:       kind,
		EventType:  string(eventType),
		EventClass: "causal",
		Status:     status,
		DurationNs: duration.Nanoseconds(),
		SdkVersion: sdkVersion,
	}

	if method != "" {
		ce.EventAttrs = map[string]interface{}{"method": method}
	}

	client.WriteEvent(ce)
}

// WrapGRPCHandler wraps a gRPC handler function to automatically:
//  1. Extract Incidentary trace context from md into the handler's context.
//  2. Call the handler with the enriched context.
//  3. Record a grpc_in event after the handler returns.
//
// If client is nil the handler is still called but no event is recorded.
// The returned wrapper never panics.
func WrapGRPCHandler(client *Client, md GRPCMetadataCarrier, handler func(ctx context.Context) error) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		ctx = ExtractGRPCContext(ctx, md)
		start := time.Now()
		err := handler(ctx)
		RecordGRPCCall(ctx, client, KindHTTPIn, "", start, err)
		return err
	}
}
