package incidentary

import (
	"context"
	"time"
)

const (
	// QueueHeaderTraceID is the message header key for the Incidentary trace ID.
	// This is the same wire value as TraceIDHeader; it is exposed under a
	// queue-oriented name for clarity in queue instrumentation code.
	QueueHeaderTraceID = TraceIDHeader
	// QueueHeaderParentCe is the message header key for the parent causal event ID.
	// This is the same wire value as ParentCeHeader.
	QueueHeaderParentCe = ParentCeHeader
)

// QueueHeaders is a map of string headers for context propagation through
// queue messages (Kafka, SQS, AMQP, etc.).
type QueueHeaders map[string]string

// InjectQueueContext injects the Incidentary trace context from ctx into a new
// copy of headers. The original headers map is never mutated.
// If ctx carries no trace context (empty traceID), headers are copied as-is.
func InjectQueueContext(ctx context.Context, headers QueueHeaders) QueueHeaders {
	traceID, ceID := ContextTrace(ctx)

	out := make(QueueHeaders, len(headers)+2)
	for k, v := range headers {
		out[k] = v
	}

	if traceID != "" {
		out[QueueHeaderTraceID] = traceID
	}
	if ceID != "" {
		out[QueueHeaderParentCe] = ceID
	}

	return out
}

// ExtractQueueContext reads Incidentary trace headers from headers and returns
// a new context.Context derived from ctx with the trace context set.
// If the required headers are absent, ctx is returned unchanged.
func ExtractQueueContext(ctx context.Context, headers QueueHeaders) context.Context {
	if len(headers) == 0 {
		return ctx
	}

	traceID := headers[QueueHeaderTraceID]
	ceID := headers[QueueHeaderParentCe]

	if traceID == "" && ceID == "" {
		return ctx
	}

	return setTraceContext(ctx, traceID, ceID)
}

// WrapQueueConsumer wraps a consumer handler function to automatically:
//  1. Extract trace context from message headers into the handler's context.
//  2. Call the handler.
//  3. Record a QUEUE_CONSUME event after the handler returns.
//
// If client is nil the handler is still called but no event is recorded.
func WrapQueueConsumer(
	client *Client,
	handler func(ctx context.Context, headers QueueHeaders, body []byte) error,
) func(ctx context.Context, headers QueueHeaders, body []byte) error {
	return func(ctx context.Context, headers QueueHeaders, body []byte) error {
		ctx = ExtractQueueContext(ctx, headers)
		start := time.Now()

		err := handler(ctx, headers, body)

		if client != nil {
			RecordQueueConsume(ctx, client, "", start, err)
		}

		return err
	}
}

// RecordQueuePublish records a QUEUE_PUBLISH event. Call this after
// successfully publishing a message to a queue or topic.
// If client is nil this is a no-op.
func RecordQueuePublish(ctx context.Context, client *Client, topic string, startTime time.Time) {
	if client == nil {
		return
	}

	traceID, parentCe := ContextTrace(ctx)
	durationNs := time.Since(startTime).Nanoseconds()
	if durationNs < 0 {
		durationNs = 0
	}

	var attrs map[string]interface{}
	if topic != "" {
		attrs = map[string]interface{}{"topic": topic}
	}

	client.RecordEvent(EventQueuePublish, RecordEventOptions{
		TraceID:    traceID,
		ParentCeID: parentCe,
		DurationNs: durationNs,
		EventAttrs: attrs,
	})
}

// RecordQueueConsume records a QUEUE_CONSUME event. This is called
// automatically by WrapQueueConsumer; call it directly only when you need
// manual control over the timing or topic name.
// If client is nil this is a no-op.
func RecordQueueConsume(ctx context.Context, client *Client, topic string, startTime time.Time, err error) {
	if client == nil {
		return
	}

	traceID, parentCe := ContextTrace(ctx)
	durationNs := time.Since(startTime).Nanoseconds()
	if durationNs < 0 {
		durationNs = 0
	}

	status := 0
	if err != nil {
		status = 500
	}

	var attrs map[string]interface{}
	if topic != "" {
		attrs = map[string]interface{}{"topic": topic}
	}

	opts := RecordEventOptions{
		TraceID:    traceID,
		ParentCeID: parentCe,
		DurationNs: durationNs,
		EventAttrs: attrs,
	}
	if err != nil {
		opts.Status = &status
	}

	client.RecordEvent(EventQueueConsume, opts)
}
