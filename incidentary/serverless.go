package incidentary

import (
	"context"
	"encoding/json"
)

// WrapLambdaHandler wraps a Lambda handler function to provide trace context
// and flush buffered events before the Lambda execution environment freezes.
// The handler receives a context with traceID and ceID already set, so
// downstream calls using WrapTransport will automatically propagate headers.
//
// Flush is called after every invocation, including when the handler returns
// an error, to ensure events are not lost on Lambda freeze.
func WrapLambdaHandler(
	client *Client,
	handler func(ctx context.Context, event json.RawMessage) (interface{}, error),
) func(ctx context.Context, event json.RawMessage) (interface{}, error) {
	return func(ctx context.Context, event json.RawMessage) (interface{}, error) {
		traceID := randomUUID()
		ceID := randomUUID()
		ctx = setTraceContext(ctx, traceID, ceID)

		result, err := handler(ctx, event)

		// Always flush before Lambda freezes, even on error.
		if client != nil {
			client.FlushToBackend(nil)
		}

		return result, err
	}
}
