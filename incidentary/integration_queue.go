package incidentary

import "context"

// QueueIntegration provides queue instrumentation helpers as an Integration.
// It does not perform any global side effects during Setup — in Go there is no
// runtime monkey-patching. Instead, after registering it with a registry, users
// call Consumer() and InjectHeaders() once in their setup code to obtain the
// wrappers and wire them into their queue consumers and producers.
//
//	reg := incidentary.NewIntegrationRegistry(client)
//	queueInt := &incidentary.QueueIntegration{}
//	reg.Register(queueInt)
//	reg.SetupAll()
//
//	// Wire consumer:
//	consumeMessage = queueInt.Consumer(myHandler)
//
//	// Wire producer (before publishing):
//	headers = queueInt.InjectHeaders(ctx, incidentary.QueueHeaders{})
type QueueIntegration struct {
	client *Client
}

// Name returns the integration identifier.
func (q *QueueIntegration) Name() string {
	return "queue"
}

// Setup stores the client reference so Consumer() and InjectHeaders() can use
// it. It performs no global side effects and returns a no-op cleanup.
func (q *QueueIntegration) Setup(client *Client) (func(), error) {
	q.client = client
	return nil, nil
}

// Consumer returns a wrapped consumer handler that automatically extracts trace
// context from message headers and records a QUEUE_CONSUME event after the
// handler returns.
func (q *QueueIntegration) Consumer(
	handler func(ctx context.Context, headers QueueHeaders, body []byte) error,
) func(ctx context.Context, headers QueueHeaders, body []byte) error {
	return WrapQueueConsumer(q.client, handler)
}

// InjectHeaders injects the Incidentary trace context from ctx into a new copy
// of headers, ready for attaching to an outgoing queue message. The original
// headers map is never mutated.
func (q *QueueIntegration) InjectHeaders(ctx context.Context, headers QueueHeaders) QueueHeaders {
	return InjectQueueContext(ctx, headers)
}
