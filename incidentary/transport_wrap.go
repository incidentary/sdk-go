package incidentary

import (
	"net/http"
	"time"
)

// instrumentedTransport wraps an http.RoundTripper to inject trace context
// headers and record HTTP_OUT events in the ring buffer.
type instrumentedTransport struct {
	client *Client
	base   http.RoundTripper
}

// WrapTransport returns an http.RoundTripper that injects Incidentary trace
// headers into outgoing requests and records HTTP_OUT skeleton events. If base
// is nil, http.DefaultTransport is used. If client is nil, the returned
// transport passes through to base without modification.
func WrapTransport(client *Client, base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	if client == nil {
		return base
	}
	return &instrumentedTransport{client: client, base: base}
}

func (t *instrumentedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	traceID, parentCe := ContextTrace(req.Context())
	if traceID == "" {
		// No active trace context — pass through without instrumentation.
		return t.base.RoundTrip(req)
	}

	ceID := randomUUID()

	// Clone the request to avoid mutating the caller's headers.
	cloned := req.Clone(req.Context())
	cloned.Header.Set(TraceIDHeader, traceID)
	cloned.Header.Set(ParentCeHeader, ceID)

	start := time.Now()
	resp, err := t.base.RoundTrip(cloned)
	durationNs := time.Since(start).Nanoseconds()

	status := 0
	if err == nil && resp != nil {
		status = resp.StatusCode
	}

	ce := &SkeletonCe{
		CeID:       ceID,
		TraceID:    traceID,
		ParentCeID: parentCe,
		ServiceID:  t.client.ServiceName,
		WallTsNs:   time.Now().UnixNano(),
		Kind:       KindHTTPOut,
		EventType:  string(EventHTTPOut),
		StatusCode: status,
		DurationNs: durationNs,
	}
	t.client.WriteEvent(ce)

	return resp, err
}
