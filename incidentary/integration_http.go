package incidentary

import "net/http"

// HTTPIntegration provides inbound and outbound HTTP instrumentation helpers
// through the standard Integration interface.
//
// In Go there is no runtime monkey-patching, so HTTPIntegration does not
// automatically intercept HTTP calls. Instead, after registering it with a
// registry, users call Middleware() and Transport() once in their setup code
// to obtain the wrappers and wire them into their HTTP server and client.
//
//	reg := incidentary.NewIntegrationRegistry(client)
//	httpInt := &incidentary.HTTPIntegration{}
//	reg.Register(httpInt)
//	reg.SetupAll()
//
//	// Wire inbound:
//	http.Handle("/", httpInt.Middleware()(myHandler))
//
//	// Wire outbound:
//	httpClient := &http.Client{Transport: httpInt.Transport(nil)}
type HTTPIntegration struct {
	client *Client
}

// Name returns the integration identifier.
func (h *HTTPIntegration) Name() string {
	return "http"
}

// Setup stores the client reference so Middleware() and Transport() can use
// it. It performs no global side effects and returns a no-op cleanup.
func (h *HTTPIntegration) Setup(client *Client) (func(), error) {
	h.client = client
	return nil, nil
}

// Middleware returns a function that wraps an http.Handler with Incidentary
// inbound request instrumentation. The client stored during Setup is used; if
// Setup has not been called, a nil client is passed to the underlying
// Middleware helper (which gracefully handles nil).
func (h *HTTPIntegration) Middleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return Middleware(h.client, next)
	}
}

// Transport returns an http.RoundTripper that injects Incidentary trace
// headers into outgoing requests and records HTTP_OUT events. If base is nil,
// http.DefaultTransport is used. If the client stored during Setup is nil, the
// returned transport is the base transport unchanged.
func (h *HTTPIntegration) Transport(base http.RoundTripper) http.RoundTripper {
	return WrapTransport(h.client, base)
}
