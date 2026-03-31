package incidentary

import "net/http"

// ChiMiddleware returns middleware compatible with the go-chi/chi router.
// Chi uses the standard func(http.Handler) http.Handler middleware signature,
// so this adapter is a direct pass-through to the underlying Middleware helper.
//
// Usage:
//
//	r := chi.NewRouter()
//	r.Use(incidentary.ChiMiddleware(client))
func ChiMiddleware(client *Client) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return Middleware(client, next)
	}
}
