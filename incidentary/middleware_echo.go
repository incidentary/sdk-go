package incidentary

import "net/http"

// EchoMiddlewareFunc returns a func(http.Handler) http.Handler middleware
// adapter compatible with labstack/echo via echo.WrapMiddleware.
//
// Since echo cannot be imported as a dependency, this adapter works with the
// standard http.Handler interface. Echo users can wire it with
// echo.WrapMiddleware():
//
//	e := echo.New()
//	e.Use(echo.WrapMiddleware(incidentary.EchoMiddlewareFunc(client)))
func EchoMiddlewareFunc(client *Client) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return Middleware(client, next)
	}
}
