package incidentary

import "net/http"

// GinMiddleware returns an http.Handler that wraps next with Incidentary
// inbound request instrumentation, compatible with gin-gonic/gin.
//
// Since gin cannot be imported as a dependency, this adapter works with the
// standard http.Handler interface. Gin users can wire it with gin.WrapH():
//
//	router.Use(gin.WrapH(incidentary.GinMiddleware(client, router)))
//
// Alternatively, wire it manually in a gin middleware function:
//
//	router.Use(func(c *gin.Context) {
//	    incidentary.Middleware(client, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
//	        c.Request = r  // propagate enriched context back to gin
//	        c.Next()
//	    })).ServeHTTP(c.Writer, c.Request)
//	})
func GinMiddleware(client *Client, next http.Handler) http.Handler {
	return Middleware(client, next)
}
