// Package httpserver is the inbound HTTP edge: an http.Server with every
// timeout set, and the middleware a service needs around its router without
// inheriting a framework. It depends on the standard library only; chi or
// http.ServeMux stay in the service.
//
// Recommended order, outermost first:
//
//	RealIP → RequestID → Metrics → AccessLog → Recover → Tracing (otelx/server/http)
//	→ httpx.Shed → httpx.RateLimit → CORS → NoCache → MaxBytes → Timeout → APIKey → Routed(mux)
//
// Routed publishes the ServeMux pattern to the middleware outside it; chi
// users register the chain inside the router instead (see RouteFunc).
//
// Metrics and AccessLog sit outside Recover so a panic is counted and
// logged as a 500; shedding sits before authentication so refusing costs
// less than admitting; MaxBytes sits before anything that reads the body.
//
// Every middleware shares one ResponseRecorder per request (see Record), so
// they agree on the status and byte count, and hijacked connections
// (WebSocket) keep working.
//
// Logging goes through *slog.Logger; nil discards. Middleware log with the
// request context and add no request id themselves: wire LogAttrs into the
// logger (logx.ContextHandler) and every line of the request carries it.
package httpserver
