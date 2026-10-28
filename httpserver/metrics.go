package httpserver

import (
	"context"
	"net/http"
	"time"
)

// RequestStat is one served request, with closed-set labels only: Method is
// normalized, Route is a pattern, never a path.
type RequestStat struct {
	Method   string
	Route    string
	Status   int
	Elapsed  time.Duration
	BytesIn  int64
	BytesOut int64
	Panicked bool
}

// Observer receives request statistics. Implementations must be cheap and
// non-blocking: they run on every request. httpserver/otel exports them as
// http.server.* metrics.
type Observer interface {
	// ObserveInFlight is called with +1 when a request starts and -1 when
	// it ends.
	ObserveInFlight(ctx context.Context, delta int)
	ObserveRequest(ctx context.Context, s RequestStat)
}

// NopObserver discards everything.
type NopObserver struct{}

func (NopObserver) ObserveInFlight(context.Context, int)        {}
func (NopObserver) ObserveRequest(context.Context, RequestStat) {}

// OrNop returns o or a NopObserver.
func OrNop(o Observer) Observer {
	if o == nil {
		return NopObserver{}
	}
	return o
}

// RouteFunc names the route a request matched, for metrics, logs and span
// names. It is called after the handler ran. Return "" when unknown; never
// return the raw path.
//
// For chi, register the middleware inside the router (r.Use) and pass
//
//	func(r *http.Request) string { return chi.RouteContext(r.Context()).RoutePattern() }
//
// For net/http's ServeMux use PatternRoute and wrap the mux in Routed.
type RouteFunc func(*http.Request) string

// PatternRoute is the RouteFunc for net/http's ServeMux (Go 1.22 patterns).
// The mux sets the pattern on the request pointer it receives, so a
// middleware that copied the request (RequestID, Timeout, Tracing) would hide
// it from the ones outside; PatternRoute therefore also reads what Routed or
// SetRoute published through the context.
func PatternRoute(r *http.Request) string {
	if r.Pattern != "" {
		return r.Pattern
	}
	if h, ok := r.Context().Value(routeKey{}).(*routeHolder); ok {
		return h.pattern
	}
	return ""
}

type routeKey struct{}

// routeHolder is shared by pointer through the context, so it survives
// request copies and lets an inner middleware tell the outer ones the route.
type routeHolder struct{ pattern string }

// withRouteHolder returns r carrying a holder, adding one when absent.
func withRouteHolder(r *http.Request) *http.Request {
	if _, ok := r.Context().Value(routeKey{}).(*routeHolder); ok {
		return r
	}
	return r.WithContext(context.WithValue(r.Context(), routeKey{}, &routeHolder{}))
}

// SetRoute publishes the matched route for the middleware outside the
// caller. Routers other than ServeMux call it from their innermost
// middleware, e.g. for chi:
//
//	r.Use(func(next http.Handler) http.Handler {
//	    return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
//	        next.ServeHTTP(w, req)
//	        httpserver.SetRoute(req, chi.RouteContext(req.Context()).RoutePattern())
//	    })
//	})
func SetRoute(r *http.Request, pattern string) {
	if pattern == "" {
		return
	}
	if h, ok := r.Context().Value(routeKey{}).(*routeHolder); ok {
		h.pattern = pattern
	}
}

// Routed wraps a net/http ServeMux so the pattern it matched reaches the
// middleware outside it: Metrics(...)(RequestID(...)(Routed(mux))).
func Routed(mux http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r)
		SetRoute(r, r.Pattern)
	})
}

// Metrics reports every request to obs with the route named by route
// (default PatternRoute). Place it outside Recover so panics count as 500.
func Metrics(obs Observer, route RouteFunc) func(http.Handler) http.Handler {
	obs = OrNop(obs)
	if route == nil {
		route = PatternRoute
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rec := Record(w)
			r = withRouteHolder(r)
			ctx := r.Context()
			obs.ObserveInFlight(ctx, 1)
			start := time.Now()
			defer func() {
				obs.ObserveInFlight(ctx, -1)
				rt := route(r)
				SetRoute(r, rt)
				obs.ObserveRequest(ctx, RequestStat{
					Method:   NormalizeMethod(r.Method),
					Route:    rt,
					Status:   StatusOf(rec, r),
					Elapsed:  time.Since(start),
					BytesIn:  max(r.ContentLength, 0),
					BytesOut: rec.BytesWritten(),
					Panicked: rec.Panicked(),
				})
			}()
			next.ServeHTTP(rec, r)
		})
	}
}

// NormalizeMethod maps unknown methods to "_OTHER" so the method label is a
// closed set (OpenTelemetry HTTP semantic conventions).
func NormalizeMethod(m string) string {
	switch m {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch,
		http.MethodDelete, http.MethodConnect, http.MethodOptions, http.MethodTrace:
		return m
	default:
		return "_OTHER"
	}
}
