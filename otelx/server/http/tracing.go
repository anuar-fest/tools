// Package serverhttp traces the HTTP server you own: a middleware that
// continues the caller's trace (or starts one) and names the server span
// after the matched route. The http.server.* metrics for the same requests
// are metrics/http's Observer.
//
// The middleware is written on the OpenTelemetry API rather than wrapping
// otelhttp so the span can be named after the route, which routers only
// know after the handler ran, and so the status and panic flag come from
// the one ResponseRecorder every httpserver middleware shares.
package serverhttp

import (
	"net/http"
	"strconv"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/anuar-fest/tools/httpserver"
)

// InstrumentationName is the tracer name.
const InstrumentationName = "github.com/anuar-fest/tools/otelx/server/http"

// HeaderTraceID is the response header that carries the trace id so a
// support ticket can quote it.
const HeaderTraceID = "X-Trace-Id"

// TracingConfig configures Tracing.
type TracingConfig struct {
	// Tracer defaults to the global tracer named InstrumentationName.
	Tracer trace.Tracer
	// Propagator defaults to the global propagator.
	Propagator propagation.TextMapPropagator
	// Route names the matched route after the handler ran; default
	// httpserver.PatternRoute.
	Route httpserver.RouteFunc
	// Skip leaves probes and scrapes untraced.
	Skip func(*http.Request) bool
	// NoTraceIDHeader stops echoing the trace id in HeaderTraceID.
	NoTraceIDHeader bool
	// PublicEndpoint treats an inbound trace context as untrusted: the
	// server span starts a new trace and links to the caller's instead of
	// continuing it. Set it on ingress that browsers or partners call.
	PublicEndpoint bool
}

// Tracing runs each request in a server span. The span starts as "METHOD"
// and is renamed "METHOD route" once the router matched; status 5xx and
// recovered panics set the error status. Put it outside Recover, so the
// panic is seen as a 500, and inside Metrics and AccessLog.
func Tracing(cfg TracingConfig) func(http.Handler) http.Handler {
	tracer := cfg.Tracer
	if tracer == nil {
		tracer = otel.Tracer(InstrumentationName)
	}
	prop := cfg.Propagator
	if prop == nil {
		prop = otel.GetTextMapPropagator()
	}
	route := cfg.Route
	if route == nil {
		route = httpserver.PatternRoute
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if cfg.Skip != nil && cfg.Skip(r) {
				next.ServeHTTP(w, r)
				return
			}
			method := httpserver.NormalizeMethod(r.Method)
			ctx := prop.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
			opts := []trace.SpanStartOption{trace.WithSpanKind(trace.SpanKindServer), trace.WithAttributes(requestAttrs(r, method)...)}
			if cfg.PublicEndpoint {
				if remote := trace.SpanContextFromContext(ctx); remote.IsValid() {
					opts = append(opts, trace.WithLinks(trace.Link{SpanContext: remote}))
				}
				ctx = trace.ContextWithSpanContext(ctx, trace.SpanContext{})
			}
			ctx, span := tracer.Start(ctx, method, opts...)
			defer span.End()
			if !cfg.NoTraceIDHeader && span.SpanContext().HasTraceID() {
				w.Header().Set(HeaderTraceID, span.SpanContext().TraceID().String())
			}
			rec := httpserver.Record(w)
			r = r.WithContext(ctx)
			next.ServeHTTP(rec, r)

			status := httpserver.StatusOf(rec, r)
			if rt := route(r); rt != "" {
				httpserver.SetRoute(r, rt)
				span.SetName(spanName(method, rt))
				span.SetAttributes(semconv.HTTPRoute(rt))
			}
			span.SetAttributes(semconv.HTTPResponseStatusCode(status))
			if et := errorType(status, rec.Panicked()); et != "" {
				span.SetAttributes(semconv.ErrorTypeKey.String(et))
				span.SetStatus(codes.Error, http.StatusText(status))
			}
		})
	}
}

// spanName is "METHOD route"; ServeMux patterns already start with the
// method ("GET /items/{id}"), chi patterns do not ("/items/{id}").
func spanName(method, route string) string {
	if strings.HasPrefix(route, method+" ") {
		return route
	}
	return method + " " + route
}

func requestAttrs(r *http.Request, method string) []attribute.KeyValue {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	attrs := make([]attribute.KeyValue, 0, 8)
	attrs = append(attrs,
		semconv.HTTPRequestMethodKey.String(method),
		semconv.URLPath(r.URL.Path),
		semconv.URLScheme(scheme),
		semconv.NetworkProtocolVersion(strings.TrimPrefix(r.Proto, "HTTP/")),
		semconv.ClientAddress(httpserver.ClientIP(r)),
	)
	if method == "_OTHER" {
		attrs = append(attrs, attribute.String("http.request.method_original", r.Method))
	}
	if r.Host != "" {
		attrs = append(attrs, semconv.ServerAddress(r.Host))
	}
	if ua := r.UserAgent(); ua != "" {
		attrs = append(attrs, semconv.UserAgentOriginal(ua))
	}
	return attrs
}

// errorType follows the HTTP server conventions: 5xx responses carry the
// status code as error.type; a recovered panic says so.
func errorType(status int, panicked bool) string {
	switch {
	case panicked:
		return "panic"
	case status >= 500:
		return strconv.Itoa(status)
	default:
		return ""
	}
}
