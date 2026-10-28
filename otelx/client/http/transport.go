// Package clienthttp traces outbound HTTP calls: one client span per call
// with the HTTP semantic conventions and the trace context injected into
// the request. Wrap the resilience/httpx transport so the span covers every
// attempt; metrics/http measures the same calls.
package clienthttp

import (
	"net/http"
	"net/url"
	"strconv"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/anuar-fest/tools/metrics"
)

// InstrumentationName is the tracer name.
const InstrumentationName = "github.com/anuar-fest/tools/otelx/client/http"

// Config configures Transport.
type Config struct {
	// Tracer defaults to the global tracer named InstrumentationName.
	Tracer trace.Tracer
	// Propagator defaults to the global propagator.
	Propagator propagation.TextMapPropagator
	// PeerService names the dependency (peer.service) and the span:
	// "GET fraud-api". Without it the span is named after the host.
	PeerService string
}

// Transport wraps next with one client span per call and injects the trace
// context into the request. Wrap the resilience/httpx transport, so the
// span covers retries (resilience/otel adds them as events) and the
// headers are injected before the first attempt; put metricshttp.Transport
// inside it so the duration sample carries the span as an exemplar:
//
//	tr, _ := httpx.NewTransport(httpx.TransportConfig{...})
//	client := &http.Client{Transport: clienthttp.Transport(metricshttp.Transport(tr, mcfg), clienthttp.Config{PeerService: "fraud"})}
//
// The span ends when headers arrive; reading the body is not timed.
// url.full carries no query string: that is where tokens travel.
// A nil next is http.DefaultTransport, which has no timeouts; prefer httpx.
func Transport(next http.RoundTripper, cfg Config) http.RoundTripper {
	if next == nil {
		next = http.DefaultTransport
	}
	if cfg.Tracer == nil {
		cfg.Tracer = otel.Tracer(InstrumentationName)
	}
	if cfg.Propagator == nil {
		cfg.Propagator = otel.GetTextMapPropagator()
	}
	return &transport{next: next, cfg: cfg}
}

type transport struct {
	next http.RoundTripper
	cfg  Config
}

func (t *transport) RoundTrip(req *http.Request) (*http.Response, error) {
	host, port := hostPort(req.URL)
	name := req.Method + " " + host
	attrs := []attribute.KeyValue{
		semconv.HTTPRequestMethodKey.String(req.Method),
		semconv.ServerAddress(host),
		semconv.URLFull(sanitizedURL(req.URL)),
		semconv.URLScheme(req.URL.Scheme),
	}
	if port > 0 {
		attrs = append(attrs, semconv.ServerPort(port))
	}
	if t.cfg.PeerService != "" {
		name = req.Method + " " + t.cfg.PeerService
		attrs = append(attrs, attribute.String("peer.service", t.cfg.PeerService))
	}
	ctx, span := t.cfg.Tracer.Start(req.Context(), name, trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(attrs...))
	defer span.End()

	req = req.Clone(ctx) // RoundTripper contract: never mutate the caller's request
	t.cfg.Propagator.Inject(ctx, propagation.HeaderCarrier(req.Header))
	resp, err := t.next.RoundTrip(req)
	if err != nil {
		span.RecordError(err)
		span.SetAttributes(semconv.ErrorTypeKey.String(metrics.ErrorType(err)))
		span.SetStatus(codes.Error, "request failed")
		return nil, err
	}
	span.SetAttributes(semconv.HTTPResponseStatusCode(resp.StatusCode))
	if resp.StatusCode >= http.StatusBadRequest {
		span.SetAttributes(semconv.ErrorTypeKey.String(strconv.Itoa(resp.StatusCode)))
		span.SetStatus(codes.Error, http.StatusText(resp.StatusCode))
	}
	return resp, nil
}

func hostPort(u *url.URL) (string, int) {
	host := u.Hostname()
	if p := u.Port(); p != "" {
		n, _ := strconv.Atoi(p) // a URL that parsed has a numeric port
		return host, n
	}
	switch u.Scheme {
	case "https":
		return host, 443
	case "http":
		return host, 80
	}
	return host, 0
}

func sanitizedURL(u *url.URL) string {
	c := *u
	c.User = nil
	c.RawQuery = ""
	c.ForceQuery = false
	c.Fragment = ""
	return c.String()
}
