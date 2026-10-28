// Package metricshttp measures HTTP on the OpenTelemetry API: an Observer
// for package httpserver that exports the http.server.* instruments of the
// HTTP semantic conventions, and a Transport that records outbound calls as
// http.client.request.duration. Spans are otelx/server/http and
// otelx/client/http; both sides share one bucket set so a server panel and
// a client panel line up.
package metricshttp

import (
	"context"
	"strconv"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"

	"github.com/anuar-fest/tools/httpserver"
	"github.com/anuar-fest/tools/metrics"
)

// InstrumentationName is the meter name.
const InstrumentationName = "github.com/anuar-fest/tools/metrics/http"

// Config configures NewObserver.
type Config struct {
	// Meter to create instruments on (default: the global MeterProvider).
	Meter metric.Meter
}

// Observer implements httpserver.Observer:
//
//	http.server.request.duration   histogram (s)   method, route, status, error.type
//	http.server.active_requests    up/down counter
//	http.server.request.body.size  histogram (By)  same attributes, when known
//	http.server.response.body.size histogram (By)
//
// The Prometheus exporter renders http_server_request_duration_seconds and
// friends. Route is a pattern, never a path, so cardinality stays flat.
type Observer struct {
	duration metric.Float64Histogram
	active   metric.Int64UpDownCounter
	reqSize  metric.Int64Histogram
	respSize metric.Int64Histogram
}

// NewObserver creates the instruments.
func NewObserver(cfg Config) (*Observer, error) {
	meter := cfg.Meter
	if meter == nil {
		meter = otel.GetMeterProvider().Meter(InstrumentationName)
	}
	o := &Observer{}
	var err error
	o.duration, err = meter.Float64Histogram("http.server.request.duration",
		metric.WithDescription("Duration of inbound HTTP requests."),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(metrics.LatencyBuckets...))
	if err != nil {
		return nil, err
	}
	o.active, err = meter.Int64UpDownCounter("http.server.active_requests",
		metric.WithDescription("Inbound requests in flight."), metric.WithUnit("{request}"))
	if err != nil {
		return nil, err
	}
	o.reqSize, err = meter.Int64Histogram("http.server.request.body.size",
		metric.WithDescription("Declared request body size."), metric.WithUnit("By"))
	if err != nil {
		return nil, err
	}
	o.respSize, err = meter.Int64Histogram("http.server.response.body.size",
		metric.WithDescription("Response body bytes written."), metric.WithUnit("By"))
	if err != nil {
		return nil, err
	}
	return o, nil
}

// ObserveInFlight implements httpserver.Observer.
func (o *Observer) ObserveInFlight(ctx context.Context, delta int) {
	o.active.Add(ctx, int64(delta))
}

// ObserveRequest implements httpserver.Observer.
func (o *Observer) ObserveRequest(ctx context.Context, s httpserver.RequestStat) {
	attrs := make([]attribute.KeyValue, 0, 4)
	attrs = append(attrs, semconv.HTTPRequestMethodKey.String(s.Method), semconv.HTTPResponseStatusCode(s.Status))
	if s.Route != "" {
		attrs = append(attrs, semconv.HTTPRoute(s.Route))
	}
	if et := serverErrorType(s.Status, s.Panicked); et != "" {
		attrs = append(attrs, semconv.ErrorTypeKey.String(et))
	}
	set := metric.WithAttributeSet(attribute.NewSet(attrs...))
	o.duration.Record(ctx, s.Elapsed.Seconds(), set)
	if s.BytesIn > 0 {
		o.reqSize.Record(ctx, s.BytesIn, set)
	}
	o.respSize.Record(ctx, s.BytesOut, set)
}

// serverErrorType follows the HTTP server conventions: 5xx responses carry
// the status code as error.type; a recovered panic says so.
func serverErrorType(status int, panicked bool) string {
	switch {
	case panicked:
		return "panic"
	case status >= 500:
		return strconv.Itoa(status)
	default:
		return ""
	}
}

var _ httpserver.Observer = (*Observer)(nil)
