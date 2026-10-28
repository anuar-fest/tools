package metricshttp

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"

	"github.com/anuar-fest/tools/metrics"
)

// TransportConfig configures Transport.
type TransportConfig struct {
	// Meter defaults to the global meter named InstrumentationName.
	Meter metric.Meter
	// PeerService names the dependency (peer.service) on the metric so one
	// histogram serves every outbound client.
	PeerService string
}

// Transport wraps next and records http.client.request.duration by method,
// server address, peer.service and status; error.type is the status code
// from 400 up or the transport error's classification. Timed up to the
// response headers; reading the body is not. Put it inside
// clienthttp.Transport so the sample carries the span as an exemplar, and
// around the resilience/httpx transport so retries are one sample:
//
//	tr, _ := httpx.NewTransport(httpx.TransportConfig{...})
//	client := &http.Client{Transport: clienthttp.Transport(metricshttp.Transport(tr, metricshttp.TransportConfig{PeerService: "fraud"}), tcfg)}
//
// Attributes are the closed subset: no URL, no peer-specific text beyond
// the host. A nil next is http.DefaultTransport, which has no timeouts;
// prefer httpx.
func Transport(next http.RoundTripper, cfg TransportConfig) (http.RoundTripper, error) {
	if next == nil {
		next = http.DefaultTransport
	}
	if cfg.Meter == nil {
		cfg.Meter = otel.GetMeterProvider().Meter(InstrumentationName)
	}
	duration, err := cfg.Meter.Float64Histogram("http.client.request.duration",
		metric.WithDescription("Duration of outbound HTTP requests up to the response headers."),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(metrics.LatencyBuckets...))
	if err != nil {
		return nil, err
	}
	return &transport{next: next, cfg: cfg, duration: duration}, nil
}

type transport struct {
	next     http.RoundTripper
	cfg      TransportConfig
	duration metric.Float64Histogram
}

func (t *transport) RoundTrip(req *http.Request) (*http.Response, error) {
	attrs := make([]attribute.KeyValue, 0, 5)
	attrs = append(attrs, semconv.HTTPRequestMethodKey.String(req.Method), semconv.ServerAddress(hostOf(req.URL)))
	if t.cfg.PeerService != "" {
		attrs = append(attrs, attribute.String("peer.service", t.cfg.PeerService))
	}
	start := time.Now()
	resp, err := t.next.RoundTrip(req)
	if err != nil {
		t.record(req.Context(), start, append(attrs, semconv.ErrorTypeKey.String(metrics.ErrorType(err))))
		return nil, err
	}
	attrs = append(attrs, semconv.HTTPResponseStatusCode(resp.StatusCode))
	if resp.StatusCode >= http.StatusBadRequest {
		attrs = append(attrs, semconv.ErrorTypeKey.String(strconv.Itoa(resp.StatusCode)))
	}
	t.record(req.Context(), start, attrs)
	return resp, nil
}

func (t *transport) record(ctx context.Context, start time.Time, attrs []attribute.KeyValue) {
	t.duration.Record(ctx, time.Since(start).Seconds(), metric.WithAttributeSet(attribute.NewSet(attrs...)))
}

func hostOf(u *url.URL) string {
	if u == nil {
		return ""
	}
	return u.Hostname()
}
