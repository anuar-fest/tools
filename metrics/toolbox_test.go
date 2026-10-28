package metrics_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/anuar-fest/tools/metrics"
)

func collect(t *testing.T, reader *sdkmetric.ManualReader) map[string]metricdata.Metrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	out := map[string]metricdata.Metrics{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			out[m.Name] = m
		}
	}
	return out
}

func TestPrometheus_ServesCollectorsAndOTelInstruments(t *testing.T) {
	p, err := metrics.NewPrometheus(metrics.PrometheusConfig{Namespace: "svc"})
	if err != nil {
		t.Fatal(err)
	}
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(p.Reader()))
	defer func() { _ = mp.Shutdown(context.Background()) }()
	ctr, _ := mp.Meter("m").Int64Counter("widgets")
	ctr.Add(context.Background(), 2)
	rr := httptest.NewRecorder()
	p.Handler().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, want := range []string{"go_goroutines", "svc_widgets_total 2"} {
		if !strings.Contains(rr.Body.String(), want) {
			t.Errorf("missing %q in\n%s", want, rr.Body.String())
		}
	}
	if p.Registry() == nil {
		t.Fatal("registry must be exposed")
	}
}

func TestGuard_DropsHighCardinalityAndKeepsBuckets(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader), sdkmetric.WithView(metrics.Guard(metrics.GuardConfig{
		Allow: map[string][]attribute.Key{"narrow": {"kept"}},
	})))
	m := mp.Meter("m")
	h, _ := m.Float64Histogram("lat", metric.WithExplicitBucketBoundaries(0.1, 1))
	h.Record(context.Background(), 0.5, metric.WithAttributes(attribute.String("http.route", "/x"), attribute.String("user.id", "u1"), attribute.String("url.path", "/x/1")))
	c, _ := m.Int64Counter("narrow")
	c.Add(context.Background(), 1, metric.WithAttributes(attribute.String("kept", "a"), attribute.String("dropped", "b"), attribute.String("user.id", "u")))
	ms := collect(t, reader)
	hist := ms["lat"].Data.(metricdata.Histogram[float64])
	if len(hist.DataPoints) != 1 || len(hist.DataPoints[0].Bounds) != 2 {
		t.Fatalf("explicit buckets must survive the view: %+v", hist.DataPoints)
	}
	if _, ok := hist.DataPoints[0].Attributes.Value("user.id"); ok {
		t.Fatal("user.id must be dropped")
	}
	if _, ok := hist.DataPoints[0].Attributes.Value("http.route"); !ok {
		t.Fatal("http.route must be kept")
	}
	dp := ms["narrow"].Data.(metricdata.Sum[int64]).DataPoints[0]
	if _, ok := dp.Attributes.Value("kept"); !ok || dp.Attributes.Len() != 1 {
		t.Fatalf("allow list must keep only 'kept': %v", dp.Attributes)
	}
}

func TestBuilder_ValidatesAndCapsCardinality(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	var buf bytes.Buffer
	b, err := metrics.NewBuilder(metrics.BuilderConfig{Meter: mp.Meter("m"), Namespace: "customers", Logger: slog.New(slog.NewJSONHandler(&buf, nil)), DefaultMaxCardinality: 2})
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range []metrics.Definition{
		{Name: "Usage", Help: "h"},
		{Name: "usage_total", Help: "h"},
		{Name: "usage", Help: ""},
		{Name: "usage", Help: "h", Labels: []string{"le"}},
		{Name: "usage", Help: "h", Labels: []string{"a", "a"}},
		{Name: "usage", Help: "h", Labels: []string{"Bad-Label"}},
	} {
		if _, err := b.Counter(bad); err == nil {
			t.Errorf("%+v must be rejected", bad)
		}
	}
	if _, err := metrics.NewBuilder(metrics.BuilderConfig{Namespace: "Bad"}); err == nil {
		t.Fatal("bad namespace must fail")
	}
	ctr, err := b.Counter(metrics.Definition{Name: "usage_events", Help: "Usage events.", Unit: "{event}", Labels: []string{"event", "status"}})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	ctr.Inc(ctx, "stt", "applied")
	ctr.Inc(ctx, "stt", "applied")
	ctr.Inc(ctx, "tts", "applied")
	ctr.Inc(ctx, "x", "y") // third combination: over the cap
	ctr.Inc(ctx, "only-one")
	hist, _ := b.Histogram(metrics.Definition{Name: "sync_duration", Help: "d", Unit: "s", Labels: []string{"kind"}})
	hist.Since(ctx, time.Now().Add(-10*time.Millisecond), "full")
	g, _ := b.Gauge(metrics.Definition{Name: "queue_depth", Help: "q"})
	g.Set(ctx, 7)

	ms := collect(t, reader)
	sum := ms["customers.usage_events"].Data.(metricdata.Sum[int64])
	got := map[string]int64{}
	for _, dp := range sum.DataPoints {
		ev, _ := dp.Attributes.Value("event")
		st, _ := dp.Attributes.Value("status")
		got[ev.AsString()+"/"+st.AsString()] = dp.Value
	}
	if got["stt/applied"] != 2 || got["tts/applied"] != 1 || got["_other/_other"] != 1 || got["_invalid/_invalid"] != 1 {
		t.Fatalf("series = %v", got)
	}
	if limited := ms["metrics.cardinality_limited"].Data.(metricdata.Sum[int64]).DataPoints[0].Value; limited != 1 {
		t.Fatalf("limited = %d", limited)
	}
	if !strings.Contains(buf.String(), "cardinality cap") || !strings.Contains(buf.String(), "metric misuse") {
		t.Fatalf("misuse and cap must be logged once: %s", buf.String())
	}
	if h := ms["customers.sync_duration"].Data.(metricdata.Histogram[float64]); h.DataPoints[0].Count != 1 || len(h.DataPoints[0].Bounds) != len(metrics.LatencyBuckets) {
		t.Fatalf("histogram: %+v", h.DataPoints)
	}
	if gv := ms["customers.queue_depth"].Data.(metricdata.Gauge[float64]).DataPoints[0].Value; gv != 7 {
		t.Fatalf("gauge = %v", gv)
	}
}

type timeoutError struct{}

func (timeoutError) Error() string   { return "t" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return false }

func TestErrorType(t *testing.T) {
	cases := map[string]error{
		"":                         nil,
		"context.Canceled":         context.Canceled,
		"context.DeadlineExceeded": context.DeadlineExceeded,
		"io.EOF":                   io.EOF,
		"timeout":                  &net.OpError{Err: timeoutError{}},
		"errors.errorString":       errors.New("x"),
	}
	for want, err := range cases {
		if got := metrics.ErrorType(err); got != want {
			t.Errorf("ErrorType(%v) = %q, want %q", err, got, want)
		}
	}
}
