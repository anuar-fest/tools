package metrics_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"

	"github.com/anuar-fest/tools/metrics"
)

func attrValue(set attribute.Set, key string) string {
	v, ok := set.Value(attribute.Key(key))
	if !ok {
		return ""
	}
	return v.String()
}

func TestNew_ResourceReadersAndPrometheus(t *testing.T) {
	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	m, err := metrics.New(ctx, metrics.Config{
		ServiceName: "svc", ServiceVersion: "1.2.3", Environment: "test", InstanceID: "pod-1",
		Readers:     []sdkmetric.Reader{reader},
		Runtime:     true,
		SkipGlobals: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	res := *m.Resource().Set()
	for k, want := range map[string]string{"service.name": "svc", "service.version": "1.2.3", "deployment.environment.name": "test", "service.instance.id": "pod-1", "telemetry.sdk.language": "go"} {
		if got := attrValue(res, k); got != want {
			t.Errorf("resource %s = %q, want %q", k, got, want)
		}
	}
	ctr, _ := m.Meter("m").Int64Counter("widgets")
	ctr.Add(ctx, 3, metric.WithAttributes(attribute.String("kind", "a"), attribute.String("user.id", "u1")))
	ms := collect(t, reader)
	dp := ms["widgets"].Data.(metricdata.Sum[int64]).DataPoints[0]
	if dp.Value != 3 {
		t.Fatalf("counter must reach the manual reader: %+v", dp)
	}
	if _, ok := dp.Attributes.Value("user.id"); ok {
		t.Fatal("the Guard view is on by default")
	}
	if _, ok := ms["go.goroutine.count"]; !ok {
		t.Fatal("Runtime registers the runtime instruments")
	}
	rr := httptest.NewRecorder()
	m.Handler().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rr.Body.String()
	for _, want := range []string{"go_goroutines", "process_", "widgets_total", `target_info{`, `service_name="svc"`} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics must contain %q:\n%s", want, body)
		}
	}
	if strings.Contains(body, "otel_scope_name") {
		t.Error("scope labels are off by default")
	}
	if err := m.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := m.Shutdown(ctx); err != nil {
		t.Fatal("Shutdown must be idempotent:", err)
	}
}

func TestNew_SharedResourceAndDisabled(t *testing.T) {
	res := resource.NewSchemaless(attribute.String("service.name", "from-traces"))
	m, err := metrics.New(context.Background(), metrics.Config{Resource: res, Disabled: true, SkipGlobals: true})
	if err != nil {
		t.Fatal(err)
	}
	if m.Resource() != res {
		t.Fatal("Resource must be used as given")
	}
	rr := httptest.NewRecorder()
	m.Handler().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rr.Code != http.StatusNotFound || m.Registry() != nil {
		t.Fatal("disabled: no registry, 404 handler")
	}
}

func TestNew_Validation(t *testing.T) {
	if _, err := metrics.New(context.Background(), metrics.Config{SkipGlobals: true}); err == nil {
		t.Fatal("ServiceName is required without a Resource")
	}
}

func TestNew_SharesAnExistingPrometheusRegistry(t *testing.T) {
	reg := prometheus.NewRegistry()
	legacy := prometheus.NewCounter(prometheus.CounterOpts{Name: "legacy_total", Help: "promauto era"})
	reg.MustRegister(legacy)
	legacy.Inc()
	m, err := metrics.New(context.Background(), metrics.Config{ServiceName: "svc", SkipGlobals: true,
		Prometheus: metrics.PrometheusConfig{Registry: reg, Namespace: "svc"}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = m.Shutdown(context.Background()) }()
	ctr, _ := m.Meter("m").Int64Counter("new")
	ctr.Add(context.Background(), 1)
	rr := httptest.NewRecorder()
	m.Handler().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if body := rr.Body.String(); !strings.Contains(body, "legacy_total 1") || !strings.Contains(body, "svc_new_total 1") {
		t.Fatalf("one /metrics must serve both generations:\n%s", body)
	}
}

func TestNew_SetsTheGlobalProvider(t *testing.T) {
	before := otel.GetMeterProvider()
	t.Cleanup(func() { otel.SetMeterProvider(before) })
	m, err := metrics.New(context.Background(), metrics.Config{ServiceName: "svc", Disabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if otel.GetMeterProvider() != m.MeterProvider() {
		t.Fatal("the global must be the provider New built")
	}
}
