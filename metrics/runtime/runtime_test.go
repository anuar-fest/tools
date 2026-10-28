package metricsruntime_test

import (
	"context"
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	metricsruntime "github.com/anuar-fest/tools/metrics/runtime"
)

func TestRegister(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	reg, err := metricsruntime.Register(mp.Meter("m"))
	if err != nil {
		t.Fatal(err)
	}
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	ms := map[string]metricdata.Metrics{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			ms[m.Name] = m
		}
	}
	if g := ms["go.goroutine.count"].Data.(metricdata.Gauge[int64]).DataPoints[0].Value; g < 1 {
		t.Fatalf("goroutines = %d", g)
	}
	for _, name := range []string{"go.memory.used", "go.memory.allocated", "go.memory.allocations", "go.memory.gc.goal", "go.processor.limit"} {
		if _, ok := ms[name]; !ok {
			t.Errorf("%s missing", name)
		}
	}
	if err := reg.Unregister(); err != nil {
		t.Fatal(err)
	}
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	for _, sm := range rm.ScopeMetrics {
		if len(sm.Metrics) != 0 {
			t.Fatal("unregistered instruments must stop reporting")
		}
	}
}
