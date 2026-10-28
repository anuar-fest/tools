package resilienceotel_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/anuar-fest/tools/resilience"
	"github.com/anuar-fest/tools/resilience/bulkhead"
	resilienceotel "github.com/anuar-fest/tools/resilience/otel"
	"github.com/anuar-fest/tools/resilience/workpool"
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

func sumInt(t *testing.T, m metricdata.Metrics, want ...attribute.KeyValue) int64 {
	t.Helper()
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("%s is %T, not Sum[int64]", m.Name, m.Data)
	}
	set := attribute.NewSet(want...)
	for _, dp := range sum.DataPoints {
		if dp.Attributes.Equals(&set) {
			return dp.Value
		}
	}
	return 0
}

func TestObserver_ExportsDecisionsDurationsAndBreakerState(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	o, err := resilienceotel.New(resilienceotel.Config{Meter: mp.Meter("test")})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	o.ObserveDecision(ctx, resilience.Event{Dependency: "fraud", Policy: resilience.PolicyRetry, Decision: resilience.DecisionRetry, Attempt: 1, Err: errors.New("x")})
	o.ObserveDecision(ctx, resilience.Event{Dependency: "fraud", Policy: resilience.PolicyRetry, Decision: resilience.DecisionRetry, Attempt: 2})
	o.ObserveDecision(ctx, resilience.Event{Dependency: "fraud", Policy: resilience.PolicyStack, Decision: resilience.DecisionSuccess, Elapsed: 120 * time.Millisecond})
	o.ObserveBreakerState(ctx, resilience.StateChange{Dependency: "fraud", From: resilience.BreakerClosed, To: resilience.BreakerOpen})

	ms := collect(t, reader)
	dec := ms["resilience.decisions"]
	if n := sumInt(t, dec,
		resilienceotel.AttrDependency.String("fraud"),
		resilienceotel.AttrPolicy.String("retry"),
		resilienceotel.AttrDecision.String("retry")); n != 2 {
		t.Fatalf("retry decisions = %d", n)
	}
	hist, ok := ms["resilience.call.duration"].Data.(metricdata.Histogram[float64])
	if !ok || len(hist.DataPoints) != 1 || hist.DataPoints[0].Count != 1 || hist.DataPoints[0].Sum < 0.1 {
		t.Fatalf("duration histogram = %+v", ms["resilience.call.duration"].Data)
	}
	gauge, ok := ms["resilience.breaker.state"].Data.(metricdata.Gauge[int64])
	if !ok || len(gauge.DataPoints) != 1 || gauge.DataPoints[0].Value != int64(resilience.BreakerOpen) {
		t.Fatalf("breaker state = %+v", ms["resilience.breaker.state"].Data)
	}
	if n := sumInt(t, ms["resilience.breaker.transitions"],
		resilienceotel.AttrDependency.String("fraud"),
		resilienceotel.AttrFrom.String("closed"),
		resilienceotel.AttrTo.String("open")); n != 1 {
		t.Fatalf("transitions = %d", n)
	}
}

func TestObserver_AddsSpanEventsForRejectionsOnly(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	o, _ := resilienceotel.New(resilienceotel.Config{Meter: mp.Meter("test")})

	ctx, span := tp.Tracer("t").Start(context.Background(), "call")
	o.ObserveDecision(ctx, resilience.Event{Dependency: "d", Policy: resilience.PolicyStack, Decision: resilience.DecisionSuccess})
	o.ObserveDecision(ctx, resilience.Event{Dependency: "d", Policy: resilience.PolicyBreaker, Decision: resilience.DecisionBreakerOpen})
	span.End()

	spans := exporter.GetSpans()
	if len(spans) != 1 || len(spans[0].Events) != 1 || spans[0].Events[0].Name != "resilience.decision" {
		t.Fatalf("spans = %+v", spans)
	}
}

func TestObserver_RegisterBulkheadAndPool(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	o, _ := resilienceotel.New(resilienceotel.Config{Meter: mp.Meter("test")})

	bh, _ := bulkhead.New(bulkhead.Config{Name: "db", MaxConcurrent: 3})
	release, _ := bh.Acquire(context.Background())
	defer release()
	reg, err := o.RegisterBulkhead(bh)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reg.Unregister() }()

	p, _ := workpool.New(workpool.Config{Name: "jobs", Workers: 1, QueueSize: 5})
	defer func() { _ = p.Close(context.Background()) }()
	preg, err := o.RegisterPool(p)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = preg.Unregister() }()

	ms := collect(t, reader)
	inflight := ms["resilience.bulkhead.inflight"].Data.(metricdata.Gauge[int64])
	capacity := ms["resilience.bulkhead.capacity"].Data.(metricdata.Gauge[int64])
	if inflight.DataPoints[0].Value != 1 || capacity.DataPoints[0].Value != 3 {
		t.Fatalf("inflight=%d capacity=%d", inflight.DataPoints[0].Value, capacity.DataPoints[0].Value)
	}
	qcap := ms["resilience.workpool.capacity"].Data.(metricdata.Gauge[int64])
	if qcap.DataPoints[0].Value != 5 {
		t.Fatalf("pool capacity = %d", qcap.DataPoints[0].Value)
	}
	if _, err := o.RegisterBulkhead(nil); err == nil {
		t.Fatal("nil bulkhead rejected")
	}
}
