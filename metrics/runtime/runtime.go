// Package metricsruntime exports the Go runtime as OpenTelemetry
// instruments for setups without the Prometheus Go collectors (OTLP-only).
// With Prometheus on, the client_golang collectors already serve go_* and
// process_* series; registering this too is harmless but redundant.
package metricsruntime

import (
	"context"
	"runtime"
	runtimemetrics "runtime/metrics"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// InstrumentationName is the meter name.
const InstrumentationName = "github.com/anuar-fest/tools/metrics/runtime"

// Register exports the runtime instruments on meter (nil: the global meter
// named InstrumentationName):
//
//	go.goroutine.count        goroutines
//	go.memory.used            heap bytes held by live and unswept objects
//	go.memory.allocated       cumulative bytes allocated
//	go.memory.allocations     cumulative objects allocated
//	go.memory.gc.goal         heap size the next GC aims for
//	go.processor.limit        GOMAXPROCS
//
// Unregister the result on shutdown.
func Register(meter metric.Meter) (metric.Registration, error) {
	if meter == nil {
		meter = otel.Meter(InstrumentationName)
	}
	goroutines, err := meter.Int64ObservableGauge("go.goroutine.count", metric.WithDescription("Live goroutines."), metric.WithUnit("{goroutine}"))
	if err != nil {
		return nil, err
	}
	used, err := meter.Int64ObservableGauge("go.memory.used", metric.WithDescription("Heap memory in use."), metric.WithUnit("By"))
	if err != nil {
		return nil, err
	}
	allocated, err := meter.Int64ObservableCounter("go.memory.allocated", metric.WithDescription("Bytes allocated since start."), metric.WithUnit("By"))
	if err != nil {
		return nil, err
	}
	allocations, err := meter.Int64ObservableCounter("go.memory.allocations", metric.WithDescription("Objects allocated since start."), metric.WithUnit("{allocation}"))
	if err != nil {
		return nil, err
	}
	goal, err := meter.Int64ObservableGauge("go.memory.gc.goal", metric.WithDescription("Heap size the next GC cycle aims for."), metric.WithUnit("By"))
	if err != nil {
		return nil, err
	}
	procs, err := meter.Int64ObservableGauge("go.processor.limit", metric.WithDescription("GOMAXPROCS."), metric.WithUnit("{thread}"))
	if err != nil {
		return nil, err
	}
	samples := []runtimemetrics.Sample{
		{Name: "/sched/goroutines:goroutines"},
		{Name: "/memory/classes/heap/objects:bytes"},
		{Name: "/gc/heap/allocs:bytes"},
		{Name: "/gc/heap/allocs:objects"},
		{Name: "/gc/heap/goal:bytes"},
	}
	return meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		runtimemetrics.Read(samples)
		o.ObserveInt64(goroutines, uint64Of(samples[0]))
		o.ObserveInt64(used, uint64Of(samples[1]))
		o.ObserveInt64(allocated, uint64Of(samples[2]))
		o.ObserveInt64(allocations, uint64Of(samples[3]))
		o.ObserveInt64(goal, uint64Of(samples[4]))
		o.ObserveInt64(procs, int64(runtime.GOMAXPROCS(0)))
		return nil
	}, goroutines, used, allocated, allocations, goal, procs)
}

func uint64Of(s runtimemetrics.Sample) int64 {
	if s.Value.Kind() != runtimemetrics.KindUint64 {
		return 0
	}
	v := s.Value.Uint64()
	if v > 1<<62 {
		return 1 << 62
	}
	return int64(v)
}
