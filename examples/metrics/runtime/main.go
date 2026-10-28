// Command runtime shows the Go runtime instruments for a setup without
// the Prometheus collectors: an OTLP-only service turns Runtime on, and
// go.goroutine.count, go.memory.* and go.processor.limit are pushed with
// everything else. A manual reader prints them here instead of a collector.
//
//	go run ./metrics/runtime
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/anuar-fest/tools/metrics"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	reader := sdkmetric.NewManualReader()
	m, err := metrics.New(ctx, metrics.Config{
		ServiceName:       "runtime-example",
		DisablePrometheus: true, // no /metrics, so no client_golang collectors: register the OTel runtime instruments instead
		Runtime:           true,
		Readers:           []sdkmetric.Reader{reader}, // a service sets OTLP.Endpoint; the reader stands in for the collector
	})
	if err != nil {
		return err
	}
	defer func() { _ = m.Shutdown(context.WithoutCancel(ctx)) }() // flush even if ctx ended

	// Some goroutines and garbage to look at.
	stop := make(chan struct{})
	for range 50 {
		go func() { <-stop }() //nolint:contextcheck // parked on purpose; closed below
	}
	defer close(stop)

	for range 3 {
		var rm metricdata.ResourceMetrics
		if err := reader.Collect(ctx, &rm); err != nil {
			return err
		}
		for _, sm := range rm.ScopeMetrics {
			for _, mt := range sm.Metrics {
				switch d := mt.Data.(type) {
				case metricdata.Gauge[int64]:
					fmt.Printf("%-22s %d %s\n", mt.Name, d.DataPoints[0].Value, mt.Unit)
				case metricdata.Sum[int64]:
					fmt.Printf("%-22s %d %s (cumulative)\n", mt.Name, d.DataPoints[0].Value, mt.Unit)
				}
			}
		}
		fmt.Println("--")
		time.Sleep(time.Second)
	}
	return nil
}
