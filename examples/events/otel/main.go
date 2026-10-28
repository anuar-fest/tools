// Command otel shows tracing and metrics for events: the trace context
// travels in the envelope, the consumer span is a child of the producer's,
// and the Observer exports messages and durations by consumer, type and
// outcome. The in-process bus stands in for a broker.
//
//	go run ./events/otel
//	curl -s localhost:9090/metrics | grep events_
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"

	"github.com/anuar-fest/tools/events"
	eventsotel "github.com/anuar-fest/tools/events/otel"
	"github.com/anuar-fest/tools/httpserver"
	"github.com/anuar-fest/tools/lifecycle"
	"github.com/anuar-fest/tools/metrics"
	"github.com/anuar-fest/tools/otelx"
)

func main() {
	adminAddr := flag.String("admin-addr", ":9090", "admin listen address for /metrics")
	flag.Parse()
	os.Exit(run(context.Background(), *adminAddr))
}

func run(ctx context.Context, adminAddr string) int {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	app, err := build(ctx, adminAddr, logger)
	if err != nil {
		logger.Error("boot failed", "err", err)
		return 1
	}
	return lifecycle.ExitCode(app.Run(ctx))
}

func build(ctx context.Context, adminAddr string, logger *slog.Logger) (*lifecycle.App, error) {
	exporter, err := stdouttrace.New(stdouttrace.WithPrettyPrint()) // spans to stdout; a service points Traces.Endpoint at a collector
	if err != nil {
		return nil, err
	}
	tel, err := otelx.New(ctx, otelx.Config{ServiceName: "events-otel-example", Traces: otelx.TracesConfig{Exporter: exporter}, Logger: logger})
	if err != nil {
		return nil, err
	}
	m, err := metrics.New(ctx, metrics.Config{Resource: tel.Resource(), Logger: logger})
	if err != nil {
		return nil, err
	}
	obs, err := eventsotel.NewObserver(eventsotel.Config{}) // events.messages, events.message.duration
	if err != nil {
		return nil, err
	}

	// Consumer side: Traced continues the producer's trace; Observe records the outcome.
	handler := events.Chain(func(ctx context.Context, e events.Envelope) error {
		logger.InfoContext(ctx, "projected", "id", e.ID) // in a real service otelx.LogAttrs puts trace_id on this line
		return nil
	}, eventsotel.Traced(nil, "projector"), events.Observe(obs, "projector"))
	bus := events.PublisherFunc(func(ctx context.Context, e events.Envelope) error {
		return handler(events.WithDelivery(ctx, events.Delivery{Attempt: 1, Consumer: "projector"}), e)
	})
	// Producer side: TracedPublisher starts the producer span and injects traceparent into the envelope.
	pub := eventsotel.TracedPublisher(bus, nil)

	admin := http.NewServeMux()
	admin.Handle("GET /metrics", m.Handler())
	adminSrv, err := httpserver.New(httpserver.Config{Addr: adminAddr, Handler: admin, Logger: logger})
	if err != nil {
		return nil, err
	}
	if err := adminSrv.Listen(ctx); err != nil {
		return nil, err
	}
	app := lifecycle.New(lifecycle.Config{Logger: logger})
	app.Add(lifecycle.Component{Name: "telemetry", Stop: tel.Shutdown})
	app.Add(lifecycle.Component{Name: "metrics", Stop: m.Shutdown})
	app.Add(lifecycle.Component{Name: "admin-http", Run: adminSrv.Run})
	app.Add(lifecycle.Component{Name: "producer", Run: func(ctx context.Context) error {
		t := time.NewTicker(3 * time.Second)
		defer t.Stop()
		for i := 1; ; i++ {
			select {
			case <-ctx.Done():
				return nil
			case <-t.C:
				e := events.New("orders.order.placed.v1", "orders-api", []byte(fmt.Sprintf(`{"order_id":"o-%d"}`, i)), events.WithTenant("acme"))
				if err := pub.Publish(ctx, e); err != nil {
					logger.Warn("publish failed", "err", err)
				}
			}
		}
	}})
	return app, nil
}
