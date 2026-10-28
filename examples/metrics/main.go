// Command metrics shows the one meter provider a service installs:
// Prometheus on an admin port with the Go and process collectors, the
// cardinality Guard, and business metrics through the Builder with closed
// label sets and a cap, so a bad label cannot grow the scrape.
//
//	go run ./metrics
//	curl -s localhost:9090/metrics | grep -E 'shop_|go_goroutines|target_info'
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/anuar-fest/tools/httpserver"
	"github.com/anuar-fest/tools/lifecycle"
	"github.com/anuar-fest/tools/metrics"
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
	fmt.Fprintf(os.Stderr, "curl -s localhost%s/metrics | grep shop_\n", adminAddr)
	return lifecycle.ExitCode(app.Run(ctx))
}

func build(ctx context.Context, adminAddr string, logger *slog.Logger) (*lifecycle.App, error) {
	m, err := metrics.New(ctx, metrics.Config{
		ServiceName: "shop", ServiceVersion: "dev", Environment: "local",
		// The default Guard drops ids, paths and query text everywhere. An allow list per instrument goes further: only these keys survive.
		Views:   []sdkmetric.View{metrics.Guard(metrics.GuardConfig{Allow: map[string][]attribute.Key{"shop.orders": {"status"}}})},
		NoGuard: true,
		Logger:  logger,
	})
	if err != nil {
		return nil, err
	}
	b, err := metrics.NewBuilder(metrics.BuilderConfig{Namespace: "shop", Logger: logger, DefaultMaxCardinality: 100})
	if err != nil {
		return nil, err
	}
	// Names are validated at boot: no _total suffix, no uppercase, no reserved labels, Help required.
	orders, err := b.Counter(metrics.Definition{Name: "orders", Help: "Orders by outcome.", Unit: "{order}", Labels: []string{"status"}})
	if err != nil {
		return nil, err
	}
	checkout, err := b.Histogram(metrics.Definition{Name: "checkout_duration", Help: "Checkout latency.", Unit: "s", Labels: []string{"payment"}})
	if err != nil {
		return nil, err
	}
	queue, err := b.Gauge(metrics.Definition{Name: "fulfilment_queue_depth", Help: "Orders waiting for fulfilment."})
	if err != nil {
		return nil, err
	}
	// A metric keyed by order id would be a bug: beyond the cap the values become "_other" and shop.cardinality_limited counts it.
	byOrder, err := b.Counter(metrics.Definition{Name: "order_events", Help: "Demonstrates the cap.", Labels: []string{"order_id"}, MaxCardinality: 5})
	if err != nil {
		return nil, err
	}

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
	app.Add(lifecycle.Component{Name: "metrics", Stop: m.Shutdown}) // added first, stops last: the final scrape still works while the rest drains
	app.Add(lifecycle.Component{Name: "admin-http", Run: adminSrv.Run})
	app.Add(lifecycle.Component{Name: "shop", Run: func(ctx context.Context) error {
		t := time.NewTicker(300 * time.Millisecond)
		defer t.Stop()
		for i := 0; ; i++ {
			select {
			case <-ctx.Done():
				return nil
			case <-t.C:
				start := time.Now().Add(-time.Duration(rand.IntN(400)) * time.Millisecond)
				status, payment := "ok", "card"
				if i%7 == 0 {
					status, payment = "declined", "wallet"
				}
				orders.Inc(ctx, status)
				checkout.Since(ctx, start, payment)
				queue.Set(ctx, float64(rand.IntN(20)))
				byOrder.Inc(ctx, fmt.Sprintf("o-%d", i))
				orders.Inc(ctx, "ok", "extra") // wrong arity: recorded under _invalid and logged once
			}
		}
	}})
	return app, nil
}
