// Command health shows liveness versus readiness: a registry with a startup
// gate, cached concurrent checks, a readiness-change hook, and the drain
// flag that lifecycle flips before anything stops.
//
//	go run ./health -fail-after 20s
//	curl -i localhost:9090/healthz/ready   # 503 starting → 200 ok → 503 after -fail-after → 503 draining on SIGTERM
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/anuar-fest/tools/health"
	"github.com/anuar-fest/tools/httpserver"
	"github.com/anuar-fest/tools/lifecycle"
)

func main() {
	addr := flag.String("admin-addr", ":9090", "admin listen address")
	failAfter := flag.Duration("fail-after", 0, "make the simulated database check fail after this long (0 = never)")
	flag.Parse()
	os.Exit(run(context.Background(), *addr, *failAfter))
}

func run(ctx context.Context, addr string, failAfter time.Duration) int {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	start := time.Now()

	hc := health.New(health.Config{
		StartupGate: true, // readiness fails until MarkStarted: the service decides when it has loaded what it needs
		Timeout:     2 * time.Second,
		Info:        map[string]string{"name": "health-example", "version": "dev"},
		Logger:      logger,
		OnReadinessChange: func(from, to health.Report) { // hook alarms here; it runs on the probe's goroutine
			logger.Info("readiness changed", "from", from.Status, "to", to.Status)
		},
	})
	// Liveness answers "is the process wedged"; readiness "may traffic come". A dependency outage is a readiness matter only.
	hc.AddLiveness("event-loop", func(context.Context) error { return nil })
	hc.AddReadiness("database", func(context.Context) error {
		if failAfter > 0 && time.Since(start) > failAfter {
			return errors.New("connection refused")
		}
		return nil
	})
	hc.AddReadiness("broker", health.Bool(func() bool { return true }, "nats disconnected"))

	mux := http.NewServeMux()
	mux.Handle("GET /healthz/live", hc.LiveHandler())
	mux.Handle("GET /healthz/ready", hc.ReadyHandler())
	srv, err := httpserver.New(httpserver.Config{Addr: addr, Handler: mux, Logger: logger})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if err := srv.Listen(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	app := lifecycle.New(lifecycle.Config{
		Logger:     logger,
		OnShutdown: func(string) { hc.SetDraining(true) }, // first thing on SIGTERM: readiness goes 503 so the balancer stops sending traffic
	})
	app.Add(lifecycle.Component{Name: "admin-http", Run: srv.Run})
	app.Add(lifecycle.Component{Name: "warm-up", Run: func(ctx context.Context) error {
		select { // load caches, run migrations, then open the gate
		case <-time.After(time.Second):
			hc.MarkStarted()
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}})
	return lifecycle.ExitCode(app.Run(ctx))
}
