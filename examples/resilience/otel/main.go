// Command otel shows the resilience Observer on OpenTelemetry: every
// decision, duration, breaker transition and bulkhead saturation reaches
// /metrics, and rejections become events on the active span.
//
//	go run ./resilience/otel -fail-ratio 0.7
//	curl -s localhost:9090/metrics | grep -E 'resilience_(decisions_total|breaker_state|bulkhead)'
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"time"

	"github.com/anuar-fest/tools/httpserver"
	"github.com/anuar-fest/tools/lifecycle"
	"github.com/anuar-fest/tools/metrics"
	"github.com/anuar-fest/tools/resilience/breaker"
	"github.com/anuar-fest/tools/resilience/bulkhead"
	"github.com/anuar-fest/tools/resilience/httpx"
	resilienceotel "github.com/anuar-fest/tools/resilience/otel"
)

func main() {
	adminAddr := flag.String("admin-addr", ":9090", "admin listen address for /metrics")
	failRatio := flag.Float64("fail-ratio", 0.5, "fraction of upstream calls that answer 503")
	flag.Parse()
	os.Exit(run(context.Background(), *adminAddr, *failRatio))
}

func run(ctx context.Context, adminAddr string, failRatio float64) int {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	app, err := build(ctx, adminAddr, failRatio, logger)
	if err != nil {
		logger.Error("boot failed", "err", err)
		return 1
	}
	return lifecycle.ExitCode(app.Run(ctx))
}

func build(ctx context.Context, adminAddr string, failRatio float64, logger *slog.Logger) (*lifecycle.App, error) {
	m, err := metrics.New(ctx, metrics.Config{ServiceName: "resilience-otel-example", Logger: logger})
	if err != nil {
		return nil, err
	}
	obs, err := resilienceotel.New(resilienceotel.Config{}) // the global meter provider metrics.New just installed
	if err != nil {
		return nil, err
	}
	bh, err := bulkhead.New(bulkhead.Config{Name: "upstream", MaxConcurrent: 4, Observer: obs})
	if err != nil {
		return nil, err
	}
	br, err := breaker.New(breaker.Config{Name: "upstream", Trip: breaker.ConsecutiveFailures(3), OpenFor: 5 * time.Second, Observer: obs})
	if err != nil {
		return nil, err
	}
	tr, err := httpx.NewTransport(httpx.TransportConfig{Name: "upstream", CallTimeout: 2 * time.Second, AttemptTimeout: 500 * time.Millisecond, Bulkhead: bh, Breaker: br, Observer: obs})
	if err != nil {
		return nil, err
	}
	if _, err = obs.RegisterBulkhead(bh); err != nil { // saturation gauges; lives as long as the process
		return nil, err
	}

	upstream, err := httpserver.New(httpserver.Config{Addr: "127.0.0.1:0", Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if rand.Float64() < failRatio {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	})})
	if err != nil {
		return nil, err
	}
	if err = upstream.Listen(ctx); err != nil {
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
	app.Add(lifecycle.Component{Name: "metrics", Stop: m.Shutdown})
	app.Add(lifecycle.Component{Name: "admin-http", Run: adminSrv.Run})
	app.Add(lifecycle.Component{Name: "upstream", Run: upstream.Run})
	app.Add(lifecycle.Component{Name: "caller", Run: func(ctx context.Context) error {
		client := &http.Client{Transport: tr}
		url := "http://" + upstream.Addr().String() + "/"
		t := time.NewTicker(200 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-t.C:
				req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
				resp, err := client.Do(req)
				if err != nil {
					logger.Warn("call failed", "err", err) // breaker_open shows up as resilience_decisions_total{decision="breaker_open"}
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
			}
		}
	}})
	fmt.Fprintf(os.Stderr, "curl -s localhost%s/metrics | grep resilience_\n", adminAddr)
	return app, nil
}
