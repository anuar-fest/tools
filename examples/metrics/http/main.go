// Command http shows HTTP metrics on both sides: the Observer behind
// httpserver.Metrics exports http.server.* by method, route and status,
// and the Transport records http.client.request.duration per peer service.
// An in-process upstream is the dependency.
//
//	go run ./metrics/http
//	curl localhost:8080/api/items/7 ; curl localhost:8080/api/upstream
//	curl -s localhost:9090/metrics | grep -E 'http_(server|client)_'
package main

import (
	"context"
	"encoding/json"
	"flag"
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/anuar-fest/tools/httpserver"
	"github.com/anuar-fest/tools/lifecycle"
	"github.com/anuar-fest/tools/metrics"
	metricshttp "github.com/anuar-fest/tools/metrics/http"
	"github.com/anuar-fest/tools/resilience/httpx"
)

func main() {
	addr := flag.String("addr", ":8080", "API listen address")
	adminAddr := flag.String("admin-addr", ":9090", "admin listen address for /metrics")
	flag.Parse()
	os.Exit(run(context.Background(), *addr, *adminAddr))
}

func run(ctx context.Context, addr, adminAddr string) int {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	app, err := build(ctx, addr, adminAddr, logger)
	if err != nil {
		logger.Error("boot failed", "err", err)
		return 1
	}
	return lifecycle.ExitCode(app.Run(ctx))
}

func build(ctx context.Context, addr, adminAddr string, logger *slog.Logger) (*lifecycle.App, error) {
	m, err := metrics.New(ctx, metrics.Config{ServiceName: "metricshttp-example", Logger: logger})
	if err != nil {
		return nil, err
	}
	upstream, err := httpserver.New(httpserver.Config{Addr: "127.0.0.1:0", Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok") })})
	if err != nil {
		return nil, err
	}
	if err = upstream.Listen(ctx); err != nil {
		return nil, err
	}

	// Outbound: the metrics transport inside the tracing one (when both are used) so the sample carries the span as exemplar.
	base, err := httpx.NewTransport(httpx.TransportConfig{Name: "upstream", CallTimeout: 2 * time.Second})
	if err != nil {
		return nil, err
	}
	tr, err := metricshttp.Transport(base, metricshttp.TransportConfig{PeerService: "upstream"})
	if err != nil {
		return nil, err
	}
	client := &http.Client{Transport: tr}

	// Inbound: the Observer implements httpserver.Observer; Routed makes the route label a pattern, never a path.
	obs, err := metricshttp.NewObserver(metricshttp.Config{})
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/items/{id}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"id": r.PathValue("id")})
	})
	mux.HandleFunc("GET /api/upstream", func(w http.ResponseWriter, r *http.Request) {
		req, _ := http.NewRequestWithContext(r.Context(), http.MethodGet, "http://"+upstream.Addr().String()+"/", nil)
		resp, callErr := client.Do(req)
		if callErr != nil {
			httpserver.Fail(w, r, http.StatusBadGateway, "upstream did not answer")
			return
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /api/panic", func(http.ResponseWriter, *http.Request) { panic("deliberate: error_type=panic on the metric") })
	// Metrics outside Recover so the 500 from a panic is counted; the tracing middleware (otelx/server/http) would go between them.
	h := httpserver.Metrics(obs, nil)(httpserver.Recover(logger)(httpserver.Routed(mux)))
	srv, err := httpserver.New(httpserver.Config{Addr: addr, Handler: h, Logger: logger})
	if err != nil {
		return nil, err
	}
	if err = srv.Listen(ctx); err != nil {
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
	app.Add(lifecycle.Component{Name: "http", Run: srv.Run})
	return app, nil
}
