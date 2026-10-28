// Command http shows server spans for an HTTP server you own: the caller's
// trace is continued (or a new one started), the span is renamed after the
// matched route, 5xx and panics set the error status, and the trace id is
// echoed in X-Trace-Id so a support ticket can quote it.
//
//	go run ./otelx/server/http
//	curl -i localhost:8080/api/items/7          # X-Trace-Id; the span "GET /api/items/{id}" is printed on stdout
//	curl -i localhost:8080/api/panic            # 500; the span carries error.type=panic
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"

	"github.com/anuar-fest/tools/httpserver"
	"github.com/anuar-fest/tools/lifecycle"
	"github.com/anuar-fest/tools/logx"
	"github.com/anuar-fest/tools/otelx"
	serverhttp "github.com/anuar-fest/tools/otelx/server/http"
)

func main() {
	addr := flag.String("addr", ":8080", "API listen address")
	flag.Parse()
	os.Exit(run(context.Background(), *addr))
}

func run(ctx context.Context, addr string) int {
	logger, err := logx.New(logx.Config{Format: logx.FormatText, ContextAttrs: []logx.ContextAttrs{httpserver.LogAttrs, otelx.LogAttrs}})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	app, err := build(ctx, addr, logger)
	if err != nil {
		logger.Error("boot failed", "err", err)
		return 1
	}
	return lifecycle.ExitCode(app.Run(ctx))
}

func build(ctx context.Context, addr string, logger *slog.Logger) (*lifecycle.App, error) {
	exporter, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		return nil, err
	}
	tel, err := otelx.New(ctx, otelx.Config{ServiceName: "serverhttp-example", Traces: otelx.TracesConfig{Exporter: exporter}, Logger: logger})
	if err != nil {
		return nil, err
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/items/{id}", func(w http.ResponseWriter, r *http.Request) {
		logger.InfoContext(r.Context(), "loading item", "id", r.PathValue("id")) // trace_id on the line through LogAttrs
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"id": r.PathValue("id")})
	})
	mux.HandleFunc("GET /api/panic", func(http.ResponseWriter, *http.Request) { panic("deliberate") })

	// Tracing sits outside Recover (the panic is seen as a 500 on the span) and inside RequestID and AccessLog.
	// Pair with metrics/http: httpserver.Metrics(metricshttp observer) goes outside Tracing.
	chain := []func(http.Handler) http.Handler{
		httpserver.RequestID(httpserver.RequestIDConfig{}),
		httpserver.AccessLog(httpserver.AccessLogConfig{Logger: logger}),
		serverhttp.Tracing(serverhttp.TracingConfig{
			Skip: func(r *http.Request) bool { return strings.HasPrefix(r.URL.Path, "/healthz") }, // probes and scrapes stay untraced
		}),
		httpserver.Recover(logger),
		httpserver.Timeout(10 * time.Second),
	}
	h := httpserver.Routed(mux) // the matched pattern reaches the span name
	for i := len(chain) - 1; i >= 0; i-- {
		h = chain[i](h)
	}
	srv, err := httpserver.New(httpserver.Config{Addr: addr, Handler: h, Logger: logger})
	if err != nil {
		return nil, err
	}
	if err := srv.Listen(ctx); err != nil {
		return nil, err
	}
	app := lifecycle.New(lifecycle.Config{Logger: logger})
	app.Add(lifecycle.Component{Name: "telemetry", Stop: tel.Shutdown}) // stops last: the final spans leave the process
	app.Add(lifecycle.Component{Name: "http", Run: srv.Run})
	return app, nil
}
