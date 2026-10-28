// Command httpserver shows the inbound HTTP edge without a framework: a
// server with timeouts and a bounded shutdown, the middleware chain in its
// documented order, problem+json errors, the pagination guard and a
// constant-time API key.
//
//	go run ./httpserver -api-key local
//	curl -i -H 'X-API-Key: local' 'localhost:8080/api/items/7?limit=5'   # X-Request-Id, JSON
//	curl -i -H 'X-API-Key: local' 'localhost:8080/api/items/7?limit=9999' # 400 problem+json with max_limit
//	curl -i -H 'X-API-Key: local' localhost:8080/api/panic                # 500 problem+json, one ERROR access line
//	curl -i localhost:8080/api/items/7                                    # 401 without the key
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/anuar-fest/tools/httpserver"
	"github.com/anuar-fest/tools/lifecycle"
	"github.com/anuar-fest/tools/logx"
)

func main() {
	addr := flag.String("addr", ":8080", "API listen address")
	apiKey := flag.String("api-key", "", "API key for /api; empty leaves it open (local only)")
	flag.Parse()
	os.Exit(run(context.Background(), *addr, *apiKey))
}

func run(ctx context.Context, addr, apiKey string) int {
	logger, err := logx.New(logx.Config{Format: logx.FormatText, ContextAttrs: []logx.ContextAttrs{httpserver.LogAttrs}})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	handler, err := newAPI(logger, apiKey)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	srv, err := httpserver.New(httpserver.Config{Addr: addr, Handler: handler, WriteTimeout: 30 * time.Second, Logger: logger})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if err := srv.Listen(ctx); err != nil { // a busy port fails the boot here, not at the first probe
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	app := lifecycle.New(lifecycle.Config{Logger: logger})
	app.Add(lifecycle.Component{Name: "http", Run: srv.Run})
	return lifecycle.ExitCode(app.Run(ctx))
}

func newAPI(logger *slog.Logger, apiKey string) (http.Handler, error) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/items/{id}", func(w http.ResponseWriter, r *http.Request) {
		page, err := httpserver.ParsePage(r, httpserver.PageConfig{}) // limit and offset, capped
		if err != nil {
			httpserver.WriteError(w, r, err) // 400 problem+json with the cap in the body
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"id": r.PathValue("id"), "limit": page.Limit, "offset": page.Offset,
			"request_id": httpserver.RequestIDFrom(r.Context()), "consumer": httpserver.ConsumerFrom(r.Context()),
		})
	})
	mux.HandleFunc("GET /api/upstream", func(w http.ResponseWriter, r *http.Request) {
		httpserver.Fail(w, r, http.StatusBadGateway, "The upstream dependency did not answer.") // stable code, no internals
	})
	mux.HandleFunc("GET /api/panic", func(http.ResponseWriter, *http.Request) {
		panic("deliberate: Recover turns this into a 500 problem and one ERROR access line")
	})

	// Outermost first. RealIP before anything logs the client; RequestID before the access log; Recover inside the log so the panic is logged as a 500.
	chain := []func(http.Handler) http.Handler{
		httpserver.RealIP(httpserver.PrivateNetworks()),
		httpserver.RequestID(httpserver.RequestIDConfig{}),
		httpserver.AccessLog(httpserver.AccessLogConfig{Logger: logger}),
		httpserver.Recover(logger),
		httpserver.NoCache(),
		httpserver.MaxBytes(1 << 20),
		httpserver.Timeout(10 * time.Second),
	}
	if apiKey != "" {
		auth, err := httpserver.APIKey(httpserver.APIKeyConfig{Keys: map[string]string{"local": apiKey}, Logger: logger})
		if err != nil {
			return nil, err
		}
		chain = append(chain, auth)
	}
	return compose(httpserver.Routed(mux), chain...), nil // Routed: the matched pattern reaches the access log
}

// compose applies middleware outermost-first: compose(h, a, b) = a(b(h)).
func compose(h http.Handler, mws ...func(http.Handler) http.Handler) http.Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v) // the client is gone if this fails
}
