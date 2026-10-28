// Command resilience shows the two ways to protect an outbound dependency:
// a reviewed stack preset around any operation, and the HTTP transport that
// composes bulkhead, breaker, retry and timeouts with status-aware
// classification. A flaky in-process upstream makes the policies visible.
//
//	go run ./resilience -fail-ratio 0.6 -calls 40
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"time"

	"github.com/anuar-fest/tools/httpserver"
	"github.com/anuar-fest/tools/resilience/breaker"
	"github.com/anuar-fest/tools/resilience/bulkhead"
	"github.com/anuar-fest/tools/resilience/httpx"
	"github.com/anuar-fest/tools/resilience/stack"
)

func main() {
	failRatio := flag.Float64("fail-ratio", 0.5, "fraction of upstream calls that answer 503")
	calls := flag.Int("calls", 20, "calls to make")
	flag.Parse()
	if err := run(context.Background(), *failRatio, *calls); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, failRatio float64, calls int) error {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// A flaky upstream on a loopback port.
	upstream, err := httpserver.New(httpserver.Config{Addr: "127.0.0.1:0", Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if rand.Float64() < failRatio {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, "ok")
	})})
	if err != nil {
		return err
	}
	if err = upstream.Listen(ctx); err != nil {
		return err
	}
	go func() { _ = upstream.Serve() }() //nolint:contextcheck // Serve ends through Shutdown below, not a context
	defer func() { _ = upstream.Shutdown(ctx) }()
	url := "http://" + upstream.Addr().String() + "/score"

	// 1. The HTTP transport: one bulkhead, one breaker, one transport per dependency so failure domains stay separate.
	bh, err := bulkhead.New(bulkhead.Config{Name: "scoring", MaxConcurrent: 8})
	if err != nil {
		return err
	}
	br, err := breaker.New(breaker.Config{Name: "scoring", Trip: breaker.ConsecutiveFailures(3), OpenFor: 2 * time.Second})
	if err != nil {
		return err
	}
	tr, err := httpx.NewTransport(httpx.TransportConfig{
		Name: "scoring", CallTimeout: 2 * time.Second, AttemptTimeout: 500 * time.Millisecond, Bulkhead: bh, Breaker: br,
	})
	if err != nil {
		return err
	}
	client := &http.Client{Transport: tr}
	for i := 0; i < calls; i++ {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		resp, callErr := client.Do(req)
		switch {
		case callErr != nil: // ErrBreakerOpen, ErrBulkheadFull, a timeout, or a StatusError after the retries gave up
			var se *httpx.StatusError
			logger.Warn("call failed", "n", i, "err", callErr, "status_error", errors.As(callErr, &se))
		default:
			_, _ = io.Copy(io.Discard, resp.Body) // drain so the connection is reused
			_ = resp.Body.Close()
			logger.Info("call ok", "n", i, "status", resp.StatusCode)
		}
	}

	// 2. The stack preset for any operation: deadline → bulkhead → limit → retry(breaker(attempt-timeout(op))).
	cfg, err := stack.Preset("scoring-model", stack.ReadFast, stack.PresetOptions{MaxConcurrent: 4})
	if err != nil {
		return err
	}
	s, err := stack.New(cfg)
	if err != nil {
		return err
	}
	score, err := stack.DoValue(ctx, s, func(context.Context) (int, error) {
		if rand.Float64() < failRatio {
			return 0, errors.New("model busy")
		}
		return 87, nil
	})
	logger.Info("stack result", "score", score, "err", err, "outcome", stack.Outcome(err))
	return nil
}
