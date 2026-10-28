// Command http shows the traced outbound transport: one client span per
// call named after the peer service, the trace context injected into the
// request, and the resilience transport inside it so the span covers every
// retry. An in-process upstream echoes the traceparent it received.
//
//	go run ./otelx/client/http
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"

	"github.com/anuar-fest/tools/httpserver"
	"github.com/anuar-fest/tools/otelx"
	clienthttp "github.com/anuar-fest/tools/otelx/client/http"
	"github.com/anuar-fest/tools/resilience/httpx"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	exporter, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		return err
	}
	tel, err := otelx.New(ctx, otelx.Config{ServiceName: "clienthttp-example", Traces: otelx.TracesConfig{Exporter: exporter}})
	if err != nil {
		return err
	}
	defer func() { _ = tel.Shutdown(context.WithoutCancel(ctx)) }() // flush even if ctx ended

	upstream, err := httpserver.New(httpserver.Config{Addr: "127.0.0.1:0", Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = fmt.Fprintf(w, "upstream saw traceparent=%q\n", r.Header.Get("traceparent")) //nolint:gosec // echoed quoted, text/plain, loopback demo
	})})
	if err != nil {
		return err
	}
	if err = upstream.Listen(ctx); err != nil {
		return err
	}
	go func() { _ = upstream.Serve() }() //nolint:contextcheck // Serve ends through Shutdown below, not a context
	defer func() { _ = upstream.Shutdown(ctx) }()

	// httpx owns timeouts and retries; clienthttp wraps it so one span covers all attempts and the headers are injected before the first.
	// Pair with metrics/http: clienthttp.Transport(metricshttp.Transport(tr, ...), ...) puts the sample inside the span.
	tr, err := httpx.NewTransport(httpx.TransportConfig{Name: "fraud", CallTimeout: 5 * time.Second, AttemptTimeout: 2 * time.Second})
	if err != nil {
		return err
	}
	client := &http.Client{Transport: clienthttp.Transport(tr, clienthttp.Config{PeerService: "fraud"})}

	ctx, root := tel.Tracer("example").Start(ctx, "score customer") // the client span becomes a child of this one
	defer root.End()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+upstream.Addr().String()+"/score?token=secret", nil) // url.full on the span has no query
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	fmt.Print(string(body))
	return nil
}
