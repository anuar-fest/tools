// Command otelx shows the one tracing SDK install: the resource, an
// OTLP/HTTP exporter (or stdout here), parent-based sampling, the W3C
// propagators, and LogAttrs so every log line inside a span carries its
// trace id. Shutdown flushes the last spans.
//
//	go run ./otelx                                    # spans pretty-printed to stdout
//	go run ./otelx -otlp-endpoint localhost:4318      # spans to a collector
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/anuar-fest/tools/logx"
	"github.com/anuar-fest/tools/otelx"
)

func main() {
	endpoint := flag.String("otlp-endpoint", "", "OTLP/HTTP collector host:port; empty prints spans to stdout")
	flag.Parse()
	if err := run(context.Background(), *endpoint); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, endpoint string) error {
	logger, err := logx.New(logx.Config{Format: logx.FormatText, ContextAttrs: []logx.ContextAttrs{otelx.LogAttrs}})
	if err != nil {
		return err
	}
	var exporter sdktrace.SpanExporter
	if endpoint == "" {
		if exporter, err = stdouttrace.New(stdouttrace.WithPrettyPrint()); err != nil {
			return err
		}
	}
	tel, err := otelx.New(ctx, otelx.Config{
		ServiceName: "otelx-example", ServiceVersion: "dev", Environment: "local",
		Traces: otelx.TracesConfig{Exporter: exporter, Endpoint: endpoint, Insecure: true, SampleRatio: 1},
		Logger: logger,
	})
	if err != nil {
		return err
	}
	defer func() { // last on exit, so the final spans leave the process; lifecycle does this with Stop: tel.Shutdown
		flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if err := tel.Shutdown(flushCtx); err != nil {
			logger.Error("telemetry shutdown", "err", err)
		}
	}()

	tracer := tel.Tracer("example")
	ctx, root := tracer.Start(ctx, "place order", trace.WithAttributes(attribute.String("tenant.id", "acme")))
	logger.InfoContext(ctx, "handling request") // trace_id and span_id come from LogAttrs
	_, child := tracer.Start(ctx, "charge card")
	time.Sleep(20 * time.Millisecond)
	child.End()
	root.End()
	logger.Info("outside any span: no trace id", "traced", tel.TracingEnabled())
	return nil
}
