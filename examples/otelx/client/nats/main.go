// Command nats shows tracing for raw nats.go: the trace context travels in
// the message header, every publish is a producer span and every handled
// message a consumer span joined to it, and a handler panic is recovered
// instead of killing the subscriber. It needs a NATS server.
//
//	go run ./otelx/client/nats -nats-url nats://localhost:4222
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"

	"github.com/anuar-fest/tools/otelx"
	clientnats "github.com/anuar-fest/tools/otelx/client/nats"
)

func main() {
	url := flag.String("nats-url", "", "NATS URL (required)")
	flag.Parse()
	if *url == "" {
		fmt.Fprintln(os.Stderr, "-nats-url is required")
		os.Exit(2)
	}
	if err := run(context.Background(), *url); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, url string) error {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	exporter, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		return err
	}
	tel, err := otelx.New(ctx, otelx.Config{ServiceName: "clientnats-example", Traces: otelx.TracesConfig{Exporter: exporter}, Logger: logger})
	if err != nil {
		return err
	}
	defer func() { _ = tel.Shutdown(context.WithoutCancel(ctx)) }() // flush even if ctx ended

	nc, err := nats.Connect(url, nats.Name("clientnats-example"))
	if err != nil {
		return err
	}
	defer nc.Close()
	n, err := clientnats.New(clientnats.Config{Consumer: "greeter", Logger: logger})
	if err != nil {
		return err
	}

	done := make(chan struct{})
	// Pair with metrics/nats: n.MsgHandler(mt.Handler(handle)) nests the metrics wrapper inside the span.
	sub, err := nc.QueueSubscribe("example.greet", "greeter", n.MsgHandler(func(ctx context.Context, msg *nats.Msg) error { //nolint:contextcheck // the handler context comes from the message header, not the caller
		logger.InfoContext(ctx, "handled", "subject", msg.Subject, "data", string(msg.Data)) // inside the consumer span
		close(done)
		return nil
	}))
	if err != nil {
		return err
	}
	defer func() { _ = sub.Unsubscribe() }()

	ctx, root := tel.Tracer("example").Start(ctx, "greet")
	if err := n.PublishMsg(ctx, nc, &nats.Msg{Subject: "example.greet", Data: []byte("hello")}); err != nil { // producer span; traceparent in the header
		root.End()
		return err
	}
	root.End()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		return fmt.Errorf("no message within 5s")
	}
	return nil
}
