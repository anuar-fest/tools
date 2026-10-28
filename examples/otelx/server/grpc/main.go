// Command grpc shows the gRPC server you own: tracing interceptors that
// continue the caller's trace and turn a panic into codes.Internal, a
// Server with keepalive, size and shutdown defaults that fits lifecycle,
// and grpc.health.v1 answering from the same registry as /healthz/ready.
//
//	go run ./otelx/server/grpc
//	grpcurl -plaintext localhost:9000 grpc.health.v1.Health/Check     # SERVING; the server span is printed on stdout
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"

	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"

	"github.com/anuar-fest/tools/health"
	"github.com/anuar-fest/tools/lifecycle"
	"github.com/anuar-fest/tools/otelx"
	servergrpc "github.com/anuar-fest/tools/otelx/server/grpc"
)

func main() {
	addr := flag.String("grpc-addr", ":9000", "gRPC listen address")
	flag.Parse()
	os.Exit(run(context.Background(), *addr))
}

func run(ctx context.Context, addr string) int {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
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
	tel, err := otelx.New(ctx, otelx.Config{ServiceName: "servergrpc-example", Traces: otelx.TracesConfig{Exporter: exporter}, Logger: logger})
	if err != nil {
		return nil, err
	}
	hc := health.New(health.Config{Logger: logger})
	hc.AddReadiness("always", func(context.Context) error { return nil })

	ic, err := servergrpc.New(servergrpc.Config{
		Logger: logger,
		Skip:   func(string) bool { return false }, // the default skips the health service; this example has nothing else to trace
	})
	if err != nil {
		return nil, err
	}
	// Pair with metrics/grpc: Options: ms.ServerOptions() chains the metrics interceptors inside the tracing ones.
	srv, err := servergrpc.NewServer(servergrpc.ServerConfig{Addr: addr, Interceptors: ic, Health: hc, Logger: logger})
	if err != nil {
		return nil, err
	}
	// Register your services on srv.GRPC() here, before Listen.
	if err := srv.Listen(ctx); err != nil {
		return nil, err
	}
	app := lifecycle.New(lifecycle.Config{Logger: logger, OnShutdown: func(string) { hc.SetDraining(true) }}) // Check answers NOT_SERVING while draining
	app.Add(lifecycle.Component{Name: "telemetry", Stop: tel.Shutdown})
	app.Add(lifecycle.Component{Name: "grpc", Run: srv.Run}) // GracefulStop under the budget, then Stop
	return app, nil
}
