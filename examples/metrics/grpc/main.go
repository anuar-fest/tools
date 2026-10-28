// Command grpc shows gRPC metrics on both sides: server interceptors that
// export rpc.server.call.duration, message sizes and in-flight calls, and
// client interceptors that export rpc.client.call.duration per peer
// service. The in-process server's health service is the dependency.
//
//	go run ./metrics/grpc
//	curl -s localhost:9090/metrics | grep -E 'rpc_(server|client)_'
package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/anuar-fest/tools/health"
	"github.com/anuar-fest/tools/httpserver"
	"github.com/anuar-fest/tools/lifecycle"
	"github.com/anuar-fest/tools/metrics"
	metricsgrpc "github.com/anuar-fest/tools/metrics/grpc"
	servergrpc "github.com/anuar-fest/tools/otelx/server/grpc"
)

func main() {
	adminAddr := flag.String("admin-addr", ":9090", "admin listen address for /metrics")
	flag.Parse()
	os.Exit(run(context.Background(), *adminAddr))
}

func run(ctx context.Context, adminAddr string) int {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	app, err := build(ctx, adminAddr, logger)
	if err != nil {
		logger.Error("boot failed", "err", err)
		return 1
	}
	return lifecycle.ExitCode(app.Run(ctx))
}

func build(ctx context.Context, adminAddr string, logger *slog.Logger) (*lifecycle.App, error) {
	m, err := metrics.New(ctx, metrics.Config{ServiceName: "metricsgrpc-example", Logger: logger})
	if err != nil {
		return nil, err
	}
	// Server side: the default Skip leaves the health service unmeasured; measure it here because it is all this example serves.
	ms, err := metricsgrpc.NewServer(metricsgrpc.ServerConfig{Skip: func(string) bool { return false }})
	if err != nil {
		return nil, err
	}
	hc := health.New(health.Config{})
	// With tracing too: Interceptors: tracingInterceptors (outer) and Options: ms.ServerOptions() (inner).
	srv, err := servergrpc.NewServer(servergrpc.ServerConfig{Addr: "127.0.0.1:0", Health: hc, Options: ms.ServerOptions(), Logger: logger})
	if err != nil {
		return nil, err
	}
	if err = srv.Listen(ctx); err != nil {
		return nil, err
	}
	// Client side: interceptors only; keepalive and credentials come from otelx/client/grpc or your own dial options.
	mc, err := metricsgrpc.NewClient(metricsgrpc.ClientConfig{PeerService: "orders"})
	if err != nil {
		return nil, err
	}
	cc, err := grpc.NewClient(srv.Addr().String(), append(mc.DialOptions(), grpc.WithTransportCredentials(insecure.NewCredentials()))...)
	if err != nil {
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
	app.Add(lifecycle.Component{Name: "grpc", Run: srv.Run})
	app.Add(lifecycle.Component{Name: "caller", Run: func(ctx context.Context) error {
		defer func() { _ = cc.Close() }()
		health := grpc_health_v1.NewHealthClient(cc)
		t := time.NewTicker(500 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-t.C:
				if _, err := health.Check(ctx, &grpc_health_v1.HealthCheckRequest{}); err != nil && ctx.Err() == nil {
					logger.Warn("check failed", "err", err)
				}
			}
		}
	}})
	return app, nil
}
