// Command grpc shows traced outbound gRPC: a client span per RPC with the
// trace context in the metadata, and dial options with keepalive so a dead
// connection is found before the next deadline. An in-process server with
// grpc.health.v1 is the dependency.
//
//	go run ./otelx/client/grpc
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/anuar-fest/tools/health"
	"github.com/anuar-fest/tools/otelx"
	clientgrpc "github.com/anuar-fest/tools/otelx/client/grpc"
	servergrpc "github.com/anuar-fest/tools/otelx/server/grpc"
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
	tel, err := otelx.New(ctx, otelx.Config{ServiceName: "clientgrpc-example", Traces: otelx.TracesConfig{Exporter: exporter}})
	if err != nil {
		return err
	}
	defer func() { _ = tel.Shutdown(context.WithoutCancel(ctx)) }() // flush even if ctx ended

	// The dependency: a plain server with the health service.
	hc := health.New(health.Config{})
	srv, err := servergrpc.NewServer(servergrpc.ServerConfig{Addr: "127.0.0.1:0", Health: hc})
	if err != nil {
		return err
	}
	if err = srv.Listen(ctx); err != nil {
		return err
	}
	go func() { _ = srv.Serve() }() //nolint:contextcheck // Serve ends through Shutdown below, not a context
	defer func() { _ = srv.Shutdown(ctx) }()

	// The client: interceptors plus keepalive from DialOptions; credentials are always yours to add.
	// Pair with metrics/grpc: append(ic.DialOptions(), mc.DialOptions()...) chains the metrics interceptors inside.
	ic, err := clientgrpc.New(clientgrpc.Config{PeerService: "orders", KeepaliveTime: 30 * time.Second})
	if err != nil {
		return err
	}
	cc, err := grpc.NewClient(srv.Addr().String(), append(ic.DialOptions(), grpc.WithTransportCredentials(insecure.NewCredentials()))...)
	if err != nil {
		return err
	}
	defer func() { _ = cc.Close() }()

	ctx, root := tel.Tracer("example").Start(ctx, "check orders")
	defer root.End()
	resp, err := grpc_health_v1.NewHealthClient(cc).Check(ctx, &grpc_health_v1.HealthCheckRequest{}) // span grpc.health.v1.Health/Check, child of "check orders"
	if err != nil {
		return err
	}
	fmt.Println("orders is", resp.GetStatus())
	return nil
}
