package servergrpc_test

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/anuar-fest/tools/health"
	servergrpc "github.com/anuar-fest/tools/otelx/server/grpc"
)

type rig struct {
	exporter *tracetest.InMemoryExporter
	ic       *servergrpc.Interceptors
}

func newRig(t *testing.T) *rig {
	t.Helper()
	r := &rig{exporter: tracetest.NewInMemoryExporter()}
	var err error
	r.ic, err = servergrpc.New(servergrpc.Config{
		Tracer:     sdktrace.NewTracerProvider(sdktrace.WithSyncer(r.exporter)).Tracer("t"),
		Propagator: propagation.TraceContext{},
		Skip:       func(string) bool { return false }, // trace the health service too, it is our only service here
	})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func attrValue(attrs []attribute.KeyValue, key string) string {
	for _, a := range attrs {
		if string(a.Key) == key {
			return a.Value.String()
		}
	}
	return ""
}

func TestUnary_DirectInvocationStatusAndPanic(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	info := &grpc.UnaryServerInfo{FullMethod: "/shop.Orders/Place"}
	_, err := r.ic.Unary()(ctx, nil, info, func(context.Context, any) (any, error) { return nil, status.Error(codes.InvalidArgument, "bad") })
	if status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
	_, err = r.ic.Unary()(ctx, nil, info, func(context.Context, any) (any, error) { panic("boom") })
	if status.Code(err) != codes.Internal {
		t.Fatalf("panic must become Internal: %v", err)
	}
	_, _ = r.ic.Unary()(ctx, nil, info, func(context.Context, any) (any, error) { return "ok", nil })

	spans := r.exporter.GetSpans()
	if len(spans) != 3 || spans[0].Name != "shop.Orders/Place" || spans[0].SpanKind != trace.SpanKindServer {
		t.Fatalf("spans: %+v", spans)
	}
	if attrValue(spans[0].Attributes, "rpc.grpc.status_code") != "3" || spans[0].Status.Code.String() != "Unset" {
		t.Fatalf("InvalidArgument is the client's fault, not a server error: %+v", spans[0])
	}
	if attrValue(spans[1].Attributes, "error.type") != "Internal" || spans[1].Status.Code.String() != "Error" {
		t.Fatalf("panic span: %+v", spans[1])
	}
	if attrValue(spans[2].Attributes, "rpc.service") != "shop.Orders" || attrValue(spans[2].Attributes, "rpc.method") != "Place" || attrValue(spans[2].Attributes, "rpc.grpc.status_code") != "0" {
		t.Fatalf("service/method/status: %+v", spans[2].Attributes)
	}
}

func TestServer_HealthOverTheWireContinuesTrace(t *testing.T) {
	r := newRig(t)
	hc := health.New(health.Config{CacheTTL: -1})
	up := true
	hc.AddReadiness("dep", health.Bool(func() bool { return up }, "down"))
	srv, err := servergrpc.NewServer(servergrpc.ServerConfig{Addr: "127.0.0.1:0", Interceptors: r.ic, Health: hc, ShutdownTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	if err := srv.Listen(ctx); err != nil {
		t.Fatal(err)
	}
	go func() { runErr <- srv.Run(ctx) }()

	cc, err := grpc.NewClient(srv.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cc.Close() }()
	client := grpc_health_v1.NewHealthClient(cc)

	remote := trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{9}, SpanID: trace.SpanID{8}, TraceFlags: trace.FlagsSampled})
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(trace.ContextWithRemoteSpanContext(context.Background(), remote), carrier)
	md := metadata.New(carrier) // lower-cases keys, as gRPC requires
	callCtx, cancelCall := context.WithTimeout(metadata.NewOutgoingContext(context.Background(), md), 5*time.Second)
	defer cancelCall()
	resp, err := client.Check(callCtx, &grpc_health_v1.HealthCheckRequest{})
	if err != nil || resp.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
		t.Fatalf("check: %v %v", resp, err)
	}
	up = false
	resp, _ = client.Check(callCtx, &grpc_health_v1.HealthCheckRequest{Service: servergrpc.ServiceReady})
	if resp.GetStatus() != grpc_health_v1.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("readiness lost must be NOT_SERVING: %v", resp)
	}
	if _, err := client.Check(callCtx, &grpc_health_v1.HealthCheckRequest{Service: "nope"}); status.Code(err) != codes.NotFound {
		t.Fatalf("unknown service: %v", err)
	}
	watch, err := client.Watch(callCtx, &grpc_health_v1.HealthCheckRequest{Service: servergrpc.ServiceLive})
	if err != nil {
		t.Fatal(err)
	}
	first, err := watch.Recv()
	if err != nil || first.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
		t.Fatalf("watch first: %v %v", first, err)
	}
	cancelCall()
	if _, err := watch.Recv(); err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("canceled watch must end with a status error: %v", err)
	}

	var check tracetest.SpanStub
	for _, s := range r.exporter.GetSpans() {
		if s.Name == "grpc.health.v1.Health/Check" && s.Parent.SpanID() == remote.SpanID() {
			check = s
		}
	}
	if check.SpanContext.TraceID() != remote.TraceID() || attrValue(check.Attributes, "client.address") != "127.0.0.1" {
		t.Fatalf("server span must continue the caller's trace: %+v", check)
	}
	// The server ends the stream span after the client saw the cancellation; wait for it.
	var watchSpan tracetest.SpanStub
	for deadline := time.Now().Add(3 * time.Second); watchSpan.Name == "" && time.Now().Before(deadline); {
		for _, s := range r.exporter.GetSpans() {
			if s.Name == "grpc.health.v1.Health/Watch" {
				watchSpan = s
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	types := map[string]bool{}
	for _, e := range watchSpan.Events {
		types[attrValue(e.Attributes, "rpc.message.type")] = true
	}
	if !types["RECEIVED"] || !types["SENT"] {
		t.Fatalf("stream messages must be events in both directions: %+v", watchSpan)
	}

	cancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("clean shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run must return after cancel")
	}
	if err := srv.Listen(context.Background()); !errors.Is(err, servergrpc.ErrClosed) {
		t.Fatalf("after shutdown: %v", err)
	}
}

func TestNewServer_RequiresAddr(t *testing.T) {
	if _, err := servergrpc.NewServer(servergrpc.ServerConfig{}); err == nil {
		t.Fatal("Addr required")
	}
}
