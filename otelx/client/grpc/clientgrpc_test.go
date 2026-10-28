package clientgrpc_test

import (
	"context"
	"errors"
	"io"
	"net"
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
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	clientgrpc "github.com/anuar-fest/tools/otelx/client/grpc"
)

func attrValue(attrs []attribute.KeyValue, key string) string {
	for _, a := range attrs {
		if string(a.Key) == key {
			return a.Value.String()
		}
	}
	return ""
}

func TestUnary_DirectInvocationInjectsAndRecordsSpan(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tracer := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter)).Tracer("t")
	ic, err := clientgrpc.New(clientgrpc.Config{Tracer: tracer, Propagator: propagation.TraceContext{}, PeerService: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, root := tracer.Start(context.Background(), "root")
	var outgoing metadata.MD
	err = ic.Unary()(ctx, "/shop.Orders/Place", nil, nil, nil, func(ctx context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		outgoing, _ = metadata.FromOutgoingContext(ctx)
		return status.Error(codes.Unavailable, "down")
	})
	root.End()
	if status.Code(err) != codes.Unavailable {
		t.Fatal(err)
	}
	if tp := outgoing.Get("traceparent"); len(tp) != 1 || tp[0] == "" {
		t.Fatalf("traceparent must be injected: %v", outgoing)
	}
	var cs tracetest.SpanStub
	for _, s := range exporter.GetSpans() {
		if s.SpanKind == trace.SpanKindClient {
			cs = s
		}
	}
	if cs.Name != "shop.Orders/Place" || cs.Parent.SpanID() != root.SpanContext().SpanID() || cs.Status.Code.String() != "Error" {
		t.Fatalf("client span: %+v", cs)
	}
	if attrValue(cs.Attributes, "peer.service") != "orders" || attrValue(cs.Attributes, "rpc.grpc.status_code") != "14" || attrValue(cs.Attributes, "error.type") != "Unavailable" {
		t.Fatalf("attributes: %v", cs.Attributes)
	}
}

func TestStream_OverTheWireEndsSpanOnEOF(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tracer := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter)).Tracer("t")
	ic, err := clientgrpc.New(clientgrpc.Config{Tracer: tracer, Propagator: propagation.TraceContext{}})
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var seenTraceparent string
	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		seenTraceparent = md.Get("traceparent")[0]
		return h(ctx, req)
	}))
	hs := health.NewServer()
	grpc_health_v1.RegisterHealthServer(srv, hs)
	go func() { _ = srv.Serve(ln) }()
	defer srv.Stop()

	cc, err := grpc.NewClient(ln.Addr().String(), append(ic.DialOptions(), grpc.WithTransportCredentials(insecure.NewCredentials()))...)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cc.Close() }()
	client := grpc_health_v1.NewHealthClient(cc)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.Check(ctx, &grpc_health_v1.HealthCheckRequest{}); err != nil {
		t.Fatal(err)
	}
	if seenTraceparent == "" {
		t.Fatal("server must receive the injected trace context")
	}
	wctx, wcancel := context.WithCancel(ctx)
	watch, err := client.Watch(wctx, &grpc_health_v1.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := watch.Recv(); err != nil {
		t.Fatal(err)
	}
	wcancel()
	if _, err := watch.Recv(); err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("canceled stream must error: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	var watchSpan tracetest.SpanStub
	for time.Now().Before(deadline) {
		for _, s := range exporter.GetSpans() {
			if s.Name == "grpc.health.v1.Health/Watch" {
				watchSpan = s
			}
		}
		if watchSpan.Name != "" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	types := map[string]bool{}
	for _, e := range watchSpan.Events {
		types[attrValue(e.Attributes, "rpc.message.type")] = true
	}
	if watchSpan.Name == "" || !types["SENT"] || !types["RECEIVED"] {
		t.Fatalf("stream span must end once with sent and received message events: %+v", watchSpan)
	}
	if attrValue(watchSpan.Attributes, "server.address") != "127.0.0.1" {
		t.Fatalf("server.address from the target: %v", watchSpan.Attributes)
	}
}
