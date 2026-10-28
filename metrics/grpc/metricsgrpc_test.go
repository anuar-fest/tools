package metricsgrpc_test

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	metricsgrpc "github.com/anuar-fest/tools/metrics/grpc"
)

type rig struct {
	reader *sdkmetric.ManualReader
	mp     *sdkmetric.MeterProvider
}

func newRig() *rig {
	r := &rig{reader: sdkmetric.NewManualReader()}
	r.mp = sdkmetric.NewMeterProvider(sdkmetric.WithReader(r.reader))
	return r
}

func (r *rig) metrics(t *testing.T) map[string]metricdata.Metrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := r.reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	out := map[string]metricdata.Metrics{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			out[m.Name] = m
		}
	}
	return out
}

func attr(set attribute.Set, key string) string {
	v, ok := set.Value(attribute.Key(key))
	if !ok {
		return ""
	}
	return v.String()
}

// byStatus indexes histogram series by rpc.grpc.status_code.
func byStatus(h metricdata.Histogram[float64]) map[string]metricdata.HistogramDataPoint[float64] {
	out := map[string]metricdata.HistogramDataPoint[float64]{}
	for _, dp := range h.DataPoints {
		out[attr(dp.Attributes, "rpc.grpc.status_code")] = dp
	}
	return out
}

func TestServer_UnaryStatusPanicAndSizes(t *testing.T) {
	r := newRig()
	s, err := metricsgrpc.NewServer(metricsgrpc.ServerConfig{Meter: r.mp.Meter("m"), Skip: func(string) bool { return false }})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	info := &grpc.UnaryServerInfo{FullMethod: "/shop.Orders/Place"}
	req := &grpc_health_v1.HealthCheckRequest{Service: "ready"}
	_, err = s.Unary()(ctx, req, info, func(context.Context, any) (any, error) { return nil, status.Error(codes.InvalidArgument, "bad") })
	if status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
	_, _ = s.Unary()(ctx, req, info, func(context.Context, any) (any, error) { return &grpc_health_v1.HealthCheckResponse{}, nil })

	// A panic is recorded as Internal and re-raised for the outer (tracing) recover.
	func() {
		defer func() {
			if p := recover(); p != "boom" {
				t.Fatalf("the panic must reach the caller unchanged: %v", p)
			}
		}()
		_, _ = s.Unary()(ctx, req, info, func(context.Context, any) (any, error) { panic("boom") })
	}()

	ms := r.metrics(t)
	series := byStatus(ms["rpc.server.call.duration"].Data.(metricdata.Histogram[float64]))
	if len(series) != 3 {
		t.Fatalf("one series per status: %v", series)
	}
	if dp := series["3"]; dp.Count != 1 || attr(dp.Attributes, "error.type") != "InvalidArgument" || attr(dp.Attributes, "rpc.service") != "shop.Orders" || attr(dp.Attributes, "rpc.method") != "Place" {
		t.Fatalf("InvalidArgument series: %v", dp.Attributes)
	}
	if dp := series["0"]; dp.Count != 1 || attr(dp.Attributes, "error.type") != "" {
		t.Fatalf("OK series carries no error.type: %v", dp.Attributes)
	}
	if dp := series["13"]; dp.Count != 1 || attr(dp.Attributes, "error.type") != "Internal" {
		t.Fatalf("panic series: %v", dp.Attributes)
	}
	if active := ms["rpc.server.active_requests"].Data.(metricdata.Sum[int64]).DataPoints[0].Value; active != 0 {
		t.Fatalf("active = %d", active)
	}
	var reqSamples, respSamples uint64
	for _, dp := range ms["rpc.server.request.size"].Data.(metricdata.Histogram[int64]).DataPoints {
		reqSamples += dp.Count
	}
	for _, dp := range ms["rpc.server.response.size"].Data.(metricdata.Histogram[int64]).DataPoints {
		respSamples += dp.Count
	}
	if reqSamples != 3 || respSamples != 1 {
		t.Fatalf("request sizes for every call (%d), response size only when there was one (%d)", reqSamples, respSamples)
	}
}

func TestServer_SkipsHealthByDefault(t *testing.T) {
	r := newRig()
	s, err := metricsgrpc.NewServer(metricsgrpc.ServerConfig{Meter: r.mp.Meter("m")})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = s.Unary()(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/grpc.health.v1.Health/Check"}, func(context.Context, any) (any, error) { return nil, nil })
	if _, ok := r.metrics(t)["rpc.server.call.duration"]; ok {
		t.Fatal("the health service is skipped by default")
	}
}

func TestServerAndClient_OverTheWireStreams(t *testing.T) {
	r := newRig()
	s, err := metricsgrpc.NewServer(metricsgrpc.ServerConfig{Meter: r.mp.Meter("m"), Skip: func(string) bool { return false }})
	if err != nil {
		t.Fatal(err)
	}
	c, err := metricsgrpc.NewClient(metricsgrpc.ClientConfig{Meter: r.mp.Meter("m"), PeerService: "health"})
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(s.ServerOptions()...)
	grpc_health_v1.RegisterHealthServer(srv, health.NewServer())
	go func() { _ = srv.Serve(ln) }()
	defer srv.Stop()

	cc, err := grpc.NewClient(ln.Addr().String(), append(c.DialOptions(), grpc.WithTransportCredentials(insecure.NewCredentials()))...)
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
	if _, err := client.Check(ctx, &grpc_health_v1.HealthCheckRequest{Service: "nope"}); status.Code(err) != codes.NotFound {
		t.Fatalf("unknown service: %v", err)
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

	// The server records its stream sample after the client saw the cancellation; wait for it.
	var serverSeries map[string]metricdata.HistogramDataPoint[float64]
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		serverSeries = byStatus(r.metrics(t)["rpc.server.call.duration"].Data.(metricdata.Histogram[float64]))
		if _, ok := serverSeries["1"]; ok {
			break
		}
	}
	for code, want := range map[string]uint64{"0": 1, "5": 1, "1": 1} {
		if serverSeries[code].Count != want {
			t.Fatalf("server series %s: %+v", code, serverSeries)
		}
	}
	if attr(serverSeries["1"].Attributes, "rpc.method") != "Watch" {
		t.Fatalf("stream sample names the method: %v", serverSeries["1"].Attributes)
	}
	clientSeries := byStatus(r.metrics(t)["rpc.client.call.duration"].Data.(metricdata.Histogram[float64]))
	if dp, ok := clientSeries["1"]; !ok || dp.Count != 1 || attr(dp.Attributes, "peer.service") != "health" || attr(dp.Attributes, "server.address") != "127.0.0.1" || attr(dp.Attributes, "error.type") != "Canceled" {
		t.Fatalf("client stream series: %+v", clientSeries)
	}
	if dp := clientSeries["0"]; dp.Count != 1 || attr(dp.Attributes, "rpc.method") != "Check" {
		t.Fatalf("client unary series: %+v", clientSeries)
	}
}

func TestClient_UnaryDirectInvocation(t *testing.T) {
	r := newRig()
	c, err := metricsgrpc.NewClient(metricsgrpc.ClientConfig{Meter: r.mp.Meter("m"), PeerService: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	err = c.Unary()(context.Background(), "/shop.Orders/Place", nil, nil, nil, func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
		return status.Error(codes.Unavailable, "down")
	})
	if status.Code(err) != codes.Unavailable {
		t.Fatal(err)
	}
	dp := r.metrics(t)["rpc.client.call.duration"].Data.(metricdata.Histogram[float64]).DataPoints[0]
	if dp.Count != 1 || attr(dp.Attributes, "peer.service") != "orders" || attr(dp.Attributes, "rpc.grpc.status_code") != "14" || attr(dp.Attributes, "error.type") != "Unavailable" {
		t.Fatalf("attributes: %v", dp.Attributes)
	}
	if _, has := dp.Attributes.Value("server.address"); has {
		t.Fatal("no connection, no address")
	}
}
