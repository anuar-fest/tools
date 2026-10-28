// Package metricsgrpc measures gRPC on the OpenTelemetry API: server
// interceptors that export rpc.server.* and client interceptors that export
// rpc.client.call.duration, with the RPC semantic conventions. Spans,
// propagation, panic recovery and keepalive are otelx/server/grpc and
// otelx/client/grpc; chain those first and these second so a duration
// sample is recorded with the span in its context.
package metricsgrpc

import (
	"context"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/anuar-fest/tools/metrics"
)

// InstrumentationName is the meter name.
const InstrumentationName = "github.com/anuar-fest/tools/metrics/grpc"

// Attribute keys (RPC semantic conventions).
const (
	AttrRPCSystem     = attribute.Key("rpc.system")
	AttrRPCService    = attribute.Key("rpc.service")
	AttrRPCMethod     = attribute.Key("rpc.method")
	AttrGRPCStatus    = attribute.Key("rpc.grpc.status_code")
	AttrServerAddress = attribute.Key("server.address")
	AttrServerPort    = attribute.Key("server.port")
	AttrPeerService   = attribute.Key("peer.service")
	AttrErrorType     = attribute.Key("error.type")
)

// ServerConfig configures NewServer.
type ServerConfig struct {
	// Meter defaults to the global meter named InstrumentationName.
	Meter metric.Meter
	// Skip leaves a method unmeasured; default skips the health service.
	Skip func(fullMethod string) bool
}

// Server measures inbound RPCs:
//
//	rpc.server.call.duration   histogram (s)   rpc.system, rpc.service, rpc.method, rpc.grpc.status_code, error.type
//	rpc.server.request.size    histogram (By)  same
//	rpc.server.response.size   histogram (By)  same
//	rpc.server.active_requests up/down counter
type Server struct {
	cfg      ServerConfig
	duration metric.Float64Histogram
	reqSize  metric.Int64Histogram
	respSize metric.Int64Histogram
	active   metric.Int64UpDownCounter
}

// NewServer creates the instruments.
func NewServer(cfg ServerConfig) (*Server, error) {
	if cfg.Meter == nil {
		cfg.Meter = otel.GetMeterProvider().Meter(InstrumentationName)
	}
	if cfg.Skip == nil {
		cfg.Skip = func(m string) bool { return strings.HasPrefix(m, "/grpc.health.v1.Health/") }
	}
	s := &Server{cfg: cfg}
	var err error
	s.duration, err = cfg.Meter.Float64Histogram("rpc.server.call.duration",
		metric.WithDescription("Duration of inbound RPCs."), metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(metrics.LatencyBuckets...))
	if err != nil {
		return nil, err
	}
	s.reqSize, err = cfg.Meter.Int64Histogram("rpc.server.request.size", metric.WithDescription("Request message size."), metric.WithUnit("By"))
	if err != nil {
		return nil, err
	}
	s.respSize, err = cfg.Meter.Int64Histogram("rpc.server.response.size", metric.WithDescription("Response message size."), metric.WithUnit("By"))
	if err != nil {
		return nil, err
	}
	s.active, err = cfg.Meter.Int64UpDownCounter("rpc.server.active_requests", metric.WithDescription("Inbound RPCs in flight."), metric.WithUnit("{request}"))
	if err != nil {
		return nil, err
	}
	return s, nil
}

// Unary returns the unary interceptor. Chain it after the tracing
// interceptor: that one recovers panics and answers Internal; this one
// records the sample as Internal and re-panics, so the outer recover still
// sees the original stack.
func (s *Server) Unary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		if s.cfg.Skip(info.FullMethod) {
			return handler(ctx, req)
		}
		attrs := serverAttrs(info.FullMethod)
		start := time.Now()
		s.active.Add(ctx, 1)
		defer func() {
			s.active.Add(ctx, -1)
			if p := recover(); p != nil {
				s.finish(ctx, attrs, start, status.Error(codes.Internal, "panic"))
				s.recordSize(ctx, s.reqSize, req, attrs)
				panic(p)
			}
			s.finish(ctx, attrs, start, err)
			s.recordSize(ctx, s.reqSize, req, attrs)
			s.recordSize(ctx, s.respSize, resp, attrs)
		}()
		return handler(ctx, req)
	}
}

// Stream returns the stream interceptor; one sample per stream.
func (s *Server) Stream() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		if s.cfg.Skip(info.FullMethod) {
			return handler(srv, ss)
		}
		ctx := ss.Context()
		attrs := serverAttrs(info.FullMethod)
		start := time.Now()
		s.active.Add(ctx, 1)
		defer func() {
			s.active.Add(ctx, -1)
			if p := recover(); p != nil {
				s.finish(ctx, attrs, start, status.Error(codes.Internal, "panic"))
				panic(p)
			}
			s.finish(ctx, attrs, start, err)
		}()
		return handler(srv, ss)
	}
}

// ServerOptions returns the two chain options for grpc.NewServer, or for
// servergrpc.ServerConfig.Options so they chain after the tracing ones.
func (s *Server) ServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{grpc.ChainUnaryInterceptor(s.Unary()), grpc.ChainStreamInterceptor(s.Stream())}
}

func (s *Server) finish(ctx context.Context, attrs attribute.Set, start time.Time, err error) {
	s.duration.Record(ctx, time.Since(start).Seconds(), metric.WithAttributeSet(withStatus(attrs, err)))
}

func (s *Server) recordSize(ctx context.Context, h metric.Int64Histogram, msg any, attrs attribute.Set) {
	if m, ok := msg.(proto.Message); ok && m != nil {
		h.Record(ctx, int64(proto.Size(m)), metric.WithAttributeSet(attrs))
	}
}

func serverAttrs(fullMethod string) attribute.Set {
	service, method := splitMethod(fullMethod)
	return attribute.NewSet(AttrRPCSystem.String("grpc"), AttrRPCService.String(service), AttrRPCMethod.String(method))
}

// withStatus adds the status code and, for a failed call, error.type.
func withStatus(attrs attribute.Set, err error) attribute.Set {
	code := status.Code(err)
	kvs := attrs.ToSlice()
	kvs = append(kvs, AttrGRPCStatus.Int(int(code)))
	if err != nil {
		kvs = append(kvs, AttrErrorType.String(code.String()))
	}
	return attribute.NewSet(kvs...)
}

func splitMethod(full string) (service, method string) {
	full = strings.TrimPrefix(full, "/")
	if i := strings.LastIndexByte(full, '/'); i >= 0 {
		return full[:i], full[i+1:]
	}
	return full, ""
}
