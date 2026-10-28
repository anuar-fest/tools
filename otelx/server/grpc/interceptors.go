package servergrpc

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"runtime/debug"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// InstrumentationName is the tracer name.
const InstrumentationName = "github.com/anuar-fest/tools/otelx/server/grpc"

// Attribute keys (RPC semantic conventions).
const (
	AttrRPCSystem     = attribute.Key("rpc.system")
	AttrRPCService    = attribute.Key("rpc.service")
	AttrRPCMethod     = attribute.Key("rpc.method")
	AttrGRPCStatus    = attribute.Key("rpc.grpc.status_code")
	AttrClientAddress = attribute.Key("client.address")
	AttrErrorType     = attribute.Key("error.type")
)

// Config configures New.
type Config struct {
	// Tracer defaults to the global tracer named InstrumentationName.
	Tracer trace.Tracer
	// Propagator defaults to the global propagator.
	Propagator propagation.TextMapPropagator
	// Logger receives recovered panics. nil discards.
	Logger *slog.Logger
	// Skip leaves a method uninstrumented; default skips the health service.
	Skip func(fullMethod string) bool
}

// Interceptors traces a server and contains handler panics.
type Interceptors struct {
	cfg Config
}

// New validates the configuration and fills the defaults.
func New(cfg Config) (*Interceptors, error) {
	if cfg.Tracer == nil {
		cfg.Tracer = otel.Tracer(InstrumentationName)
	}
	if cfg.Propagator == nil {
		cfg.Propagator = otel.GetTextMapPropagator()
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	if cfg.Skip == nil {
		cfg.Skip = func(m string) bool { return strings.HasPrefix(m, "/grpc.health.v1.Health/") }
	}
	return &Interceptors{cfg: cfg}, nil
}

// Unary returns the unary interceptor. Put it first in the chain so the
// span and the recovery cover everything after it (metrics/grpc goes
// second).
func (i *Interceptors) Unary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		if i.cfg.Skip(info.FullMethod) {
			return handler(ctx, req)
		}
		ctx, span := i.start(ctx, info.FullMethod)
		defer func() {
			if p := recover(); p != nil {
				err = i.recovered(ctx, info.FullMethod, p)
				resp = nil
			}
			i.finish(span, err)
		}()
		return handler(ctx, req)
	}
}

// Stream returns the stream interceptor. Each message is a span event.
func (i *Interceptors) Stream() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		if i.cfg.Skip(info.FullMethod) {
			return handler(srv, ss)
		}
		ctx, span := i.start(ss.Context(), info.FullMethod) //nolint:contextcheck // a gRPC stream carries its own context; there is no parameter to inherit
		defer func() {
			if p := recover(); p != nil {
				err = i.recovered(ctx, info.FullMethod, p)
			}
			i.finish(span, err)
		}()
		return handler(srv, &serverStream{ServerStream: ss, ctx: ctx, span: span})
	}
}

// ServerOptions returns the two chain options for grpc.NewServer.
func (i *Interceptors) ServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{grpc.ChainUnaryInterceptor(i.Unary()), grpc.ChainStreamInterceptor(i.Stream())}
}

func (i *Interceptors) start(ctx context.Context, fullMethod string) (context.Context, trace.Span) {
	md, _ := metadata.FromIncomingContext(ctx)
	ctx = i.cfg.Propagator.Extract(ctx, mdCarrier(md))
	service, method := splitMethod(fullMethod)
	attrs := []attribute.KeyValue{AttrRPCSystem.String("grpc"), AttrRPCService.String(service), AttrRPCMethod.String(method)}
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		attrs = append(attrs, AttrClientAddress.String(hostOf(p.Addr.String())))
	}
	return i.cfg.Tracer.Start(ctx, strings.TrimPrefix(fullMethod, "/"), trace.WithSpanKind(trace.SpanKindServer), trace.WithAttributes(attrs...))
}

func (i *Interceptors) finish(span trace.Span, err error) {
	code := status.Code(err)
	span.SetAttributes(AttrGRPCStatus.Int(int(code)))
	if err != nil {
		et := code.String()
		span.RecordError(err)
		span.SetAttributes(AttrErrorType.String(et))
		if serverError(code) {
			span.SetStatus(otelcodes.Error, et)
		}
	}
	span.End()
}

// recovered logs a panic with its stack and converts it to codes.Internal:
// one request fails, the server keeps serving.
func (i *Interceptors) recovered(ctx context.Context, fullMethod string, p any) error {
	i.cfg.Logger.ErrorContext(ctx, "panic recovered", "rpc", fullMethod, "panic", fmt.Sprint(p), "stack", string(debug.Stack()))
	return status.Error(codes.Internal, "internal error")
}

// serverError follows the RPC conventions: on the server only these codes
// mean the server failed; the rest are the client's doing.
func serverError(c codes.Code) bool {
	switch c {
	case codes.Unknown, codes.DeadlineExceeded, codes.Unimplemented, codes.Internal, codes.Unavailable, codes.DataLoss:
		return true
	default:
		return false
	}
}

func splitMethod(full string) (service, method string) {
	full = strings.TrimPrefix(full, "/")
	if i := strings.LastIndexByte(full, '/'); i >= 0 {
		return full[:i], full[i+1:]
	}
	return full, ""
}

func hostOf(addr string) string {
	if h, _, err := net.SplitHostPort(addr); err == nil {
		return h
	}
	return addr
}

// mdCarrier adapts gRPC metadata to the propagator.
type mdCarrier metadata.MD

func (c mdCarrier) Get(key string) string {
	if v := metadata.MD(c).Get(key); len(v) > 0 {
		return v[0]
	}
	return ""
}

func (c mdCarrier) Set(key, value string) { metadata.MD(c).Set(key, value) }

func (c mdCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

// serverStream gives the handler the span context and records messages.
type serverStream struct {
	grpc.ServerStream
	ctx  context.Context
	span trace.Span
	sent int
	recv int
}

func (s *serverStream) Context() context.Context { return s.ctx }

func (s *serverStream) SendMsg(m any) error {
	err := s.ServerStream.SendMsg(m)
	if err == nil {
		s.sent++
		s.span.AddEvent("message", trace.WithAttributes(attribute.String("rpc.message.type", "SENT"), attribute.Int("rpc.message.id", s.sent)))
	}
	return err
}

func (s *serverStream) RecvMsg(m any) error {
	err := s.ServerStream.RecvMsg(m)
	if err == nil {
		s.recv++
		s.span.AddEvent("message", trace.WithAttributes(attribute.String("rpc.message.type", "RECEIVED"), attribute.Int("rpc.message.id", s.recv)))
	}
	return err
}
