// Package clientgrpc traces outbound gRPC calls: one client span per RPC
// with the RPC semantic conventions, the trace context injected into the
// metadata, and dial options with keepalive defaults. The
// rpc.client.call.duration histogram is metrics/grpc; append its
// DialOptions after these.
package clientgrpc

import (
	"context"
	"net"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// InstrumentationName is the tracer name.
const InstrumentationName = "github.com/anuar-fest/tools/otelx/client/grpc"

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

// Config configures New.
type Config struct {
	// Tracer defaults to the global tracer named InstrumentationName.
	Tracer trace.Tracer
	// Propagator defaults to the global propagator.
	Propagator propagation.TextMapPropagator
	// PeerService names the dependency on spans.
	PeerService string
	// KeepaliveTime pings an idle connection; default 30s. KeepaliveTimeout
	// waits for the ack; default 10s. Keep KeepaliveTime above the server's
	// MinClientPingInterval or it will drop the connection.
	KeepaliveTime    time.Duration
	KeepaliveTimeout time.Duration
}

// Interceptors traces a client connection.
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
	if cfg.KeepaliveTime <= 0 {
		cfg.KeepaliveTime = 30 * time.Second
	}
	if cfg.KeepaliveTimeout <= 0 {
		cfg.KeepaliveTimeout = 10 * time.Second
	}
	return &Interceptors{cfg: cfg}, nil
}

// DialOptions returns the interceptor chain and keepalive parameters for
// grpc.NewClient. Add credentials yourself; this package never chooses
// them. Append metricsgrpc.DialOptions after these so the duration sample
// is recorded inside the span.
func (i *Interceptors) DialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithChainUnaryInterceptor(i.Unary()),
		grpc.WithChainStreamInterceptor(i.Stream()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: i.cfg.KeepaliveTime, Timeout: i.cfg.KeepaliveTimeout}),
	}
}

// Unary returns the unary client interceptor.
func (i *Interceptors) Unary() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx, span := i.start(ctx, method, cc)
		err := invoker(ctx, method, req, reply, cc, opts...)
		i.finish(span, err)
		return err
	}
}

// Stream returns the stream client interceptor. The span ends when the
// stream ends: a receive error (io.EOF included), a send error, or the
// context ending.
func (i *Interceptors) Stream() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx, span := i.start(ctx, method, cc)
		cs, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			i.finish(span, err)
			return nil, err
		}
		w := &clientStream{ClientStream: cs, span: span, finish: func(err error) { i.finish(span, err) }}
		go func() {
			// The stream's context ending is the last way a stream can finish.
			<-cs.Context().Done()
			w.end(cs.Context().Err())
		}()
		return w, nil
	}
}

func (i *Interceptors) start(ctx context.Context, method string, cc *grpc.ClientConn) (context.Context, trace.Span) {
	service, m := splitMethod(method)
	kvs := []attribute.KeyValue{AttrRPCSystem.String("grpc"), AttrRPCService.String(service), AttrRPCMethod.String(m)}
	if cc != nil {
		if host, port := hostPort(cc.Target()); host != "" {
			kvs = append(kvs, AttrServerAddress.String(host))
			if port > 0 {
				kvs = append(kvs, AttrServerPort.Int(port))
			}
		}
	}
	if i.cfg.PeerService != "" {
		kvs = append(kvs, AttrPeerService.String(i.cfg.PeerService))
	}
	ctx, span := i.cfg.Tracer.Start(ctx, strings.TrimPrefix(method, "/"), trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(kvs...))
	md, ok := metadata.FromOutgoingContext(ctx)
	if ok {
		md = md.Copy()
	} else {
		md = metadata.MD{}
	}
	i.cfg.Propagator.Inject(ctx, mdCarrier(md))
	return metadata.NewOutgoingContext(ctx, md), span
}

func (i *Interceptors) finish(span trace.Span, err error) {
	code := status.Code(err)
	span.SetAttributes(AttrGRPCStatus.Int(int(code)))
	if err != nil {
		et := code.String()
		span.RecordError(err)
		span.SetAttributes(AttrErrorType.String(et))
		span.SetStatus(otelcodes.Error, et) // on the client every non-OK status is an error
	}
	span.End()
}

func splitMethod(full string) (service, method string) {
	full = strings.TrimPrefix(full, "/")
	if i := strings.LastIndexByte(full, '/'); i >= 0 {
		return full[:i], full[i+1:]
	}
	return full, ""
}

// hostPort reads "host:port" or "scheme:///host:port" targets.
func hostPort(target string) (string, int) {
	if i := strings.Index(target, "://"); i >= 0 {
		target = strings.TrimPrefix(target[i+3:], "/")
	}
	host, portStr, err := net.SplitHostPort(target)
	if err != nil {
		return target, 0
	}
	port := 0
	for _, c := range portStr {
		if c < '0' || c > '9' {
			return host, 0
		}
		port = port*10 + int(c-'0')
	}
	return host, port
}

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

// clientStream ends the span once, whichever way the stream finishes.
type clientStream struct {
	grpc.ClientStream
	span   trace.Span
	finish func(error)
	once   sync.Once
	sent   int
	recv   int
}

func (s *clientStream) end(err error) { s.once.Do(func() { s.finish(err) }) }

func (s *clientStream) SendMsg(m any) error {
	err := s.ClientStream.SendMsg(m)
	if err != nil {
		s.end(err)
		return err
	}
	s.sent++
	s.span.AddEvent("message", trace.WithAttributes(attribute.String("rpc.message.type", "SENT"), attribute.Int("rpc.message.id", s.sent)))
	return nil
}

func (s *clientStream) RecvMsg(m any) error {
	err := s.ClientStream.RecvMsg(m)
	if err != nil {
		if err.Error() == "EOF" {
			s.end(nil) // a clean end of stream is a success
		} else {
			s.end(err)
		}
		return err
	}
	s.recv++
	s.span.AddEvent("message", trace.WithAttributes(attribute.String("rpc.message.type", "RECEIVED"), attribute.Int("rpc.message.id", s.recv)))
	return nil
}

func (s *clientStream) CloseSend() error {
	return s.ClientStream.CloseSend()
}
