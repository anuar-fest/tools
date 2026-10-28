package metricsgrpc

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	"github.com/anuar-fest/tools/metrics"
)

// ClientConfig configures NewClient.
type ClientConfig struct {
	// Meter defaults to the global meter named InstrumentationName.
	Meter metric.Meter
	// PeerService names the dependency on the metric.
	PeerService string
}

// Client measures outbound RPCs:
//
//	rpc.client.call.duration histogram (s) rpc.system, rpc.service, rpc.method, server.address, server.port, peer.service, rpc.grpc.status_code, error.type
type Client struct {
	cfg      ClientConfig
	duration metric.Float64Histogram
}

// NewClient creates the instrument.
func NewClient(cfg ClientConfig) (*Client, error) {
	if cfg.Meter == nil {
		cfg.Meter = otel.GetMeterProvider().Meter(InstrumentationName)
	}
	d, err := cfg.Meter.Float64Histogram("rpc.client.call.duration",
		metric.WithDescription("Duration of outbound RPCs."), metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(metrics.LatencyBuckets...))
	if err != nil {
		return nil, err
	}
	return &Client{cfg: cfg, duration: d}, nil
}

// DialOptions returns the interceptor chain for grpc.NewClient. Append it
// after clientgrpc.DialOptions, which also carries the keepalive
// parameters; this package sets none.
func (c *Client) DialOptions() []grpc.DialOption {
	return []grpc.DialOption{grpc.WithChainUnaryInterceptor(c.Unary()), grpc.WithChainStreamInterceptor(c.Stream())}
}

// Unary returns the unary client interceptor.
func (c *Client) Unary() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		attrs := c.attrs(method, cc)
		start := time.Now()
		err := invoker(ctx, method, req, reply, cc, opts...)
		c.finish(ctx, attrs, start, err)
		return err
	}
}

// Stream returns the stream client interceptor. The sample is recorded
// once, when the stream ends: a receive error (io.EOF included), a send
// error, or the context ending.
func (c *Client) Stream() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		attrs := c.attrs(method, cc)
		start := time.Now()
		cs, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			c.finish(ctx, attrs, start, err)
			return nil, err
		}
		w := &clientStream{ClientStream: cs, finish: func(err error) { c.finish(ctx, attrs, start, err) }}
		go func() {
			// The stream's context ending is the last way a stream can finish.
			<-cs.Context().Done()
			w.end(status.FromContextError(cs.Context().Err()).Err()) // Canceled or DeadlineExceeded, not Unknown
		}()
		return w, nil
	}
}

func (c *Client) attrs(method string, cc *grpc.ClientConn) attribute.Set {
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
	if c.cfg.PeerService != "" {
		kvs = append(kvs, AttrPeerService.String(c.cfg.PeerService))
	}
	return attribute.NewSet(kvs...)
}

func (c *Client) finish(ctx context.Context, attrs attribute.Set, start time.Time, err error) {
	c.duration.Record(ctx, time.Since(start).Seconds(), metric.WithAttributeSet(withStatus(attrs, err)))
}

// hostPort reads "host:port" or "scheme:///host:port" targets.
func hostPort(target string) (string, int) {
	if i := indexOf(target, "://"); i >= 0 {
		target = target[i+3:]
		if len(target) > 0 && target[0] == '/' {
			target = target[1:]
		}
	}
	host, portStr, err := net.SplitHostPort(target)
	if err != nil {
		return target, 0
	}
	port := 0
	for _, ch := range portStr {
		if ch < '0' || ch > '9' {
			return host, 0
		}
		port = port*10 + int(ch-'0')
	}
	return host, port
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

// clientStream records the sample once, whichever way the stream finishes.
type clientStream struct {
	grpc.ClientStream
	finish func(error)
	once   sync.Once
}

func (s *clientStream) end(err error) { s.once.Do(func() { s.finish(err) }) }

func (s *clientStream) SendMsg(m any) error {
	err := s.ClientStream.SendMsg(m)
	if err != nil {
		s.end(err)
	}
	return err
}

func (s *clientStream) RecvMsg(m any) error {
	err := s.ClientStream.RecvMsg(m)
	if err != nil {
		if errors.Is(err, io.EOF) {
			s.end(nil) // a clean end of stream is a success
		} else {
			s.end(err)
		}
	}
	return err
}
