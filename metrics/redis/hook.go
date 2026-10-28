// Package metricsredis is the metrics hook for go-redis v9: the
// db.client.operation.duration histogram per command and per pipeline,
// db.client.connection.create_time per dial, and the pool's saturation.
// Keys never become labels; otelx/client/redis is the tracing side.
package metricsredis

import (
	"context"
	"errors"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"

	"github.com/anuar-fest/tools/metrics"
)

// InstrumentationName is the meter name.
const InstrumentationName = "github.com/anuar-fest/tools/metrics/redis"

// Config configures NewHook.
type Config struct {
	// Meter defaults to the global meter named InstrumentationName.
	Meter metric.Meter
}

// Hook implements redis.Hook. Add it with client.AddHook(hook), after the
// tracing hook so the sample is recorded inside the command's span.
type Hook struct {
	connAttrs   []attribute.KeyValue
	duration    metric.Float64Histogram
	connectTime metric.Float64Histogram
}

// NewHook creates the instruments. opts gives server.address and the
// database index for attributes; nil is allowed.
func NewHook(cfg Config, opts *redis.Options) (*Hook, error) {
	if cfg.Meter == nil {
		cfg.Meter = otel.GetMeterProvider().Meter(InstrumentationName)
	}
	h := &Hook{connAttrs: ConnAttrs(opts)}
	var err error
	h.duration, err = cfg.Meter.Float64Histogram("db.client.operation.duration",
		metric.WithDescription("Duration of database operations."), metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(metrics.FastBuckets...))
	if err != nil {
		return nil, err
	}
	h.connectTime, err = cfg.Meter.Float64Histogram("db.client.connection.create_time",
		metric.WithDescription("Time to open a new connection."), metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(metrics.FastBuckets...))
	if err != nil {
		return nil, err
	}
	return h, nil
}

// ConnAttrs is the closed attribute set that identifies the server:
// db.system.name=redis, server.address, server.port, db.namespace. The
// tracing hook puts the same set on its spans.
func ConnAttrs(opts *redis.Options) []attribute.KeyValue {
	attrs := []attribute.KeyValue{semconv.DBSystemNameKey.String("redis")}
	if opts == nil {
		return attrs
	}
	if host, port, err := net.SplitHostPort(opts.Addr); err == nil {
		attrs = append(attrs, semconv.ServerAddress(host))
		if p, err := strconv.Atoi(port); err == nil {
			attrs = append(attrs, semconv.ServerPort(p))
		}
	} else if opts.Addr != "" {
		attrs = append(attrs, semconv.ServerAddress(opts.Addr))
	}
	attrs = append(attrs, semconv.DBNamespace(strconv.Itoa(opts.DB)))
	return attrs
}

// DialHook implements redis.Hook.
func (h *Hook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		start := time.Now()
		conn, err := next(ctx, network, addr)
		mattrs := h.connAttrs
		if err != nil {
			mattrs = append(append([]attribute.KeyValue(nil), mattrs...), semconv.ErrorTypeKey.String(ErrorType(err)))
		}
		h.connectTime.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(mattrs...))
		return conn, err
	}
}

// ProcessHook implements redis.Hook: one sample per command.
func (h *Hook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		start := time.Now()
		err := next(ctx, cmd)
		h.record(ctx, strings.ToUpper(cmd.Name()), start, err)
		return err
	}
}

// ProcessPipelineHook implements redis.Hook: one sample per pipeline.
func (h *Hook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		start := time.Now()
		err := next(ctx, cmds)
		h.record(ctx, "PIPELINE", start, err)
		return err
	}
}

func (h *Hook) record(ctx context.Context, op string, start time.Time, err error) {
	mattrs := append(append(make([]attribute.KeyValue, 0, len(h.connAttrs)+2), h.connAttrs...), semconv.DBOperationName(op))
	if err != nil && !errors.Is(err, redis.Nil) { // a miss is an answer, not a failure
		mattrs = append(mattrs, semconv.ErrorTypeKey.String(ErrorType(err)))
	}
	h.duration.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(mattrs...))
}

// ErrorType is the low-cardinality error.type for a Redis error: the
// server's error prefix (WRONGTYPE, NOAUTH, READONLY...), or the generic
// classification.
func ErrorType(err error) string {
	var rerr redis.Error
	if errors.As(err, &rerr) {
		msg := rerr.Error()
		if i := strings.IndexByte(msg, ' '); i > 0 && strings.ToUpper(msg[:i]) == msg[:i] {
			return msg[:i]
		}
		return "redis.Error"
	}
	return metrics.ErrorType(err)
}

var _ redis.Hook = (*Hook)(nil)
