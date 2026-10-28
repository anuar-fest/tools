// Package clientredis is the tracing hook for go-redis v9: one client span
// per command and per pipeline with the database semantic conventions, and
// a CONNECT span per dial. Keys are not recorded unless IncludeKeys is set:
// they often carry user and tenant ids. metrics/redis measures the same
// commands.
package clientredis

import (
	"context"
	"errors"
	"net"
	"strings"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	metricsredis "github.com/anuar-fest/tools/metrics/redis"
)

// InstrumentationName is the tracer name.
const InstrumentationName = "github.com/anuar-fest/tools/otelx/client/redis"

// Config configures NewHook.
type Config struct {
	// Tracer defaults to the global tracer named InstrumentationName.
	Tracer trace.Tracer
	// IncludeKeys puts "GET key" in db.query.text instead of "GET".
	IncludeKeys bool
	// MaxQueryTextBytes truncates db.query.text; default 256.
	MaxQueryTextBytes int
}

// Hook implements redis.Hook. Add it with client.AddHook(hook), before the
// metrics hook so the duration sample lands inside the span.
type Hook struct {
	cfg       Config
	connAttrs []attribute.KeyValue
}

// NewHook builds the hook. opts gives server.address and the database
// index for attributes; nil is allowed.
func NewHook(cfg Config, opts *redis.Options) (*Hook, error) {
	if cfg.Tracer == nil {
		cfg.Tracer = otel.Tracer(InstrumentationName)
	}
	if cfg.MaxQueryTextBytes <= 0 {
		cfg.MaxQueryTextBytes = 256
	}
	return &Hook{cfg: cfg, connAttrs: metricsredis.ConnAttrs(opts)}, nil
}

// DialHook implements redis.Hook.
func (h *Hook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		ctx, span := h.cfg.Tracer.Start(ctx, "CONNECT", trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(h.connAttrs...))
		conn, err := next(ctx, network, addr)
		if err != nil {
			span.RecordError(err)
			span.SetAttributes(semconv.ErrorTypeKey.String(metricsredis.ErrorType(err)))
			span.SetStatus(codes.Error, "connect failed")
		}
		span.End()
		return conn, err
	}
}

// ProcessHook implements redis.Hook: one span per command named after it.
func (h *Hook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		op := strings.ToUpper(cmd.Name())
		attrs := append(append(make([]attribute.KeyValue, 0, len(h.connAttrs)+2), h.connAttrs...), semconv.DBOperationName(op), semconv.DBQueryText(h.queryText(cmd)))
		ctx, span := h.cfg.Tracer.Start(ctx, op, trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(attrs...))
		err := next(ctx, cmd)
		h.finish(span, op, err)
		return err
	}
}

// ProcessPipelineHook implements redis.Hook: one span per pipeline, one
// event per command.
func (h *Hook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		attrs := append(append(make([]attribute.KeyValue, 0, len(h.connAttrs)+2), h.connAttrs...), semconv.DBOperationName("PIPELINE"), semconv.DBOperationBatchSize(len(cmds)))
		ctx, span := h.cfg.Tracer.Start(ctx, "PIPELINE", trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(attrs...))
		err := next(ctx, cmds)
		if span.IsRecording() {
			for _, cmd := range cmds {
				evAttrs := []attribute.KeyValue{semconv.DBOperationName(strings.ToUpper(cmd.Name()))}
				if cmdErr := cmd.Err(); cmdErr != nil && !errors.Is(cmdErr, redis.Nil) {
					evAttrs = append(evAttrs, semconv.ErrorTypeKey.String(metricsredis.ErrorType(cmdErr)))
				}
				span.AddEvent("db.pipeline.command", trace.WithAttributes(evAttrs...))
			}
		}
		h.finish(span, "PIPELINE", err)
		return err
	}
}

func (h *Hook) finish(span trace.Span, op string, err error) {
	if err != nil && !errors.Is(err, redis.Nil) { // a miss is an answer, not a failure
		span.RecordError(err)
		span.SetAttributes(semconv.ErrorTypeKey.String(metricsredis.ErrorType(err)))
		span.SetStatus(codes.Error, op+" failed")
	}
	span.End()
}

func (h *Hook) queryText(cmd redis.Cmder) string {
	if !h.cfg.IncludeKeys || len(cmd.Args()) < 2 {
		return strings.ToUpper(cmd.Name())
	}
	text := strings.ToUpper(cmd.Name()) + " " + strings.TrimSpace(strings.SplitN(cmd.String(), " ", 3)[1])
	if len(text) > h.cfg.MaxQueryTextBytes {
		return text[:h.cfg.MaxQueryTextBytes] + "…"
	}
	return text
}

var _ redis.Hook = (*Hook)(nil)
