// Package metricspgx is the metrics hook for pgx: a pgx tracer that records
// the db.client.operation.duration histogram by operation, connection
// acquire waits and connect times, plus pool saturation gauges for pgxpool.
// It works for pgxpool and for database/sql through pgx's stdlib driver.
//
// It also owns the vocabulary both signals share: Operation turns SQL into
// a closed-set db.operation.name, ErrorType turns an error into a
// low-cardinality error.type, PoolName identifies a pool. The tracing hook
// in otelx/client/pgx imports them so spans and series agree.
package metricspgx

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"

	"github.com/anuar-fest/tools/metrics"
)

// InstrumentationName is the meter name.
const InstrumentationName = "github.com/anuar-fest/tools/metrics/pgx"

// Config configures New.
type Config struct {
	// Meter defaults to the global meter named InstrumentationName.
	Meter metric.Meter
}

// Tracer implements pgx.QueryTracer, pgx.BatchTracer, pgx.ConnectTracer
// and pgxpool.AcquireTracer with metrics only. Set it as ConnConfig.Tracer,
// or combine it with the tracing hook through multitracer.New.
type Tracer struct {
	opDuration  metric.Float64Histogram
	acquireWait metric.Float64Histogram
	connectTime metric.Float64Histogram
}

// New creates the instruments.
func New(cfg Config) (*Tracer, error) {
	if cfg.Meter == nil {
		cfg.Meter = otel.GetMeterProvider().Meter(InstrumentationName)
	}
	t := &Tracer{}
	var err error
	t.opDuration, err = cfg.Meter.Float64Histogram("db.client.operation.duration",
		metric.WithDescription("Duration of database operations."), metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(metrics.FastBuckets...))
	if err != nil {
		return nil, err
	}
	t.acquireWait, err = cfg.Meter.Float64Histogram("db.client.connection.wait_time",
		metric.WithDescription("Time waited to acquire a pooled connection."), metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(metrics.FastBuckets...))
	if err != nil {
		return nil, err
	}
	t.connectTime, err = cfg.Meter.Float64Histogram("db.client.connection.create_time",
		metric.WithDescription("Time to open a new connection."), metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(metrics.FastBuckets...))
	if err != nil {
		return nil, err
	}
	return t, nil
}

// stateKey is private to this package so the tracing hook's state in the
// same context never collides with ours under multitracer.
type stateKey struct{}

type state struct {
	start time.Time
	op    string
	attrs []attribute.KeyValue // connection attributes, reused on the sample
}

// TraceQueryStart implements pgx.QueryTracer.
func (t *Tracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, stateKey{}, &state{start: time.Now(), op: Operation(data.SQL), attrs: ConnAttrs(configOf(conn))})
}

// TraceQueryEnd implements pgx.QueryTracer.
func (t *Tracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	t.finish(ctx, data.Err)
}

// TraceBatchStart implements pgx.BatchTracer.
func (t *Tracer) TraceBatchStart(ctx context.Context, conn *pgx.Conn, _ pgx.TraceBatchStartData) context.Context {
	return context.WithValue(ctx, stateKey{}, &state{start: time.Now(), op: "BATCH", attrs: ConnAttrs(configOf(conn))})
}

// TraceBatchQuery implements pgx.BatchTracer; per-statement detail belongs
// to the span, the batch is one sample.
func (t *Tracer) TraceBatchQuery(context.Context, *pgx.Conn, pgx.TraceBatchQueryData) {}

// TraceBatchEnd implements pgx.BatchTracer.
func (t *Tracer) TraceBatchEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceBatchEndData) {
	t.finish(ctx, data.Err)
}

// TraceConnectStart implements pgx.ConnectTracer.
func (t *Tracer) TraceConnectStart(ctx context.Context, data pgx.TraceConnectStartData) context.Context {
	return context.WithValue(ctx, stateKey{}, &state{start: time.Now(), op: "CONNECT", attrs: ConnAttrs(data.ConnConfig)})
}

// TraceConnectEnd implements pgx.ConnectTracer.
func (t *Tracer) TraceConnectEnd(ctx context.Context, data pgx.TraceConnectEndData) {
	st, _ := ctx.Value(stateKey{}).(*state)
	if st == nil {
		return
	}
	mattrs := st.attrs
	if data.Err != nil {
		mattrs = append(append([]attribute.KeyValue(nil), mattrs...), semconv.ErrorTypeKey.String(ErrorType(data.Err)))
	}
	t.connectTime.Record(ctx, time.Since(st.start).Seconds(), metric.WithAttributes(mattrs...))
}

type acquireKey struct{}

// TraceAcquireStart implements pgxpool.AcquireTracer.
func (t *Tracer) TraceAcquireStart(ctx context.Context, _ *pgxpool.Pool, _ pgxpool.TraceAcquireStartData) context.Context {
	return context.WithValue(ctx, acquireKey{}, time.Now())
}

// TraceAcquireEnd implements pgxpool.AcquireTracer: the wait is recorded
// as db.client.connection.wait_time.
func (t *Tracer) TraceAcquireEnd(ctx context.Context, pool *pgxpool.Pool, data pgxpool.TraceAcquireEndData) {
	start, ok := ctx.Value(acquireKey{}).(time.Time)
	if !ok {
		return
	}
	attrs := []attribute.KeyValue{semconv.DBClientConnectionPoolName(PoolName(pool))}
	if data.Err != nil {
		attrs = append(attrs, semconv.ErrorTypeKey.String(ErrorType(data.Err)))
	}
	t.acquireWait.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(attrs...))
}

func (t *Tracer) finish(ctx context.Context, err error) {
	st, _ := ctx.Value(stateKey{}).(*state)
	if st == nil {
		return
	}
	mattrs := append(append(make([]attribute.KeyValue, 0, len(st.attrs)+2), st.attrs...), semconv.DBOperationName(st.op))
	if err != nil {
		mattrs = append(mattrs, semconv.ErrorTypeKey.String(ErrorType(err)))
	}
	t.opDuration.Record(ctx, time.Since(st.start).Seconds(), metric.WithAttributes(mattrs...))
}

func configOf(conn *pgx.Conn) *pgx.ConnConfig {
	if conn == nil {
		return nil
	}
	return conn.Config()
}

var (
	_ pgx.QueryTracer       = (*Tracer)(nil)
	_ pgx.BatchTracer       = (*Tracer)(nil)
	_ pgx.ConnectTracer     = (*Tracer)(nil)
	_ pgxpool.AcquireTracer = (*Tracer)(nil)
)
