// Package clientpgx is the tracing hook for pgx: one client span per query,
// batch and connect with the database semantic conventions, and an event
// on the active span when a pool acquire waited. It works for pgxpool and
// for database/sql through OpenDB, which is what replaces
// sql.Open("pgx", dsn). Pair it with metrics/pgx through multitracer.New
// for the duration histograms and pool gauges.
//
// Query arguments never leave the process. The query text does by default
// because pgx sends parameters separately, so the text holds no values;
// turn it off (OmitQueryText) where SQL is assembled from user input.
package clientpgx

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	metricspgx "github.com/anuar-fest/tools/metrics/pgx"
)

// InstrumentationName is the tracer name.
const InstrumentationName = "github.com/anuar-fest/tools/otelx/client/pgx"

// Config configures New.
type Config struct {
	// Tracer defaults to the global tracer named InstrumentationName.
	Tracer trace.Tracer
	// OmitQueryText keeps db.query.text off spans.
	OmitQueryText bool
	// MaxQueryTextBytes truncates db.query.text; default 1024.
	MaxQueryTextBytes int
	// SlowAcquire is the pool wait above which an event is added to the
	// active span; default 1ms. Shorter waits are the pool handing over an
	// idle connection; longer ones are contention worth seeing in the trace.
	SlowAcquire time.Duration
}

// Tracer implements pgx.QueryTracer, pgx.BatchTracer, pgx.ConnectTracer
// and pgxpool.AcquireTracer. Set it as ConnConfig.Tracer, alone or through
// multitracer.New together with the metrics hook.
type Tracer struct {
	tracer      trace.Tracer
	omitText    bool
	maxText     int
	slowAcquire time.Duration
}

// New creates the tracer.
func New(cfg Config) (*Tracer, error) {
	if cfg.Tracer == nil {
		cfg.Tracer = otel.Tracer(InstrumentationName)
	}
	t := &Tracer{tracer: cfg.Tracer, omitText: cfg.OmitQueryText, maxText: cfg.MaxQueryTextBytes, slowAcquire: cfg.SlowAcquire}
	if t.maxText <= 0 {
		t.maxText = 1024
	}
	if t.slowAcquire <= 0 {
		t.slowAcquire = time.Millisecond
	}
	return t, nil
}

type stateKey struct{}

type state struct {
	op string
}

// TraceQueryStart implements pgx.QueryTracer.
func (t *Tracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	op := metricspgx.Operation(data.SQL)
	cattrs := metricspgx.ConnAttrs(configOf(conn))
	attrs := append(append(make([]attribute.KeyValue, 0, len(cattrs)+2), cattrs...), semconv.DBOperationName(op))
	if !t.omitText {
		attrs = append(attrs, semconv.DBQueryText(t.truncate(data.SQL)))
	}
	ctx, _ = t.tracer.Start(ctx, spanName(op, cattrs), trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(attrs...))
	return context.WithValue(ctx, stateKey{}, &state{op: op})
}

// TraceQueryEnd implements pgx.QueryTracer.
func (t *Tracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	st, _ := ctx.Value(stateKey{}).(*state)
	if st == nil {
		return
	}
	span := trace.SpanFromContext(ctx)
	if rows := data.CommandTag.RowsAffected(); rows > 0 {
		span.SetAttributes(attribute.Int64("db.response.returned_rows", rows))
	}
	finish(span, st.op, data.Err)
}

// TraceBatchStart implements pgx.BatchTracer.
func (t *Tracer) TraceBatchStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchStartData) context.Context {
	cattrs := metricspgx.ConnAttrs(configOf(conn))
	attrs := append(append(make([]attribute.KeyValue, 0, len(cattrs)+2), cattrs...), semconv.DBOperationName("BATCH"))
	if data.Batch != nil {
		attrs = append(attrs, semconv.DBOperationBatchSize(data.Batch.Len()))
	}
	ctx, _ = t.tracer.Start(ctx, spanName("BATCH", cattrs), trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(attrs...))
	return context.WithValue(ctx, stateKey{}, &state{op: "BATCH"})
}

// TraceBatchQuery implements pgx.BatchTracer: one event per statement.
func (t *Tracer) TraceBatchQuery(ctx context.Context, _ *pgx.Conn, data pgx.TraceBatchQueryData) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}
	attrs := []attribute.KeyValue{semconv.DBOperationName(metricspgx.Operation(data.SQL))}
	if !t.omitText {
		attrs = append(attrs, semconv.DBQueryText(t.truncate(data.SQL)))
	}
	if data.Err != nil {
		attrs = append(attrs, semconv.ErrorTypeKey.String(metricspgx.ErrorType(data.Err)))
	}
	span.AddEvent("db.batch.query", trace.WithAttributes(attrs...))
}

// TraceBatchEnd implements pgx.BatchTracer.
func (t *Tracer) TraceBatchEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceBatchEndData) {
	st, _ := ctx.Value(stateKey{}).(*state)
	if st == nil {
		return
	}
	finish(trace.SpanFromContext(ctx), st.op, data.Err)
}

// TraceConnectStart implements pgx.ConnectTracer.
func (t *Tracer) TraceConnectStart(ctx context.Context, data pgx.TraceConnectStartData) context.Context {
	cattrs := metricspgx.ConnAttrs(data.ConnConfig)
	ctx, _ = t.tracer.Start(ctx, spanName("CONNECT", cattrs), trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(cattrs...))
	return context.WithValue(ctx, stateKey{}, &state{op: "CONNECT"})
}

// TraceConnectEnd implements pgx.ConnectTracer.
func (t *Tracer) TraceConnectEnd(ctx context.Context, data pgx.TraceConnectEndData) {
	st, _ := ctx.Value(stateKey{}).(*state)
	if st == nil {
		return
	}
	finish(trace.SpanFromContext(ctx), st.op, data.Err)
}

type acquireKey struct{}

// TraceAcquireStart implements pgxpool.AcquireTracer.
func (t *Tracer) TraceAcquireStart(ctx context.Context, _ *pgxpool.Pool, _ pgxpool.TraceAcquireStartData) context.Context {
	return context.WithValue(ctx, acquireKey{}, time.Now())
}

// TraceAcquireEnd implements pgxpool.AcquireTracer: a slow or failed wait
// becomes an event on the active span.
func (t *Tracer) TraceAcquireEnd(ctx context.Context, pool *pgxpool.Pool, data pgxpool.TraceAcquireEndData) {
	start, ok := ctx.Value(acquireKey{}).(time.Time)
	if !ok {
		return
	}
	elapsed := time.Since(start)
	if elapsed < t.slowAcquire && data.Err == nil {
		return
	}
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}
	attrs := []attribute.KeyValue{semconv.DBClientConnectionPoolName(metricspgx.PoolName(pool)), attribute.Float64("wait_ms", float64(elapsed)/float64(time.Millisecond))}
	if data.Err != nil {
		attrs = append(attrs, semconv.ErrorTypeKey.String(metricspgx.ErrorType(data.Err)))
	}
	span.AddEvent("db.connection.acquire", trace.WithAttributes(attrs...))
}

func finish(span trace.Span, op string, err error) {
	if err != nil {
		et := metricspgx.ErrorType(err)
		span.RecordError(err)
		span.SetAttributes(semconv.ErrorTypeKey.String(et))
		span.SetStatus(codes.Error, op+" failed")
	}
	span.End()
}

func (t *Tracer) truncate(sql string) string {
	if len(sql) <= t.maxText {
		return sql
	}
	return sql[:t.maxText] + "…"
}

func configOf(conn *pgx.Conn) *pgx.ConnConfig {
	if conn == nil {
		return nil
	}
	return conn.Config()
}

func spanName(op string, cattrs []attribute.KeyValue) string {
	for _, a := range cattrs {
		if a.Key == semconv.DBNamespaceKey {
			return op + " " + a.Value.AsString()
		}
	}
	return op
}

var (
	_ pgx.QueryTracer       = (*Tracer)(nil)
	_ pgx.BatchTracer       = (*Tracer)(nil)
	_ pgx.ConnectTracer     = (*Tracer)(nil)
	_ pgxpool.AcquireTracer = (*Tracer)(nil)
)
