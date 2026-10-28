package clientpgx_test

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/multitracer"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	metricspgx "github.com/anuar-fest/tools/metrics/pgx"
	clientpgx "github.com/anuar-fest/tools/otelx/client/pgx"
)

var dsn string

func TestMain(m *testing.M) {
	// -short skips the container; every integration test then skips through requireDSN.
	for _, a := range os.Args {
		if a == "-test.short=true" || a == "-test.short" {
			os.Exit(m.Run())
		}
	}
	ctx := context.Background()
	pgc, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("t"), postgres.WithUsername("t"), postgres.WithPassword("t"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(90*time.Second)))
	if err != nil {
		slog.Error("postgres container", "err", err)
		os.Exit(1)
	}
	dsn, err = pgc.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		slog.Error("dsn", "err", err)
		os.Exit(1)
	}
	code := m.Run()
	_ = testcontainers.TerminateContainer(pgc)
	os.Exit(code)
}

func requireDSN(t *testing.T) {
	t.Helper()
	if testing.Short() || dsn == "" {
		t.Skip("needs Docker; run without -short")
	}
}

type rig struct {
	exporter *tracetest.InMemoryExporter
	tracer   *clientpgx.Tracer
}

func newRig(t *testing.T, cfg clientpgx.Config) *rig {
	t.Helper()
	r := &rig{exporter: tracetest.NewInMemoryExporter()}
	cfg.Tracer = sdktrace.NewTracerProvider(sdktrace.WithSyncer(r.exporter)).Tracer("t")
	var err error
	r.tracer, err = clientpgx.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func (r *rig) span(name string) (tracetest.SpanStub, bool) {
	for _, s := range r.exporter.GetSpans() {
		if s.Name == name {
			return s, true
		}
	}
	return tracetest.SpanStub{}, false
}

func attrValue(attrs []attribute.KeyValue, key string) string {
	for _, a := range attrs {
		if string(a.Key) == key {
			return a.Value.String()
		}
	}
	return ""
}

func TestTracer_NilConnAndMissingStateAreSafe(t *testing.T) {
	r := newRig(t, clientpgx.Config{MaxQueryTextBytes: 8})
	ctx := r.tracer.TraceQueryStart(context.Background(), nil, pgx.TraceQueryStartData{SQL: "select 1, 2, 3, 4"})
	r.tracer.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{Err: &pgconn.PgError{Code: "57014"}})
	r.tracer.TraceQueryEnd(context.Background(), nil, pgx.TraceQueryEndData{}) // never started: no-op
	r.tracer.TraceBatchEnd(context.Background(), nil, pgx.TraceBatchEndData{})
	r.tracer.TraceConnectEnd(context.Background(), pgx.TraceConnectEndData{})
	r.tracer.TraceAcquireEnd(context.Background(), nil, pgxpool.TraceAcquireEndData{})
	s, ok := r.span("SELECT")
	if !ok || attrValue(s.Attributes, "db.system.name") != "postgresql" || attrValue(s.Attributes, "db.query.text") != "select 1…" || attrValue(s.Attributes, "error.type") != "57014" || s.Status.Code.String() != "Error" {
		t.Fatalf("%+v", s)
	}
	omit := newRig(t, clientpgx.Config{OmitQueryText: true})
	ctx = omit.tracer.TraceQueryStart(context.Background(), nil, pgx.TraceQueryStartData{SQL: "select 1"})
	omit.tracer.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{})
	if s, _ := omit.span("SELECT"); attrValue(s.Attributes, "db.query.text") != "" {
		t.Fatal("OmitQueryText must drop the text")
	}
}

func TestTracer_SlowAcquireBecomesAnEvent(t *testing.T) {
	r := newRig(t, clientpgx.Config{SlowAcquire: time.Nanosecond})
	tr := sdktrace.NewTracerProvider(sdktrace.WithSyncer(r.exporter)).Tracer("t")
	pctx, span := tr.Start(context.Background(), "parent")
	actx := r.tracer.TraceAcquireStart(pctx, nil, pgxpool.TraceAcquireStartData{})
	time.Sleep(time.Microsecond)
	r.tracer.TraceAcquireEnd(actx, nil, pgxpool.TraceAcquireEndData{})
	span.End()
	s, _ := r.span("parent")
	if len(s.Events) != 1 || s.Events[0].Name != "db.connection.acquire" {
		t.Fatalf("slow acquire must be an event on the active span: %+v", s.Events)
	}
}

func TestIntegration_PoolSpans(t *testing.T) {
	requireDSN(t)
	ctx := context.Background()
	r := newRig(t, clientpgx.Config{})
	pool, err := clientpgx.NewPool(ctx, dsn, r.tracer, func(c *pgxpool.Config) { c.MaxConns = 2 })
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	var n int
	if err := pool.QueryRow(ctx, "select 1").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "select * from nope"); err == nil {
		t.Fatal("missing table must fail")
	}
	b := &pgx.Batch{}
	b.Queue("select 1")
	b.Queue("select 2")
	if err := pool.SendBatch(ctx, b).Close(); err != nil {
		t.Fatal(err)
	}

	if _, ok := r.span("CONNECT t"); !ok {
		t.Fatalf("connect span missing: %v", names(r))
	}
	var failed tracetest.SpanStub
	for _, s := range r.exporter.GetSpans() {
		if s.Name == "SELECT t" && s.Status.Code.String() == "Error" {
			failed = s
		}
	}
	if attrValue(failed.Attributes, "error.type") != "42P01" || attrValue(failed.Attributes, "db.namespace") != "t" || attrValue(failed.Attributes, "db.query.text") != "select * from nope" || failed.SpanKind != trace.SpanKindClient {
		t.Fatalf("failed query span: %+v", failed)
	}
	batch, ok := r.span("BATCH t")
	if !ok || attrValue(batch.Attributes, "db.operation.batch.size") != "2" || len(batch.Events) != 2 || batch.Events[0].Name != "db.batch.query" {
		t.Fatalf("batch span: %+v", batch)
	}
}

func TestIntegration_MultitracerCombinesSpansAndMetrics(t *testing.T) {
	requireDSN(t)
	ctx := context.Background()
	r := newRig(t, clientpgx.Config{})
	reader := sdkmetric.NewManualReader()
	mt, err := metricspgx.New(metricspgx.Config{Meter: sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("m")})
	if err != nil {
		t.Fatal(err)
	}
	pool, err := clientpgx.NewPool(ctx, dsn, multitracer.New(r.tracer, mt), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var n int
	if err := pool.QueryRow(ctx, "select 7").Scan(&n); err != nil || n != 7 {
		t.Fatal(err, n)
	}
	if _, ok := r.span("SELECT t"); !ok {
		t.Fatalf("span missing: %v", names(r))
	}
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			seen[m.Name] = true
		}
	}
	for _, name := range []string{"db.client.operation.duration", "db.client.connection.wait_time", "db.client.connection.create_time"} {
		if !seen[name] {
			t.Fatalf("%s must be recorded through multitracer: %v", name, seen)
		}
	}
}

func TestIntegration_OpenDBTracesDatabaseSQL(t *testing.T) {
	requireDSN(t)
	ctx := context.Background()
	r := newRig(t, clientpgx.Config{})
	db, err := clientpgx.OpenDB(dsn, r.tracer)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var n int
	if err := db.QueryRowContext(ctx, "select 42").Scan(&n); err != nil || n != 42 {
		t.Fatal(err, n)
	}
	s, ok := r.span("SELECT t")
	if !ok || attrValue(s.Attributes, "db.query.text") != "select 42" {
		t.Fatalf("database/sql query must be traced: %v", names(r))
	}
	if _, err := clientpgx.OpenDB("://bad", r.tracer); err == nil {
		t.Fatal("bad dsn must fail")
	}
	plain, err := clientpgx.OpenDB(dsn, nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = plain.Close()
}

func names(r *rig) []string {
	spans := r.exporter.GetSpans()
	out := make([]string, 0, len(spans))
	for _, s := range spans {
		out = append(out, s.Name)
	}
	return out
}
