package metricspgx_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	metricspgx "github.com/anuar-fest/tools/metrics/pgx"
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
	reader *sdkmetric.ManualReader
	mp     *sdkmetric.MeterProvider
	tracer *metricspgx.Tracer
}

func newRig(t *testing.T) *rig {
	t.Helper()
	r := &rig{reader: sdkmetric.NewManualReader()}
	r.mp = sdkmetric.NewMeterProvider(sdkmetric.WithReader(r.reader))
	var err error
	r.tracer, err = metricspgx.New(metricspgx.Config{Meter: r.mp.Meter("m")})
	if err != nil {
		t.Fatal(err)
	}
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

func TestOperation(t *testing.T) {
	cases := map[string]string{
		"select 1":                             "SELECT",
		"  \n\tINSERT INTO t VALUES (1)":       "INSERT",
		"-- comment\nupdate t set a = 1":       "UPDATE",
		"/* c */ /* d */ delete from t":        "DELETE",
		"with x as (select 1) select * from x": "WITH",
		"begin":                                "BEGIN",
		";":                                    "OTHER",
		"":                                     "OTHER",
		"-- only a comment":                    "OTHER",
		"/* unterminated":                      "OTHER",
		"SELECTX":                              "OTHER",
		"explain analyze select 1":             "EXPLAIN",
	}
	for sql, want := range cases {
		if got := metricspgx.Operation(sql); got != want {
			t.Errorf("Operation(%q) = %q, want %q", sql, got, want)
		}
	}
}

func TestErrorType(t *testing.T) {
	cases := map[string]error{
		"42P01":                    &pgconn.PgError{Code: "42P01"},
		"context.Canceled":         context.Canceled,
		"context.DeadlineExceeded": context.DeadlineExceeded,
		"pgx.ErrNoRows":            pgx.ErrNoRows,
		"errors.errorString":       errors.New("x"),
		"":                         nil,
	}
	for want, err := range cases {
		if got := metricspgx.ErrorType(err); got != want {
			t.Errorf("ErrorType(%v) = %q, want %q", err, got, want)
		}
	}
}

func TestTracer_NilConnAndMissingStateAreSafe(t *testing.T) {
	r := newRig(t)
	ctx := r.tracer.TraceQueryStart(context.Background(), nil, pgx.TraceQueryStartData{SQL: "select 1"})
	r.tracer.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{Err: &pgconn.PgError{Code: "57014"}})
	r.tracer.TraceQueryEnd(context.Background(), nil, pgx.TraceQueryEndData{}) // never started: no-op
	r.tracer.TraceBatchEnd(context.Background(), nil, pgx.TraceBatchEndData{})
	r.tracer.TraceConnectEnd(context.Background(), pgx.TraceConnectEndData{})
	r.tracer.TraceAcquireEnd(context.Background(), nil, pgxpool.TraceAcquireEndData{})
	hist := r.metrics(t)["db.client.operation.duration"].Data.(metricdata.Histogram[float64])
	if len(hist.DataPoints) != 1 {
		t.Fatalf("one sample expected: %+v", hist.DataPoints)
	}
	dp := hist.DataPoints[0]
	sys, _ := dp.Attributes.Value("db.system.name")
	op, _ := dp.Attributes.Value("db.operation.name")
	et, _ := dp.Attributes.Value("error.type")
	if sys.AsString() != "postgresql" || op.AsString() != "SELECT" || et.AsString() != "57014" || len(dp.Bounds) != 16 {
		t.Fatalf("sample: %v bounds=%d", dp.Attributes, len(dp.Bounds))
	}
	if metricspgx.PoolName(nil) != "" || len(metricspgx.ConnAttrs(nil)) != 1 {
		t.Fatal("nil pool and nil config must be safe")
	}
}

func TestIntegration_PoolDurationsAndStats(t *testing.T) {
	requireDSN(t)
	ctx := context.Background()
	r := newRig(t)
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.Tracer = r.tracer
	cfg.MaxConns = 2
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	reg, err := metricspgx.RegisterPoolStats(r.mp.Meter("m"), pool)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reg.Unregister() }()

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

	ms := r.metrics(t)
	var sawSelect, sawError, sawBatch bool
	for _, dp := range ms["db.client.operation.duration"].Data.(metricdata.Histogram[float64]).DataPoints {
		op, _ := dp.Attributes.Value("db.operation.name")
		ns, _ := dp.Attributes.Value("db.namespace")
		et, hasErr := dp.Attributes.Value("error.type")
		if ns.AsString() != "t" {
			t.Fatalf("db.namespace must be on every sample: %v", dp.Attributes)
		}
		switch {
		case op.AsString() == "SELECT" && !hasErr:
			sawSelect = true
		case hasErr && et.AsString() == "42P01":
			sawError = true
		case op.AsString() == "BATCH":
			sawBatch = true
		}
	}
	if !sawSelect || !sawError || !sawBatch {
		t.Fatalf("operation duration must be split by operation and error.type: select=%v error=%v batch=%v", sawSelect, sawError, sawBatch)
	}
	for _, name := range []string{"db.client.connection.wait_time", "db.client.connection.create_time"} {
		h, ok := ms[name].Data.(metricdata.Histogram[float64])
		if !ok || len(h.DataPoints) == 0 || h.DataPoints[0].Count == 0 {
			t.Fatalf("%s missing or empty", name)
		}
	}
	wait := ms["db.client.connection.wait_time"].Data.(metricdata.Histogram[float64]).DataPoints[0]
	if name, _ := wait.Attributes.Value(attribute.Key("db.client.connection.pool.name")); !strings.HasPrefix(name.AsString(), "t@") {
		t.Fatalf("pool name = %q", name.AsString())
	}
	if maxConns := ms["db.client.connection.max"].Data.(metricdata.Gauge[int64]).DataPoints[0].Value; maxConns != 2 {
		t.Fatalf("pool max = %d", maxConns)
	}
	if name := metricspgx.PoolName(pool); !strings.HasPrefix(name, "t@") {
		t.Fatalf("pool name = %q", name)
	}
	if _, err := metricspgx.RegisterPoolStats(nil, nil); err == nil {
		t.Fatal("nil pool must fail")
	}
}

func TestIntegration_DatabaseSQLThroughStdlib(t *testing.T) {
	requireDSN(t)
	ctx := context.Background()
	r := newRig(t)
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Tracer = r.tracer
	db := stdlib.OpenDB(*cfg)
	defer func() { _ = db.Close() }()
	var n int
	if err := db.QueryRowContext(ctx, "select 42").Scan(&n); err != nil || n != 42 {
		t.Fatal(err, n)
	}
	var found bool
	for _, dp := range r.metrics(t)["db.client.operation.duration"].Data.(metricdata.Histogram[float64]).DataPoints {
		if op, _ := dp.Attributes.Value("db.operation.name"); op.AsString() == "SELECT" && dp.Count == 1 {
			found = true
		}
	}
	if !found {
		t.Fatal("a database/sql query must be one SELECT sample")
	}
}
