// Command pgx shows Postgres metrics through pgx: operation durations by
// a closed set of operations and error types, acquire waits, connect time,
// and pgxpool saturation gauges, all on /metrics. It needs a Postgres
// database.
//
//	go run ./metrics/pgx -db-dsn postgres://user:pass@localhost:5432/app
//	curl -s localhost:9090/metrics | grep db_client_
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/anuar-fest/tools/health"
	"github.com/anuar-fest/tools/httpserver"
	"github.com/anuar-fest/tools/lifecycle"
	"github.com/anuar-fest/tools/metrics"
	metricspgx "github.com/anuar-fest/tools/metrics/pgx"
)

func main() {
	adminAddr := flag.String("admin-addr", ":9090", "admin listen address for /metrics and /healthz")
	dsn := flag.String("db-dsn", "", "Postgres DSN (required)")
	flag.Parse()
	if *dsn == "" {
		fmt.Fprintln(os.Stderr, "-db-dsn is required")
		os.Exit(2)
	}
	os.Exit(run(context.Background(), *adminAddr, *dsn))
}

func run(ctx context.Context, adminAddr, dsn string) int {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	app, err := build(ctx, adminAddr, dsn, logger)
	if err != nil {
		logger.Error("boot failed", "err", err)
		return 1
	}
	return lifecycle.ExitCode(app.Run(ctx))
}

func build(ctx context.Context, adminAddr, dsn string, logger *slog.Logger) (*lifecycle.App, error) {
	m, err := metrics.New(ctx, metrics.Config{ServiceName: "metricspgx-example", Logger: logger})
	if err != nil {
		return nil, err
	}
	mt, err := metricspgx.New(metricspgx.Config{}) // db.client.operation.duration, connection.wait_time, connection.create_time
	if err != nil {
		return nil, err
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.ConnConfig.Tracer = mt // with tracing too: multitracer.New(tracingTracer, mt), or clientpgx.NewPool
	cfg.MaxConns = 4
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	stats, err := metricspgx.RegisterPoolStats(nil, pool) // db.client.connection.count{state}, .max, .waits, .wait, ...
	if err != nil {
		pool.Close()
		return nil, err
	}
	hc := health.New(health.Config{Logger: logger})
	hc.AddReadiness("postgres", pool.Ping)

	admin := http.NewServeMux()
	admin.Handle("GET /metrics", m.Handler())
	admin.Handle("GET /healthz/ready", hc.ReadyHandler())
	adminSrv, err := httpserver.New(httpserver.Config{Addr: adminAddr, Handler: admin, Logger: logger})
	if err != nil {
		return nil, err
	}
	if err := adminSrv.Listen(ctx); err != nil {
		return nil, err
	}
	app := lifecycle.New(lifecycle.Config{Logger: logger, OnShutdown: func(string) { hc.SetDraining(true) }})
	app.Add(lifecycle.Component{Name: "metrics", Stop: m.Shutdown})
	app.Add(lifecycle.Component{Name: "admin-http", Run: adminSrv.Run})
	app.Add(lifecycle.Component{Name: "postgres", Stop: func(context.Context) error {
		err := stats.Unregister() // before Close: the callback must not read a closed pool
		pool.Close()
		return err
	}})
	app.Add(lifecycle.Component{Name: "load", Run: func(ctx context.Context) error {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for i := 0; ; i++ {
			select {
			case <-ctx.Done():
				return nil
			case <-t.C:
				if _, err := pool.Exec(ctx, "SELECT pg_sleep(0.05)"); err != nil && ctx.Err() == nil {
					logger.Warn("query failed", "err", err)
				}
				if i%5 == 0 {
					_, _ = pool.Exec(ctx, "SELECT * FROM no_such_table") // error.type=42P01 on the duration sample
				}
			}
		}
	}})
	return app, nil
}
