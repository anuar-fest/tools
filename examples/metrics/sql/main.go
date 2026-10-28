// Command sql shows database/sql pool saturation on /metrics: in-use and
// idle connections, waits and wait time, closes by reason. The pool, not
// the database, is usually the bottleneck a slow endpoint hides. It needs
// a Postgres database.
//
//	go run ./metrics/sql -db-dsn postgres://user:pass@localhost:5432/app
//	curl -s localhost:9090/metrics | grep db_client_connection
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" driver for database/sql

	"github.com/anuar-fest/tools/health"
	"github.com/anuar-fest/tools/httpserver"
	"github.com/anuar-fest/tools/lifecycle"
	"github.com/anuar-fest/tools/metrics"
	metricssql "github.com/anuar-fest/tools/metrics/sql"
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
	m, err := metrics.New(ctx, metrics.Config{ServiceName: "sql-example", Logger: logger})
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("pgx", dsn) // clientpgx.OpenDB in a service that also wants spans; this package does not care
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(4) // small on purpose: the demo below makes the pool wait
	db.SetMaxIdleConns(4)
	db.SetConnMaxLifetime(30 * time.Minute)
	stats, err := metricssql.RegisterStats(nil, db, "main") // db.client.connection.* with pool.name="main"
	if err != nil {
		return nil, errors.Join(err, db.Close())
	}
	hc := health.New(health.Config{Logger: logger})
	hc.AddReadiness("postgres", db.PingContext)

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
		return errors.Join(stats.Unregister(), db.Close()) // unregister before Close: the callback must not read a closed pool
	}})
	app.Add(lifecycle.Component{Name: "load", Run: func(ctx context.Context) error {
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-t.C: // eight concurrent sleeps on a pool of four: waits and wait time grow
				var wg sync.WaitGroup
				for range 8 {
					wg.Add(1)
					go func() {
						defer wg.Done()
						if _, err := db.ExecContext(ctx, "SELECT pg_sleep(0.2)"); err != nil && ctx.Err() == nil {
							logger.Warn("query failed", "err", err)
						}
					}()
				}
				wg.Wait()
			}
		}
	}})
	return app, nil
}
