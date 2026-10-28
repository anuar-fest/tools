// Command redis shows the go-redis metrics hook: command durations by
// operation and error type, connect time, and the pool's saturation, all
// on /metrics. It needs a Redis server.
//
//	go run ./metrics/redis -redis-addr localhost:6379
//	curl -s localhost:9090/metrics | grep db_client_
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/anuar-fest/tools/health"
	"github.com/anuar-fest/tools/httpserver"
	"github.com/anuar-fest/tools/lifecycle"
	"github.com/anuar-fest/tools/metrics"
	metricsredis "github.com/anuar-fest/tools/metrics/redis"
)

func main() {
	adminAddr := flag.String("admin-addr", ":9090", "admin listen address for /metrics and /healthz")
	addr := flag.String("redis-addr", "", "Redis address (required)")
	flag.Parse()
	if *addr == "" {
		fmt.Fprintln(os.Stderr, "-redis-addr is required")
		os.Exit(2)
	}
	os.Exit(run(context.Background(), *adminAddr, *addr))
}

func run(ctx context.Context, adminAddr, addr string) int {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	app, err := build(ctx, adminAddr, addr, logger)
	if err != nil {
		logger.Error("boot failed", "err", err)
		return 1
	}
	return lifecycle.ExitCode(app.Run(ctx))
}

func build(ctx context.Context, adminAddr, addr string, logger *slog.Logger) (*lifecycle.App, error) {
	m, err := metrics.New(ctx, metrics.Config{ServiceName: "metricsredis-example", Logger: logger})
	if err != nil {
		return nil, err
	}
	opts := &redis.Options{Addr: addr, PoolSize: 4}
	client := redis.NewClient(opts)
	hook, err := metricsredis.NewHook(metricsredis.Config{}, opts) // db.client.operation.duration, db.client.connection.create_time
	if err != nil {
		return nil, errors.Join(err, client.Close())
	}
	client.AddHook(hook)                                               // with tracing too: client.AddHook(tracingHook) first, then this one
	stats, err := metricsredis.RegisterPoolStats(nil, client, "cache") // db.client.connection.count{state}, pool.hits, pool.misses, timeouts
	if err != nil {
		return nil, errors.Join(err, client.Close())
	}
	hc := health.New(health.Config{Logger: logger})
	hc.AddReadiness("redis", func(ctx context.Context) error { return client.Ping(ctx).Err() })

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
	app.Add(lifecycle.Component{Name: "redis", Stop: func(context.Context) error { return errors.Join(stats.Unregister(), client.Close()) }})
	app.Add(lifecycle.Component{Name: "load", Run: func(ctx context.Context) error {
		t := time.NewTicker(500 * time.Millisecond)
		defer t.Stop()
		for i := 0; ; i++ {
			select {
			case <-ctx.Done():
				return nil
			case <-t.C:
				key := fmt.Sprintf("example:%d", i%10)
				_ = client.Set(ctx, key, i, time.Minute).Err()
				_ = client.Get(ctx, "example:missing").Err() // redis.Nil: a miss, not an error on the metric
				_ = client.LPush(ctx, key, "x").Err()        // WRONGTYPE: error.type=WRONGTYPE
				pipe := client.Pipeline()                    // one PIPELINE sample
				pipe.Incr(ctx, "example:counter")
				pipe.Expire(ctx, "example:counter", time.Minute)
				_, _ = pipe.Exec(ctx)
			}
		}
	}})
	return app, nil
}
