// Command redis shows the go-redis tracing hook: a client span per
// command, per pipeline (one event per command) and per dial, with keys
// kept off the spans unless asked for. It needs a Redis server.
//
//	go run ./otelx/client/redis -redis-addr localhost:6379
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"

	"github.com/anuar-fest/tools/otelx"
	clientredis "github.com/anuar-fest/tools/otelx/client/redis"
)

func main() {
	addr := flag.String("redis-addr", "", "Redis address (required)")
	flag.Parse()
	if *addr == "" {
		fmt.Fprintln(os.Stderr, "-redis-addr is required")
		os.Exit(2)
	}
	if err := run(context.Background(), *addr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, addr string) error {
	exporter, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		return err
	}
	tel, err := otelx.New(ctx, otelx.Config{ServiceName: "clientredis-example", Traces: otelx.TracesConfig{Exporter: exporter}})
	if err != nil {
		return err
	}
	defer func() { _ = tel.Shutdown(context.WithoutCancel(ctx)) }() // flush even if ctx ended

	opts := &redis.Options{Addr: addr}
	client := redis.NewClient(opts)
	defer func() { _ = client.Close() }()
	hook, err := clientredis.NewHook(clientredis.Config{}, opts) // IncludeKeys: false; keys often carry tenant and user ids
	if err != nil {
		return err
	}
	client.AddHook(hook) // pair with metrics/redis: client.AddHook(metricsHook) after this one

	ctx, root := tel.Tracer("example").Start(ctx, "warm cache")
	defer root.End()
	if err = client.Set(ctx, "example:greeting", "hello", time.Minute).Err(); err != nil {
		return err
	}
	v, err := client.Get(ctx, "example:greeting").Result()
	if err != nil {
		return err
	}
	fmt.Println("GET →", v)
	_, err = client.Get(ctx, "example:missing").Result()
	fmt.Println("miss is not an error on the span:", errors.Is(err, redis.Nil))
	pipe := client.Pipeline() // one PIPELINE span with a db.pipeline.command event per command
	pipe.Incr(ctx, "example:counter")
	pipe.Expire(ctx, "example:counter", time.Minute)
	_, err = pipe.Exec(ctx)
	return err
}
