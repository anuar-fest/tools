package clientredis_test

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	clientredis "github.com/anuar-fest/tools/otelx/client/redis"
)

type rig struct {
	exporter *tracetest.InMemoryExporter
	hook     *clientredis.Hook
}

func newRig(t *testing.T, cfg clientredis.Config) *rig {
	t.Helper()
	r := &rig{exporter: tracetest.NewInMemoryExporter()}
	cfg.Tracer = sdktrace.NewTracerProvider(sdktrace.WithSyncer(r.exporter)).Tracer("t")
	var err error
	r.hook, err = clientredis.NewHook(cfg, &redis.Options{Addr: "cache.internal:6379", DB: 2})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// redisError is what go-redis returns for server replies: it implements redis.Error.
type redisError string

func (e redisError) Error() string { return string(e) }
func (redisError) RedisError()     {}

func attrValue(attrs []attribute.KeyValue, key string) string {
	for _, a := range attrs {
		if string(a.Key) == key {
			return a.Value.String()
		}
	}
	return ""
}

func TestProcessHook_SpansMissesAndErrors(t *testing.T) {
	r := newRig(t, clientredis.Config{})
	ctx := context.Background()
	get := redis.NewStringCmd(ctx, "get", "user:42")
	_ = r.hook.ProcessHook(func(context.Context, redis.Cmder) error { return redis.Nil })(ctx, get)
	set := redis.NewStatusCmd(ctx, "set", "k", "v")
	_ = r.hook.ProcessHook(func(context.Context, redis.Cmder) error { return nil })(ctx, set)
	incr := redis.NewIntCmd(ctx, "incr", "k")
	_ = r.hook.ProcessHook(func(context.Context, redis.Cmder) error {
		return redisError("WRONGTYPE Operation against a key holding the wrong kind of value")
	})(ctx, incr)
	plain := redis.NewIntCmd(ctx, "decr", "k")
	_ = r.hook.ProcessHook(func(context.Context, redis.Cmder) error { return errors.New("boom") })(ctx, plain)

	spans := r.exporter.GetSpans()
	if len(spans) != 4 || spans[0].Name != "GET" || spans[1].Name != "SET" || spans[2].Name != "INCR" || spans[3].Name != "DECR" {
		t.Fatalf("spans: %v", spans)
	}
	if attrValue(spans[0].Attributes, "db.query.text") != "GET" || attrValue(spans[0].Attributes, "server.address") != "cache.internal" || attrValue(spans[0].Attributes, "db.namespace") != "2" {
		t.Fatalf("GET attributes: %v", spans[0].Attributes)
	}
	if spans[0].Status.Code.String() != "Unset" {
		t.Fatal("redis.Nil is a miss, not an error")
	}
	if spans[2].Status.Code.String() != "Error" || attrValue(spans[2].Attributes, "error.type") != "WRONGTYPE" {
		t.Fatalf("server error must classify by its prefix: %+v", spans[2])
	}
	if attrValue(spans[3].Attributes, "error.type") != "errors.errorString" {
		t.Fatalf("plain error falls back to the type: %+v", spans[3])
	}
}

func TestProcessHook_IncludeKeysAndTruncation(t *testing.T) {
	r := newRig(t, clientredis.Config{IncludeKeys: true, MaxQueryTextBytes: 12})
	ctx := context.Background()
	cmd := redis.NewStringCmd(ctx, "get", "user:42:profile:long")
	_ = r.hook.ProcessHook(func(context.Context, redis.Cmder) error { return nil })(ctx, cmd)
	if got := attrValue(r.exporter.GetSpans()[0].Attributes, "db.query.text"); got != "GET user:42:…" {
		t.Fatalf("query text = %q", got)
	}
}

func TestPipelineAndDialHooks(t *testing.T) {
	r := newRig(t, clientredis.Config{})
	ctx := context.Background()
	cmds := []redis.Cmder{redis.NewStringCmd(ctx, "get", "a"), redis.NewStatusCmd(ctx, "set", "b", "1")}
	_ = r.hook.ProcessPipelineHook(func(_ context.Context, cs []redis.Cmder) error {
		cs[0].SetErr(redisError("MOVED 3999 127.0.0.1:6381"))
		return nil
	})(ctx, cmds)
	_, err := r.hook.DialHook(func(context.Context, string, string) (net.Conn, error) {
		return nil, errors.New("dial tcp: connection refused")
	})(ctx, "tcp", "x:1")
	if err == nil {
		t.Fatal("dial error must propagate")
	}
	spans := r.exporter.GetSpans()
	pipe, dial := spans[0], spans[1]
	if pipe.Name != "PIPELINE" || attrValue(pipe.Attributes, "db.operation.batch.size") != "2" || len(pipe.Events) != 2 || attrValue(pipe.Events[0].Attributes, "error.type") != "MOVED" {
		t.Fatalf("pipeline span: %+v", pipe)
	}
	if dial.Name != "CONNECT" || dial.Status.Code.String() != "Error" || attrValue(dial.Attributes, "error.type") != "errors.errorString" {
		t.Fatalf("dial span: %+v", dial)
	}
}
