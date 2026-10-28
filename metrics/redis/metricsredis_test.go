package metricsredis_test

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	metricsredis "github.com/anuar-fest/tools/metrics/redis"
)

type rig struct {
	reader *sdkmetric.ManualReader
	hook   *metricsredis.Hook
}

func newRig(t *testing.T) *rig {
	t.Helper()
	r := &rig{reader: sdkmetric.NewManualReader()}
	var err error
	r.hook, err = metricsredis.NewHook(metricsredis.Config{Meter: sdkmetric.NewMeterProvider(sdkmetric.WithReader(r.reader)).Meter("m")}, &redis.Options{Addr: "cache.internal:6379", DB: 2})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// sample is one histogram data point keyed by op/error.type.
type sample struct {
	count uint64
	attrs attribute.Set
}

func (r *rig) histogram(t *testing.T, name string) map[string]sample {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := r.reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	out := map[string]sample{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			for _, dp := range m.Data.(metricdata.Histogram[float64]).DataPoints {
				op, _ := dp.Attributes.Value("db.operation.name")
				et, _ := dp.Attributes.Value("error.type")
				out[op.AsString()+"/"+et.AsString()] = sample{count: dp.Count, attrs: dp.Attributes}
			}
		}
	}
	return out
}

// redisError is what go-redis returns for server replies: it implements redis.Error.
type redisError string

func (e redisError) Error() string { return string(e) }
func (redisError) RedisError()     {}

func TestProcessHook_DurationPerCommandMissesAndErrors(t *testing.T) {
	r := newRig(t)
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

	ops := r.histogram(t, "db.client.operation.duration")
	if ops["GET/"].count != 1 || ops["SET/"].count != 1 || ops["INCR/WRONGTYPE"].count != 1 || ops["DECR/errors.errorString"].count != 1 {
		t.Fatalf("ops = %v", ops)
	}
	attrs := ops["GET/"].attrs
	if v, _ := attrs.Value("server.address"); v.AsString() != "cache.internal" {
		t.Fatalf("connection attributes: %v", attrs)
	}
	if v, _ := attrs.Value("db.namespace"); v.AsString() != "2" {
		t.Fatalf("db index: %v", attrs)
	}
	if _, hasText := attrs.Value("db.query.text"); hasText {
		t.Fatal("query text must not be a metric attribute")
	}
	if n := attrs.Len(); n != 5 { // db.system.name, server.address, server.port, db.namespace, db.operation.name
		t.Fatalf("attribute set must stay closed, got %d: %v", n, attrs)
	}
}

func TestPipelineAndDialHooks(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	cmds := []redis.Cmder{redis.NewStringCmd(ctx, "get", "a"), redis.NewStatusCmd(ctx, "set", "b", "1")}
	_ = r.hook.ProcessPipelineHook(func(context.Context, []redis.Cmder) error { return nil })(ctx, cmds)
	_ = r.hook.ProcessPipelineHook(func(context.Context, []redis.Cmder) error {
		return redisError("MOVED 3999 127.0.0.1:6381")
	})(ctx, cmds)
	if _, err := r.hook.DialHook(func(context.Context, string, string) (net.Conn, error) {
		return nil, errors.New("dial tcp: connection refused")
	})(ctx, "tcp", "x:1"); err == nil {
		t.Fatal("dial error must propagate")
	}
	ops := r.histogram(t, "db.client.operation.duration")
	if ops["PIPELINE/"].count != 1 || ops["PIPELINE/MOVED"].count != 1 {
		t.Fatalf("pipeline samples: %v", ops)
	}
	dials := r.histogram(t, "db.client.connection.create_time")
	if len(dials) != 1 {
		t.Fatalf("dial samples: %v", dials)
	}
	for _, s := range dials {
		if et, _ := s.attrs.Value("error.type"); et.AsString() != "errors.errorString" || s.count != 1 {
			t.Fatalf("dial failure must carry error.type: %v", s.attrs)
		}
	}
}

func TestErrorType(t *testing.T) {
	cases := map[string]error{
		"NOAUTH":           redisError("NOAUTH Authentication required."),
		"redis.Error":      redisError("lowercase reply"),
		"context.Canceled": context.Canceled,
		"":                 nil,
	}
	for want, err := range cases {
		if got := metricsredis.ErrorType(err); got != want {
			t.Errorf("ErrorType(%v) = %q, want %q", err, got, want)
		}
	}
}

func TestRegisterPoolStats(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("m")
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	defer func() { _ = client.Close() }()
	reg, err := metricsredis.RegisterPoolStats(meter, client, "cache")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reg.Unregister() }()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	states := map[string]bool{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "db.client.connection.count" {
				for _, dp := range m.Data.(metricdata.Gauge[int64]).DataPoints {
					s, _ := dp.Attributes.Value("db.client.connection.state")
					pool, _ := dp.Attributes.Value("db.client.connection.pool.name")
					states[s.AsString()] = pool.AsString() == "cache"
				}
			}
		}
	}
	if !states["idle"] || !states["used"] || !states["stale"] {
		t.Fatalf("pool gauges must be exported per state with the pool name: %v", states)
	}
	if _, err := metricsredis.RegisterPoolStats(meter, nil, "x"); err == nil {
		t.Fatal("nil client must fail")
	}
}
