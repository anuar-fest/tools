package metricsredis

import (
	"context"
	"errors"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

// PoolStatser is what every go-redis client exposes.
type PoolStatser interface {
	PoolStats() *redis.PoolStats
}

// RegisterPoolStats exports the connection pool of client on meter (nil:
// the global meter), with the names the other metrics database hooks use:
//
//	db.client.connection.count{db.client.connection.state=idle|used|stale}
//	db.client.connection.pool.hits      acquires served from the idle list
//	db.client.connection.pool.misses    acquires that had to dial
//	db.client.connection.timeouts       acquires that timed out
//
// name becomes db.client.connection.pool.name. Unregister on shutdown.
func RegisterPoolStats(meter metric.Meter, client PoolStatser, name string) (metric.Registration, error) {
	if client == nil {
		return nil, errors.New("metricsredis: nil client")
	}
	if meter == nil {
		meter = otel.Meter(InstrumentationName)
	}
	count, err := meter.Int64ObservableGauge("db.client.connection.count", metric.WithDescription("Connections in the pool by state."), metric.WithUnit("{connection}"))
	if err != nil {
		return nil, err
	}
	hits, err := meter.Int64ObservableCounter("db.client.connection.pool.hits", metric.WithDescription("Acquires served by an idle connection."), metric.WithUnit("{acquire}"))
	if err != nil {
		return nil, err
	}
	misses, err := meter.Int64ObservableCounter("db.client.connection.pool.misses", metric.WithDescription("Acquires that opened a connection."), metric.WithUnit("{acquire}"))
	if err != nil {
		return nil, err
	}
	timeouts, err := meter.Int64ObservableCounter("db.client.connection.timeouts", metric.WithDescription("Acquires that timed out waiting."), metric.WithUnit("{acquire}"))
	if err != nil {
		return nil, err
	}
	pool := semconv.DBClientConnectionPoolName(name)
	idle := metric.WithAttributes(pool, semconv.DBClientConnectionStateIdle)
	used := metric.WithAttributes(pool, semconv.DBClientConnectionStateUsed)
	stale := metric.WithAttributes(pool, attribute.String("db.client.connection.state", "stale"))
	only := metric.WithAttributes(pool)
	return meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		s := client.PoolStats()
		if s == nil {
			return nil
		}
		o.ObserveInt64(count, int64(s.IdleConns), idle)
		o.ObserveInt64(count, int64(s.TotalConns)-int64(s.IdleConns), used)
		o.ObserveInt64(count, int64(s.StaleConns), stale)
		o.ObserveInt64(hits, int64(s.Hits), only)
		o.ObserveInt64(misses, int64(s.Misses), only)
		o.ObserveInt64(timeouts, int64(s.Timeouts), only)
		return nil
	}, count, hits, misses, timeouts)
}
