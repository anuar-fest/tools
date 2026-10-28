package metricspgx

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

// RegisterPoolStats exports a pgxpool's saturation on meter (nil: the
// global meter named InstrumentationName), with the same instrument names
// metricssql.RegisterStats uses for database/sql so one dashboard fits both:
//
//	db.client.connection.count{db.client.connection.state=idle|used|constructing}
//	db.client.connection.max
//	db.client.connection.waits         acquires that found the pool empty
//	db.client.connection.wait          seconds spent in those waits
//	db.client.connection.acquires      all acquires
//	db.client.connection.canceled      acquires canceled by their context
//	db.client.connection.created
//	db.client.connection.closed{reason=max_lifetime|max_idle}
//
// Unregister the result on shutdown.
func RegisterPoolStats(meter metric.Meter, pool *pgxpool.Pool) (metric.Registration, error) {
	if pool == nil {
		return nil, errors.New("metricspgx: nil pool")
	}
	if meter == nil {
		meter = otel.Meter(InstrumentationName)
	}
	g := func(name, desc, unit string) (metric.Int64ObservableGauge, error) {
		return meter.Int64ObservableGauge(name, metric.WithDescription(desc), metric.WithUnit(unit))
	}
	c := func(name, desc, unit string) (metric.Int64ObservableCounter, error) {
		return meter.Int64ObservableCounter(name, metric.WithDescription(desc), metric.WithUnit(unit))
	}
	count, err := g("db.client.connection.count", "Connections in the pool by state.", "{connection}")
	if err != nil {
		return nil, err
	}
	maxConns, err := g("db.client.connection.max", "Maximum connections the pool opens.", "{connection}")
	if err != nil {
		return nil, err
	}
	waits, err := c("db.client.connection.waits", "Acquires that found no idle connection.", "{wait}")
	if err != nil {
		return nil, err
	}
	waitTime, err := meter.Float64ObservableCounter("db.client.connection.wait", metric.WithDescription("Time spent waiting for a connection."), metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}
	acquires, err := c("db.client.connection.acquires", "Acquires in total.", "{acquire}")
	if err != nil {
		return nil, err
	}
	canceled, err := c("db.client.connection.canceled", "Acquires canceled by their context.", "{acquire}")
	if err != nil {
		return nil, err
	}
	created, err := c("db.client.connection.created", "Connections opened.", "{connection}")
	if err != nil {
		return nil, err
	}
	closed, err := c("db.client.connection.closed", "Connections closed by the pool, by reason.", "{connection}")
	if err != nil {
		return nil, err
	}
	name := semconv.DBClientConnectionPoolName(PoolName(pool))
	idle := metric.WithAttributes(name, semconv.DBClientConnectionStateIdle)
	used := metric.WithAttributes(name, semconv.DBClientConnectionStateUsed)
	constructing := metric.WithAttributes(name, semconv.DBClientConnectionStateKey.String("constructing"))
	only := metric.WithAttributes(name)
	byLifetime := metric.WithAttributes(name, attribute.String("reason", "max_lifetime"))
	byIdle := metric.WithAttributes(name, attribute.String("reason", "max_idle"))
	return meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		s := pool.Stat()
		o.ObserveInt64(count, int64(s.IdleConns()), idle)
		o.ObserveInt64(count, int64(s.AcquiredConns()), used)
		o.ObserveInt64(count, int64(s.ConstructingConns()), constructing)
		o.ObserveInt64(maxConns, int64(s.MaxConns()), only)
		o.ObserveInt64(waits, s.EmptyAcquireCount(), only)
		o.ObserveFloat64(waitTime, s.EmptyAcquireWaitTime().Seconds(), only)
		o.ObserveInt64(acquires, s.AcquireCount(), only)
		o.ObserveInt64(canceled, s.CanceledAcquireCount(), only)
		o.ObserveInt64(created, s.NewConnsCount(), only)
		o.ObserveInt64(closed, s.MaxLifetimeDestroyCount(), byLifetime)
		o.ObserveInt64(closed, s.MaxIdleDestroyCount(), byIdle)
		return nil
	}, count, maxConns, waits, waitTime, acquires, canceled, created, closed)
}
