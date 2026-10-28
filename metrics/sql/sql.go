// Package metricssql exports a database/sql pool's saturation as
// OpenTelemetry instruments: the stdlib side of the pool-stats family that
// metrics/pgx and metrics/redis continue with the same names.
package metricssql

import (
	"context"
	"database/sql"
	"errors"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

// InstrumentationName is the meter name.
const InstrumentationName = "github.com/anuar-fest/tools/metrics/sql"

// RegisterStats exports a database/sql pool's saturation as observable
// instruments on meter (nil: the global meter named InstrumentationName).
// metricspgx.RegisterPoolStats is the pgxpool equivalent:
//
//	db.client.connection.count{db.client.connection.state=idle|used}
//	db.client.connection.max
//	db.client.connection.waits          (cumulative waits for a connection)
//	db.client.connection.wait            (cumulative seconds spent waiting)
//	db.client.connection.closed{reason=max_idle|max_idle_time|max_lifetime}
//
// poolName becomes db.client.connection.pool.name. Unregister the result
// on shutdown. Alert when used approaches max or wait grows: that is the
// pool, not the database, being the bottleneck.
func RegisterStats(meter metric.Meter, db *sql.DB, poolName string) (metric.Registration, error) {
	if db == nil {
		return nil, errors.New("metricssql: nil *sql.DB")
	}
	if meter == nil {
		meter = otel.Meter(InstrumentationName)
	}
	count, err := meter.Int64ObservableGauge("db.client.connection.count",
		metric.WithDescription("Connections in the pool by state."), metric.WithUnit("{connection}"))
	if err != nil {
		return nil, err
	}
	maxConns, err := meter.Int64ObservableGauge("db.client.connection.max",
		metric.WithDescription("Maximum open connections allowed (0 = unlimited)."), metric.WithUnit("{connection}"))
	if err != nil {
		return nil, err
	}
	waits, err := meter.Int64ObservableCounter("db.client.connection.waits",
		metric.WithDescription("Times a caller waited for a connection."), metric.WithUnit("{wait}"))
	if err != nil {
		return nil, err
	}
	waitTime, err := meter.Float64ObservableCounter("db.client.connection.wait",
		metric.WithDescription("Time spent waiting for a connection."), metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}
	closed, err := meter.Int64ObservableCounter("db.client.connection.closed",
		metric.WithDescription("Connections closed by the pool, by reason."), metric.WithUnit("{connection}"))
	if err != nil {
		return nil, err
	}
	pool := semconv.DBClientConnectionPoolName(poolName)
	idle := metric.WithAttributes(pool, semconv.DBClientConnectionStateIdle)
	used := metric.WithAttributes(pool, semconv.DBClientConnectionStateUsed)
	only := metric.WithAttributes(pool)
	reason := func(r string) metric.MeasurementOption {
		return metric.WithAttributes(pool, attribute.String("reason", r))
	}
	return meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		s := db.Stats()
		o.ObserveInt64(count, int64(s.Idle), idle)
		o.ObserveInt64(count, int64(s.InUse), used)
		o.ObserveInt64(maxConns, int64(s.MaxOpenConnections), only)
		o.ObserveInt64(waits, s.WaitCount, only)
		o.ObserveFloat64(waitTime, s.WaitDuration.Seconds(), only)
		o.ObserveInt64(closed, s.MaxIdleClosed, reason("max_idle"))
		o.ObserveInt64(closed, s.MaxIdleTimeClosed, reason("max_idle_time"))
		o.ObserveInt64(closed, s.MaxLifetimeClosed, reason("max_lifetime"))
		return nil
	}, count, maxConns, waits, waitTime, closed)
}
