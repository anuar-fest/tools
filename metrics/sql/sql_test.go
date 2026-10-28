package metricssql_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	metricssql "github.com/anuar-fest/tools/metrics/sql"
)

type fakeConnector struct{}

func (fakeConnector) Connect(context.Context) (driver.Conn, error) { return fakeConn{}, nil }
func (fakeConnector) Driver() driver.Driver                        { return fakeDriver{} }

type fakeDriver struct{}

func (fakeDriver) Open(string) (driver.Conn, error) { return fakeConn{}, nil }

type fakeConn struct{}

func (fakeConn) Prepare(string) (driver.Stmt, error) { return nil, errors.ErrUnsupported }
func (fakeConn) Close() error                        { return nil }
func (fakeConn) Begin() (driver.Tx, error)           { return nil, errors.ErrUnsupported }

func TestRegisterStats(t *testing.T) {
	ctx := context.Background()
	db := sql.OpenDB(fakeConnector{})
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(3)
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	reg, err := metricssql.RegisterStats(mp.Meter("m"), db, "main")
	if err != nil {
		t.Fatal(err)
	}
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatal(err)
	}
	ms := map[string]metricdata.Metrics{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			ms[m.Name] = m
		}
	}
	var used int64
	for _, dp := range ms["db.client.connection.count"].Data.(metricdata.Gauge[int64]).DataPoints {
		state, _ := dp.Attributes.Value(attribute.Key("db.client.connection.state"))
		pool, _ := dp.Attributes.Value(attribute.Key("db.client.connection.pool.name"))
		if state.AsString() == "used" && pool.AsString() == "main" {
			used = dp.Value
		}
	}
	if maxConns := ms["db.client.connection.max"].Data.(metricdata.Gauge[int64]).DataPoints[0].Value; used != 1 || maxConns != 3 {
		t.Fatalf("used=%d max=%d", used, maxConns)
	}
	_ = conn.Close()
	if err := reg.Unregister(); err != nil {
		t.Fatal(err)
	}
	if _, err := metricssql.RegisterStats(nil, nil, "x"); err == nil {
		t.Fatal("nil db must fail")
	}
}
