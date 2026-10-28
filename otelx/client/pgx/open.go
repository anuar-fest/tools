package clientpgx

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

// OpenDB replaces sql.Open("pgx", connString) with an instrumented
// connection. t is this package's Tracer, or multitracer.New(tracer,
// metricsTracer) for spans and metrics together; nil installs none.
// Pool limits (SetMaxOpenConns, lifetimes) are still set by the caller;
// metricssql.RegisterStats exports them.
func OpenDB(connString string, t pgx.QueryTracer, opts ...stdlib.OptionOpenDB) (*sql.DB, error) {
	cfg, err := pgx.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("clientpgx: parse config: %w", err)
	}
	if t != nil {
		cfg.Tracer = t
	}
	return stdlib.OpenDB(*cfg, opts...), nil
}

// NewPool parses connString, installs the tracer (see OpenDB for what t
// may be) and lets configure adjust the pool (MaxConns, lifetimes) before
// it opens. metricspgx.RegisterPoolStats exports the pool.
func NewPool(ctx context.Context, connString string, t pgx.QueryTracer, configure func(*pgxpool.Config)) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("clientpgx: parse pool config: %w", err)
	}
	if t != nil {
		cfg.ConnConfig.Tracer = t
	}
	if configure != nil {
		configure(cfg)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("clientpgx: new pool: %w", err)
	}
	return pool, nil
}
