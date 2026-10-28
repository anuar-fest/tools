# metrics/pgx — latency and pool saturation for pgx and database/sql

## Problem
A slow endpoint on a Postgres-backed service has three suspects: the
query, the pool, or the connection setup. Without a duration histogram
split by operation, a wait histogram for the pool and saturation gauges,
all three look the same from the outside. The pool leak that filled
`pg_stat_activity` stayed invisible until the database refused connections.

## When to use / when not to
- Any service that talks to Postgres through pgx, directly or via `database/sql` with the pgx stdlib driver.
- Export the pool too: `metricspgx.RegisterPoolStats` for `*pgxpool.Pool`, `metricssql.RegisterStats` for `*sql.DB`; same instrument names, one dashboard.
- Spans are not here: pair with `otelx/client/pgx` through `multitracer.New` when you want both.

## API in 10 lines
```go
mt, _ := metricspgx.New(metricspgx.Config{})                        // global meter from metrics.New
cfg, _ := pgx.ParseConfig(dsn); cfg.Tracer = mt                     // database/sql, metrics only
db := stdlib.OpenDB(*cfg); db.SetMaxOpenConns(10)
reg, _ := metricssql.RegisterStats(nil, db, "main"); defer reg.Unregister()

pool, _ := clientpgx.NewPool(ctx, dsn, multitracer.New(tr, mt), nil)   // spans and metrics together
preg, _ := metricspgx.RegisterPoolStats(nil, pool); defer preg.Unregister()
```
Without the tracing hook, `pgxpool.ParseConfig` + `cfg.ConnConfig.Tracer = mt` +
`pgxpool.NewWithConfig` does the same for pgxpool.

## What it emits
| Instrument | Type | Attributes |
|------------|------|------------|
| `db.client.operation.duration` | histogram (s, `FastBuckets`) | `db.system.name`, `db.namespace`, `server.address`, `server.port`, `db.operation.name`, `error.type` |
| `db.client.connection.wait_time` | histogram (s, `FastBuckets`) | `db.client.connection.pool.name`, `error.type` |
| `db.client.connection.create_time` | histogram (s, `FastBuckets`) | connection attributes, `error.type` |
| `db.client.connection.count{state}`, `.max`, `.waits`, `.wait`, `.acquires`, `.canceled`, `.created`, `.closed{reason}` | gauges / counters | `db.client.connection.pool.name` |

A batch is one `BATCH` sample; the per-statement detail belongs to the span.

## Vocabulary shared with the tracing hook
| Function | Gives |
|----------|-------|
| `Operation(sql)` | the leading keyword from a closed set (`SELECT`, `INSERT`, `WITH`, `BEGIN`, … else `OTHER`), safe as a label |
| `ErrorType(err)` | the SQLSTATE (`42P01`), a context sentinel, `pgx.ErrNoRows`, or `metrics.ErrorType` |
| `PoolName(pool)` | `database@host:port` for `db.client.connection.pool.name` |
| `ConnAttrs(cfg)` | `db.system.name`, `db.namespace`, `server.address`, `server.port` |

`otelx/client/pgx` imports these so spans and series say the same thing.

## Defaults
| Parameter | Default | Why |
|-----------|---------|-----|
| `Meter` | global meter named `InstrumentationName` | what `metrics.New` installs |
| buckets | `metrics.FastBuckets` (100 µs – 10 s) | in-datacenter round trips; one panel for every database hook |

## Composition order
`cfg.Tracer` takes one tracer: `multitracer.New(tracing, metrics)` runs both
on every query, connect and acquire. Each hook keeps its state under its own
context key, so the order inside `multitracer` does not matter. Register
pool stats after the pool exists and unregister them before closing it.

## Failure modes
| Situation | Behaviour |
|-----------|-----------|
| `TraceXxxEnd` without a matching start (nil context state) | no-op |
| nil `*pgx.Conn` or nil config | sample carries `db.system.name` only |
| `RegisterPoolStats(nil pool)` | error at boot |
| invalid meter | `New` returns the instrument error |

## Refuse list
- No query text, no arguments: a label must be a closed set, and values would leak.
- No table-name parsing: a SQL parser for a label is not worth its bugs.
- No `PrepareTracer`/`CopyFromTracer` samples yet: add them when a service uses them.

## Proof
`metricspgx_test.go`: `Operation` and `ErrorType` tables, nil-safety; with
Docker (testcontainers Postgres): duration split by operation and
`error.type` (SQLSTATE on the failed query), batch sample, acquire and
connect histograms with the pool name, pool gauges through `pgxpool`, and
`database/sql` through the pgx stdlib driver. `-short` skips the container.
`otelx/client/pgx` proves the `multitracer` combination.

## Dashboard starting points
- `histogram_quantile(0.99, sum by (le, db_operation_name) (rate(db_client_operation_duration_seconds_bucket[5m])))`
- `db_client_connection_count{db_client_connection_state="used"} / db_client_connection_max` — pool saturation; alert at 0.8
- `histogram_quantile(0.99, sum by (le) (rate(db_client_connection_wait_time_seconds_bucket[5m])))` — when this grows, size the pool, not the query
