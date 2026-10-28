# otelx/client/pgx — tracing for pgx and database/sql

## Problem
Every Shaffra service opens Postgres with `sql.Open("pgx", dsn)` and sees
nothing: no span per query, no SQLSTATE on the failed one, no way to tell
from a slow trace whether the time went into the query or into waiting for
a pooled connection.

## When to use / when not to
- Replace `sql.Open("pgx", dsn)` with `clientpgx.OpenDB(dsn, tracer)`; nothing else changes.
- For `pgxpool`: `clientpgx.NewPool` or set `cfg.ConnConfig.Tracer`.
- Metrics are `metrics/pgx` (durations, pool gauges) and `metrics/sql` (`*sql.DB` gauges); pair them through `multitracer.New`.
- Turn `OmitQueryText` on where SQL is built from user input (the datalake query builder): literals in the text would reach the tracing backend.

## API in 10 lines
```go
tr, _ := clientpgx.New(clientpgx.Config{})                           // global tracer from otelx
mt, _ := metricspgx.New(metricspgx.Config{})                         // global meter from metrics
db, err := clientpgx.OpenDB(cfg.DSN.Reveal(), multitracer.New(tr, mt)) // database/sql: spans and metrics
db.SetMaxOpenConns(cfg.MaxOpen); db.SetConnMaxLifetime(30 * time.Minute)
reg, _ := metricssql.RegisterStats(nil, db, "main")
pool, err := clientpgx.NewPool(ctx, dsn, multitracer.New(tr, mt), func(c *pgxpool.Config) { c.MaxConns = 10 })
preg, _ := metricspgx.RegisterPoolStats(nil, pool)
```
Pass `tr` alone for spans only; `OpenDB` and `NewPool` take any
`pgx.QueryTracer`, nil installs none.

## What it emits
| Signal | Name | Attributes |
|--------|------|------------|
| span per query | `SELECT customers` (operation + database) | `db.system.name`, `db.namespace`, `server.address`, `server.port`, `db.operation.name`, `db.query.text`, `db.response.returned_rows`, `error.type` |
| span per batch | `BATCH customers` | `db.operation.batch.size`; one `db.batch.query` event per statement |
| span per connect | `CONNECT customers` | connection attributes |
| event on the active span | `db.connection.acquire` | `db.client.connection.pool.name`, `wait_ms`, `error.type`; only for waits ≥ `SlowAcquire` or failures |

`db.operation.name` and `error.type` come from `metricspgx.Operation` and
`metricspgx.ErrorType`, so spans and series agree. Query arguments are
never recorded.

## Defaults
| Parameter | Default | Why |
|-----------|---------|-----|
| `OmitQueryText` | off | pgx sends parameters separately; the text holds no values unless the service interpolated them |
| `MaxQueryTextBytes` | 1024 | a 40 KiB generated query is noise on a span |
| `SlowAcquire` | 1 ms | shorter waits are a hand-over from the idle list; longer ones are contention |

## Refuse list
- No metrics: that is `metrics/pgx`; one hook per signal, combined by `multitracer`.
- No table-name parsing for the span name: the query text is on the span.
- No `PrepareTracer`/`CopyFromTracer` spans yet: add them when a service uses them.
- No arguments on spans or logs, with no option to enable it.

## Proof
`pgxotel_test.go`: nil-connection safety, text truncation and omission,
slow acquire as a span event; with Docker (testcontainers Postgres):
connect/query/batch spans with SQLSTATE on the failed one, `multitracer`
delivering both spans and the three `metrics/pgx` histograms through
`NewPool`, and `OpenDB` tracing a `database/sql` query. `-short` skips the
container.
