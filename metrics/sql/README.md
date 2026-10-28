# metrics/sql — database/sql pool saturation

## Problem
A slow endpoint that is really a full connection pool looks like a slow
database. `sql.DB.Stats` has the answer (in-use, waits, wait time) and
nobody exports it.

## API in 10 lines
```go
db, _ := clientpgx.OpenDB(dsn, tracer)                  // or sql.Open: this package does not care how db was opened
db.SetMaxOpenConns(10); db.SetConnMaxLifetime(30 * time.Minute)
reg, err := metricssql.RegisterStats(nil, db, "main")   // nil: the global meter
app.Add(lifecycle.Component{Name: "postgres", Stop: func(context.Context) error { return errors.Join(reg.Unregister(), db.Close()) }})
```

## What it emits
| Instrument | Type | Attributes |
|------------|------|------------|
| `db.client.connection.count` | gauge | `db.client.connection.pool.name`, `db.client.connection.state=idle\|used` |
| `db.client.connection.max` | gauge | pool name (0 = unlimited) |
| `db.client.connection.waits` | counter | pool name |
| `db.client.connection.wait` | counter (s) | pool name |
| `db.client.connection.closed` | counter | pool name, `reason=max_idle\|max_idle_time\|max_lifetime` |

The names match `metricspgx.RegisterPoolStats` and
`metricsredis.RegisterPoolStats`, so one pool panel fits every client.

## Refuse list
- No polling goroutine: the SDK calls back on collection; nothing runs between scrapes.
- No pool sizing: `SetMaxOpenConns` and lifetimes stay with the caller.

## Proof
`sql_test.go`: a fake driver with one connection checked out reports
`used=1`, `max=3` under the pool name; unregister stops reporting; nil `db`
fails.

## Dashboard starting points
- `db_client_connection_count{db_client_connection_state="used"} / db_client_connection_max` — alert at 0.8
- `rate(db_client_connection_wait_seconds_total[5m])` — seconds per second spent waiting for a connection
