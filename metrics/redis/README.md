# metrics/redis — go-redis durations and pool saturation

## Problem
A cache call that takes 400 ms looks like slow business code, and a pool
that times out on acquire is indistinguishable from a slow server. Keys
carry tenant and user ids, so they must never become labels.

## When to use / when not to
- Every go-redis client a service owns: one `Hook` per client, one `RegisterPoolStats` per pool.
- Spans per command are `otelx/client/redis`; add both hooks when you want both signals.
- Not for redis-backed libraries that expose their own metrics (`ratelimit/redis` later): instrument once.

## API in 10 lines
```go
opts := &redis.Options{Addr: cfg.RedisAddr, DB: 0}
client := redis.NewClient(opts)
tr, _ := clientredis.NewHook(clientredis.Config{}, opts)          // spans (otelx/client/redis)
mh, _ := metricsredis.NewHook(metricsredis.Config{}, opts)        // durations; global meter from metrics.New
client.AddHook(tr)                                                // tracing first: the sample is recorded inside the span (exemplars)
client.AddHook(mh)
reg, _ := metricsredis.RegisterPoolStats(nil, client, "cache"); defer reg.Unregister()
```

## What it emits
| Instrument | Type | Attributes |
|------------|------|------------|
| `db.client.operation.duration` | histogram (s, `FastBuckets`) | `db.system.name=redis`, `server.address`, `server.port`, `db.namespace` (db index), `db.operation.name` (command or `PIPELINE`), `error.type` |
| `db.client.connection.create_time` | histogram (s, `FastBuckets`) | connection attributes, `error.type` |
| `db.client.connection.count` | gauge | `db.client.connection.pool.name`, `db.client.connection.state=idle\|used\|stale` |
| `db.client.connection.pool.hits`, `.pool.misses`, `.timeouts` | counters | `db.client.connection.pool.name` |

`redis.Nil` is a miss, not an error. `error.type` is the server's reply
prefix (`WRONGTYPE`, `NOAUTH`, `MOVED`) or the generic classification
(`ErrorType`). The command name is a closed set; keys and values never
appear.

## Defaults
| Parameter | Default | Why |
|-----------|---------|-----|
| `Meter` | global meter named `InstrumentationName` | the hook works wherever `metrics.New` ran |
| buckets | `metrics.FastBuckets` (100 µs – 10 s) | a cache round trip; the same panel as pgx |

## Failure modes
| Situation | Behaviour |
|-----------|-----------|
| dial fails | one `create_time` sample with `error.type`; the error propagates untouched |
| command fails | one `operation.duration` sample with `error.type`; the error propagates |
| `RegisterPoolStats(nil client)` | error at boot |

## Refuse list
- No keys or values as labels, with no option to enable it.
- No per-command allow-list: the command name is already a closed set.
- No pipeline per-command samples: one sample per round trip is what latency panels need; the tracing hook has one event per command.

## Proof
`metricsredis_test.go` (no server needed: the hook functions are invoked
directly): a sample per command, miss vs error, server-error prefix,
pipeline sample with and without error, dial failure sample, pool gauges
per state with the pool name, nil-client error, `ErrorType` table.

## Dashboard starting points
- `histogram_quantile(0.99, sum by (le, db_operation_name) (rate(db_client_operation_duration_seconds_bucket{db_system_name="redis"}[5m])))`
- `rate(db_client_connection_timeouts_total[5m])` — acquires that waited past the pool timeout
- `db_client_connection_count{db_client_connection_state="used"}` against `PoolSize`
