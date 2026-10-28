# otelx/client/redis — go-redis hook with a span per command

## Problem
A cache call that takes 400 ms looks like slow business code, and the
trace stops at the handler. Keys carry tenant and user ids, so logging them
for debugging leaks them.

## API in 10 lines
```go
opts := &redis.Options{Addr: cfg.RedisAddr, DB: 0}
client := redis.NewClient(opts)
hook, _ := clientredis.NewHook(clientredis.Config{}, opts)       // global tracer from otelx.New
client.AddHook(hook)
mh, _ := metricsredis.NewHook(metricsredis.Config{}, opts)       // durations and pool stats: metrics/redis
client.AddHook(mh)                                               // after the tracing hook, so the sample is inside the span
hc.AddReadiness("redis", func(ctx context.Context) error { return client.Ping(ctx).Err() })
```

## What it emits
| Signal | Name | Attributes |
|--------|------|------------|
| span per command | `GET`, `SET`, … | `db.system.name=redis`, `db.namespace` (db index), `server.address`, `server.port`, `db.operation.name`, `db.query.text` (command only unless `IncludeKeys`), `error.type` |
| span per pipeline | `PIPELINE` | `db.operation.batch.size`; one `db.pipeline.command` event per command |
| span per dial | `CONNECT` | connection attributes, `error.type` |

`redis.Nil` is a miss, not an error. `error.type` comes from
`metricsredis.ErrorType`: the server's reply prefix (`WRONGTYPE`, `NOAUTH`,
`MOVED`) or the generic classification. Durations and pool gauges are
`metrics/redis`.

## Defaults
| Parameter | Default | Why |
|-----------|---------|-----|
| `IncludeKeys` | off | keys carry ids; opt in per client |
| `MaxQueryTextBytes` | 256 | a long key is noise on a span |

## Refuse list
- No keys or values on spans by default; `IncludeKeys` is opt-in and truncated.
- No metrics: one hook per signal, so a service that only wants traces does not pay for a meter.

## Proof
`clientredis_test.go` (no server needed: hooks are invoked directly): span
per command, miss vs error, server-error prefix, text omission and
truncation, pipeline events, dial failure.
