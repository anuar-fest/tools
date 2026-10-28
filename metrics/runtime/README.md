# metrics/runtime — Go runtime instruments on the OpenTelemetry API

## Problem
A service that pushes OTLP and serves no Prometheus endpoint loses the
`go_goroutines` and `go_memstats_*` series the client_golang collectors
gave it for free; a goroutine leak then shows up as an OOM kill.

## API in 10 lines
```go
m, _ := metrics.New(ctx, metrics.Config{ServiceName: "svc", DisablePrometheus: true, OTLP: metrics.OTLPConfig{Endpoint: collector}, Runtime: true})
// or by hand, on any meter:
reg, err := metricsruntime.Register(nil)
defer reg.Unregister()
```

## What it emits
| Instrument | Type | Source |
|------------|------|--------|
| `go.goroutine.count` | gauge | `/sched/goroutines:goroutines` |
| `go.memory.used` | gauge (By) | `/memory/classes/heap/objects:bytes` |
| `go.memory.allocated` | counter (By) | `/gc/heap/allocs:bytes` |
| `go.memory.allocations` | counter | `/gc/heap/allocs:objects` |
| `go.memory.gc.goal` | gauge (By) | `/gc/heap/goal:bytes` |
| `go.processor.limit` | gauge | `runtime.GOMAXPROCS` |

Names follow the OpenTelemetry Go runtime conventions, not the Prometheus
`go_*` ones: a dashboard that keeps both is a migration, not a mistake.

## Refuse list
- No `runtime.ReadMemStats`: it stops the world; `runtime/metrics` does not.
- No GC tuning: `GOGC`/`GOMEMLIMIT` are deployment policy.

## Proof
`runtime_test.go`: every instrument present with a live goroutine count,
unregister stops reporting.
