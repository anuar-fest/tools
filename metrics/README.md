# metrics — one meter provider per process, one hook per integration

```
metrics/            install the SDK once: resource, Prometheus /metrics (Go and process collectors) or OTLP push, Guard, Builder, buckets, ErrorType
metrics/http        you own an HTTP server (httpserver.Observer) or call one (client RoundTripper): http.server.*, http.client.request.duration
metrics/grpc        you own a gRPC server or call one: rpc.server.*, rpc.client.call.duration
metrics/pgx         Postgres through pgx: operation durations, acquire waits, connect time, pgxpool saturation; Operation and ErrorType vocabulary
metrics/sql         database/sql pool saturation (stdlib only)
metrics/redis       go-redis hook: command durations, connect time, pool saturation
metrics/nats        raw nats.go: messages sent and consumed, handler durations, connection stats
metrics/runtime     Go runtime instruments for OTLP-only setups
```

Every hook records through the OpenTelemetry API (ADR 0002) on the shared
bucket sets, so one dashboard fits every integration. Traces are the
`otelx` module's job; the two are wired independently and share the
resource (`Config.Resource`). Each directory with an SDK dependency is its
own module, so a service that needs the Redis hook does not inherit pgx.

## Problem the root package solves
Six `observability.go` copies with promauto globals, one of them with a
user id as a label (80 000 series), runtime metrics only because
`promhttp.Handler()` happens to include them, and every hook picking its
own buckets so p99 panels could not be compared.

## API in 10 lines
```go
m, err := metrics.New(ctx, metrics.Config{
    Resource:   tel.Resource(),                                         // same identity as the spans; or ServiceName etc.
    Prometheus: metrics.PrometheusConfig{Registry: legacyRegistry},     // keep promauto metrics on the same /metrics
    Logger:     logger,
})
admin.Handle("GET /metrics", m.Handler())
app.Add(lifecycle.Component{Name: "metrics", Stop: m.Shutdown})         // added first, stops last
b, _ := metrics.NewBuilder(metrics.BuilderConfig{Namespace: "customers", Logger: logger})
usage, _ := b.Counter(metrics.Definition{Name: "usage_events", Help: "Usage events by kind and outcome.", Unit: "{event}", Labels: []string{"event", "status"}})
usage.Inc(ctx, "stt", "applied")                                        // customers_usage_events_total{event="stt",status="applied"}
```
Then one hook per integration: `metricshttp.NewObserver`,
`metricshttp.Transport`, `metricsgrpc.NewServer`, `metricspgx.New`,
`metricssql.RegisterStats`, `metricsredis.NewHook`, `metricsnats.New`. See
each directory's README for the pairing with its tracing hook in `otelx`.

## What is here
| Piece | Use |
|-------|-----|
| `New` | the meter provider: Prometheus pull (default) and/or OTLP push, `Guard` installed, optional runtime instruments, global provider set |
| `NewPrometheus` | the pull exporter alone: registry (yours or new), Go and process collectors, handler with OpenMetrics (exemplars) |
| `LatencyBuckets`, `FastBuckets`, `SizeBuckets` | the three bucket sets every hook uses |
| `Guard` | one View that drops `HighCardinalityKeys` from every instrument and applies per-instrument allow lists |
| `NewBuilder` → `Counter`, `Histogram`, `Gauge` | business metrics with validated names, closed label sets, a per-instrument cardinality cap (`_other`, counted in `metrics.cardinality_limited`) and arity protection (`_invalid`) |
| `ErrorType` | low-cardinality `error.type` for errors without a richer vocabulary; the hooks with one (SQLSTATE, Redis reply, gRPC code) use theirs first |

## Defaults
| Parameter | Default | Why |
|-----------|---------|-----|
| exporter | Prometheus pull on `Handler` | what the fleet scrapes today |
| Go/process collectors | on | existing `go_goroutines` dashboards keep working |
| `Guard` | on (`NoGuard` to opt out) | ids, paths, query text never become series |
| `Runtime` | off | the collectors cover it; turn on for OTLP-only |
| `OTLP.Interval` | 60 s | the collector's default scrape cadence |
| `Definition.MaxCardinality` | 1000 | a business metric with more series is a bug, not a dashboard |
| `Definition.Buckets` | `LatencyBuckets` | request-scale by default; pass `FastBuckets` for in-datacenter work |
| globals | set | a hook that was not handed a meter still works |
| error handler | untouched unless `Logger` set | the tracing side already installed one |

## Failure modes
| Situation | Behaviour |
|-----------|-----------|
| collector down (OTLP) | exports fail, SDK errors to the error handler, instruments keep working |
| neither `ServiceName` nor `Resource` | `New` fails |
| `Disabled` | no readers; instruments are no-ops; `Handler` is 404 |
| name with `_total`/`_seconds`, uppercase, reserved label, missing help | `Builder` returns an error at boot |
| wrong number of label values at a call site | recorded under `_invalid`, logged once per instrument |
| more combinations than the cap | recorded under `_other`, `metrics.cardinality_limited{metric}` grows, logged once |
| collector already registered on a shared registry | ignored |

## Refuse list
- No reading of `OTEL_*` environment variables: configuration is explicit and typed.
- No `Must*` constructors: a bad definition fails `main`, not a request.
- No summaries: histograms aggregate across pods, summaries do not.
- No OTLP/gRPC exporter in the root module: pass a reader in `Readers`.

## Proof
`metrics_test.go`: resource attributes, the Guard on by default, runtime
instruments, `/metrics` with Go collectors and `target_info`, a shared
legacy registry, disabled mode, validation, idempotent shutdown, the global
set. `toolbox_test.go`: exporter, `Guard` drops `user.id` and keeps
`http.route` and explicit buckets, builder validation table, `_other` and
`_invalid` series, overflow counter and one-time logs, `ErrorType` table.
Each sub-package has its own proof section.
