# otelx — OpenTelemetry tracing for the fleet: one SDK install, one hook per integration

```
otelx/                 install the tracing SDK once: resource, OTLP/HTTP exporter, parent-based sampling, W3C propagators, LogAttrs
otelx/server/http      when you own an HTTP server (chi, net/http): route-named server spans, X-Trace-Id
otelx/server/grpc      when you own a gRPC server: tracing interceptors with recovery, keepalive/size/shutdown defaults, grpc.health.v1 from health.Registry
otelx/client/http      outbound HTTP: traced transport with context injection
otelx/client/grpc      outbound gRPC: tracing interceptors, dial options with keepalive
otelx/client/pgx       Postgres via pgx or database/sql: a span per query, batch and connect; OpenDB and NewPool
otelx/client/redis     go-redis hook: a span per command, pipeline and dial
otelx/client/nats      raw nats.go publish/subscribe: producer and consumer spans joined across the broker
```

Metrics live in the `metrics` module and alarms in `alarms`: `metrics.New` for the meter provider
and `/metrics`, `metrics/<system>` for the hook that pairs with each of the
tracing hooks above. The two are wired independently; hand
`tel.Resource()` to `metrics.Config.Resource` so spans and series carry the
same `service.name`. Each directory with SDK dependencies is its own
module, so a service that needs the Redis hook does not inherit gRPC.

## Problem the root package solves
No tracing anywhere in the fleet: a span started in a NATS consumer has
nowhere to go, a log line and the request it belongs to cannot be joined,
and an outbound call that took 4 s is invisible in the caller's story.

## API in 10 lines
```go
tel, err := otelx.New(ctx, otelx.Config{
    ServiceName: "customers-api", ServiceVersion: version, Environment: env,
    Traces: otelx.TracesConfig{Endpoint: cfg.OTLPEndpoint, Insecure: true, SampleRatio: 0.2}, // "" = never sampled, no cost
    Logger: logger,
})
m, err := metrics.New(ctx, metrics.Config{Resource: tel.Resource(), Logger: logger})
app.Add(lifecycle.Component{Name: "telemetry", Stop: tel.Shutdown})      // added first, stops last
logger, _ := logx.New(logx.Config{ContextAttrs: []logx.ContextAttrs{httpserver.LogAttrs, otelx.LogAttrs}})
```
Then one hook per integration: `serverhttp.Tracing`, `clienthttp.Transport`,
`clientpgx.OpenDB`, `clientredis.NewHook`, `clientnats.MsgHandler`.
See each directory's README, including how it pairs with its
`metrics/<system>` hook.

## Defaults
| Parameter | Default | Why |
|-----------|---------|-----|
| trace exporter | none → `NeverSample` | a span nobody exports must cost nothing; propagation still works |
| `SampleRatio` | 1.0, parent-based | children follow the parent's decision; tune per service |
| batch | 5 s timeout, 2048 queue, 10 s export | drop before blocking a request |
| propagators | TraceContext + Baggage | W3C; the fleet has no B3 |
| globals | set | hooks that were not handed a provider still work |
| error handler | set, logs at Warn | export failures are visible, never fatal |

## Failure modes
| Situation | Behaviour |
|-----------|-----------|
| collector down | spans dropped past the queue, SDK errors to `Logger` at Warn, requests unaffected |
| `ServiceName` empty, `SampleRatio` > 1 | `New` fails |
| OTLP/gRPC collector | pass an `otlptracegrpc` exporter in `Traces.Exporter`; the root ships OTLP/HTTP only |

## Refuse list
- No reading of `OTEL_*` environment variables: configuration is explicit and typed.
- No gRPC exporter in the root module.
- No metrics: the meter provider, the exporters and the per-integration metric hooks are the `metrics` module.
- No contrib instrumentation: the hooks are written on the API (ADR 0009) so the route, the shared response recorder and the closed attribute sets are ours.

## Proof
`otelx_test.go`: resource attributes, spans reach injected processors,
never-sampled without exporter, validation, the globals set, `LogAttrs`,
idempotent shutdown. Each sub-package has its own proof section.
