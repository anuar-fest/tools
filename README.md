# tools

Shared Go platform libraries for microservices: resiliency policies,
event-driven messaging, observability adapters, and (coming) HTTP, gRPC,
database and lifecycle bootstrap. Framework-free cores, SDK adapters in
nested modules, one reviewed set of defaults instead of one copy per service.

## Modules

| Module | Import path | Depends on | What it gives you |
|--------|-------------|------------|-------------------|
| [resilience](resilience/) | `github.com/anuar-fest/tools/resilience` | stdlib | retry with budget, circuit breaker, bulkhead, deadlines, rate limiting, load shedding, worker pool, hedging, request coalescing, the canonical `stack`, HTTP transport and middleware |
| [resilience/otel](resilience/otel/) | `…/resilience/otel` | OpenTelemetry API | decisions, durations, breaker state and saturation as metrics and span events |
| [events](events/) | `github.com/anuar-fest/tools/events` | stdlib | CloudEvents-shaped envelope, ports, ack semantics, middleware, inbox, codec, type naming |
| [events/natsjs](events/natsjs/) | `…/events/natsjs` | nats.go | JetStream connection, streams, dedup publisher, bounded consumer with heartbeats, backoff and DLQ |
| [events/pg](events/pg/) | `…/events/pg` | pgx | transactional outbox with `SKIP LOCKED` relay; inbox (middleware and in-transaction) |
| [events/otel](events/otel/) | `…/events/otel` | OpenTelemetry API | trace context through the envelope, producer/consumer spans, message metrics |
| [logx](logx/) | `github.com/anuar-fest/tools/logx` | stdlib | `slog` setup with a validated level, key-based redaction, `Secret`, context hooks for request and trace ids |
| [logx/zerolog](logx/zerolog/) | `…/logx/zerolog` | zerolog | `slog.Handler` over an existing zerolog logger for the migration |
| [httpserver](httpserver/) | `github.com/anuar-fest/tools/httpserver` | stdlib | `http.Server` with timeouts and a bounded shutdown; request id, recover, access log, metrics port, trusted-proxy real IP, CORS, constant-time API key, body cap, timeout, problem+json, pagination guard |
| [health](health/) | `github.com/anuar-fest/tools/health` | stdlib | liveness vs readiness registry, drain flag, startup gate, cached concurrent checks |
| [lifecycle](lifecycle/) | `github.com/anuar-fest/tools/lifecycle` | stdlib | components started together and stopped in reverse order under one budget; honest exit code |
| [otelx](otelx/) | `github.com/anuar-fest/tools/otelx` | OpenTelemetry SDK | the one tracing SDK install: resource, OTLP/HTTP traces, propagators, `LogAttrs`; package `client/http` (traced transport) |
| [otelx/server/http](otelx/server/http/) | `…/otelx/server/http` | OpenTelemetry API | you own an HTTP server: route-named server spans, `X-Trace-Id` |
| [otelx/server/grpc](otelx/server/grpc/) | `…/otelx/server/grpc` | grpc | you own a gRPC server: tracing interceptors with recovery, keepalive/size/shutdown defaults, `grpc.health.v1` from `health` |
| [otelx/client/grpc](otelx/client/grpc/) | `…/otelx/client/grpc` | grpc | outbound gRPC: tracing interceptors and dial options |
| [otelx/client/pgx](otelx/client/pgx/) | `…/otelx/client/pgx` | pgx | Postgres: a span per query, batch and connect; `OpenDB`, `NewPool` |
| [otelx/client/redis](otelx/client/redis/) | `…/otelx/client/redis` | go-redis | Redis: a span per command, pipeline and dial |
| [otelx/client/nats](otelx/client/nats/) | `…/otelx/client/nats` | nats.go | raw NATS: producer and consumer spans joined across the broker |
| [metrics](metrics/) | `github.com/anuar-fest/tools/metrics` | OpenTelemetry metrics SDK, Prometheus client | the one meter provider: Prometheus `/metrics` with Go/process collectors or OTLP push, cardinality `Guard`, business-metric `Builder`, shared buckets, `ErrorType`; packages `sql` (database/sql pool stats) and `runtime` (Go runtime instruments) |
| [metrics/http](metrics/http/) | `…/metrics/http` | OpenTelemetry API | `http.server.*` observer for `httpserver` and the `http.client.request.duration` transport |
| [metrics/grpc](metrics/grpc/) | `…/metrics/grpc` | grpc | `rpc.server.*` and `rpc.client.call.duration` interceptors |
| [metrics/pgx](metrics/pgx/) | `…/metrics/pgx` | pgx | operation durations, acquire waits, connect time, pgxpool saturation; the `Operation`/`ErrorType` vocabulary |
| [metrics/redis](metrics/redis/) | `…/metrics/redis` | go-redis | command durations, connect time, pool saturation |
| [metrics/nats](metrics/nats/) | `…/metrics/nats` | nats.go | messages sent and consumed, handler durations, connection stats |
| [alarms](alarms/) | `github.com/anuar-fest/tools/alarms` | OpenTelemetry API | application alarms as Prometheus alerts: `alarms_total` events and `alarms_active` states, log line, span event, generated alerting rules; hooks for breakers, dead letters, readiness |
| [examples](examples/) | — | all of the above | one runnable `main` per package: the smallest correct wiring of each module |
| [templates/proto](templates/proto/) | — | buf | hermetic protobuf generation for a contracts repository |

Each module is tagged on its own: `resilience/v0.1.0`, `events/natsjs/v0.1.0`.
Go directive is 1.25 everywhere; dependency versions are chosen to keep it.

## Quick start

```go
// A service main is wiring only: logger → traces → metrics → health → components → lifecycle.
logger, _ := logx.New(logx.Config{Level: level, ContextAttrs: []logx.ContextAttrs{httpserver.LogAttrs, otelx.LogAttrs}})
tel, _ := otelx.New(ctx, otelx.Config{ServiceName: "customers-api", Traces: otelx.TracesConfig{Endpoint: otlp}})
m, _ := metrics.New(ctx, metrics.Config{Resource: tel.Resource()}); admin.Handle("GET /metrics", m.Handler())
hc := health.New(health.Config{}); hc.AddReadiness("postgres", db.PingContext)
srv, _ := httpserver.New(httpserver.Config{Addr: ":8080", Handler: chain, Logger: logger})
app := lifecycle.New(lifecycle.Config{Logger: logger, OnShutdown: func(string) { hc.SetDraining(true) }})
app.Add(lifecycle.Component{Name: "telemetry", Stop: tel.Shutdown})
app.Add(lifecycle.Component{Name: "metrics", Stop: m.Shutdown})
app.Add(lifecycle.Component{Name: "http", Run: srv.Run})
os.Exit(lifecycle.ExitCode(app.Run(ctx)))

// Protect an outbound dependency with one reviewed stack.
cfg, _ := stack.Preset("fraud", stack.ReadFast, stack.PresetOptions{Observer: obs})
fraud, _ := stack.New(cfg)
score, err := stack.DoValue(ctx, fraud, func(ctx context.Context) (Score, error) { return client.Score(ctx, p) })

// Or an HTTP client that already knows about status codes, idempotent methods and body replay.
tr, _ := httpx.NewTransport(httpx.TransportConfig{Name: "fraud", Bulkhead: bh, Breaker: br, Observer: obs})
client := &http.Client{Transport: tr}

// Publish an event durably, consume it idempotently.
nc, _ := natsjs.Connect(ctx, natsjs.Config{URL: url, Name: "customers-api"})
pub := nc.NewPublisher(natsjs.PublisherConfig{Observer: obs})
_ = pub.Publish(ctx, events.New("customers.customer.created.v1", "customers-api", payload, events.WithTenant(t)))
k, _ := nc.Consume(ctx, natsjs.ConsumerConfig{Stream: "CUSTOMERS", Name: "projector", Handler: handle,
    Middlewares: []events.Middleware{events.Inbox(inbox, "projector", obs)}, DeadLetterSubject: natsjs.DeadLetterSubject("projector")})
```

## Documentation

- [docs/roadmap.md](docs/roadmap.md) — what comes next and in which order.
- [docs/adr/](docs/adr/) — the decisions behind the layout, the observability stack, the metrics module family, Prometheus-native alarms, the policy order, the codegen, the envelope.
- [docs/pattern-template.md](docs/pattern-template.md) — how every package is documented.
- [docs/shaffra/](docs/shaffra/) — review of the Shaffra services, ranked findings, and the migration roadmap.
- Every package has a README in the pattern format: problem, when (not) to use, defaults, composition, failure modes, metrics, refuse list, proof.

## Working on it

```sh
make check        # fmt, vet, build, lint, go-directive floor, test -race for every module (integration tests need Docker)
make test-short   # unit tests only
make release MODULE=resilience VERSION=v0.1.0
```

Rules for contributors live in [CLAUDE.md](CLAUDE.md) and `.claude/rules/`.
Every change that touches Go code ends with a *Decisions & trade-offs* block.
