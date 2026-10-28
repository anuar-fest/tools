# examples — one `main` per package

One module (`github.com/anuar-fest/tools/examples`), one directory per
package it shows. Each `main` is the smallest correct wiring of that
package: flags for the infrastructure it needs, a clear exit if it is
missing, and comments where the order matters. They are compiled, vetted
and linted in CI as the proof that the modules fit together.

```sh
go run ./logx -log-format text          # no infrastructure needed
go run ./metrics/pgx -db-dsn postgres://user:pass@localhost:5432/app
```

| Directory | Shows | Needs |
|-----------|-------|-------|
| `logx` | validated level, redaction, `Secret`, context hooks | — |
| `logx/zerolog` | the zerolog bridge for the migration | — |
| `health` | liveness vs readiness, startup gate, drain on shutdown | — |
| `lifecycle` | ordered start and stop, failing component, honest exit code | — |
| `httpserver` | the middleware chain, problem+json, pagination guard, API key | — |
| `resilience` | `httpx` transport with bulkhead and breaker; a `stack` preset | — |
| `resilience/otel` | decisions, breaker state and saturation on `/metrics` | — |
| `events` | envelope, codec, middleware chain, inbox, ack semantics | — |
| `events/natsjs` | stream limits, dedup publisher, bounded consumer with DLQ | NATS with JetStream |
| `events/pg` | transactional outbox, relay, Postgres inbox | Postgres |
| `events/otel` | producer and consumer spans joined through the envelope, message metrics | — |
| `otelx` | the tracing SDK install, `LogAttrs` | — (`-otlp-endpoint` optional) |
| `otelx/server/http` | route-named server spans, `X-Trace-Id` | — |
| `otelx/server/grpc` | tracing interceptors, keepalive defaults, `grpc.health.v1` | — |
| `otelx/client/http` | traced outbound transport over `httpx` | — |
| `otelx/client/grpc` | client span per RPC, dial options with keepalive | — |
| `otelx/client/pgx` | `OpenDB` with a span per query | Postgres |
| `otelx/client/redis` | a span per command, pipeline and dial | Redis |
| `otelx/client/nats` | producer and consumer spans joined across the broker | NATS |
| `alarms` | events and states on `/metrics`, generated Prometheus rules (`-rules`); readiness and breaker hooks | — |
| `metrics` | the meter provider, `Guard`, business metrics through the `Builder` | — |
| `metrics/http` | `http.server.*` observer and the client transport | — |
| `metrics/grpc` | `rpc.server.*` and `rpc.client.call.duration` interceptors | — |
| `metrics/pgx` | operation durations, acquire waits, pool saturation | Postgres |
| `metrics/sql` | `database/sql` pool saturation | Postgres |
| `metrics/redis` | command durations, connect time, pool saturation | Redis |
| `metrics/nats` | messages, handler durations, connection stats | NATS |
| `metrics/runtime` | Go runtime instruments for OTLP-only setups | — |

Every long-running example serves `/metrics` on `-admin-addr` (default
`:9090`) and stops cleanly on SIGTERM through `lifecycle`.

## Pairing traces and metrics
The two signals are separate module families (ADR 0011) and compose:

| System | Wiring |
|--------|--------|
| HTTP server | `httpserver.Metrics(metricshttp observer)` outside `serverhttp.Tracing` |
| HTTP client | `clienthttp.Transport(metricshttp.Transport(tr, …), …)` |
| gRPC server | `ServerConfig{Interceptors: tracing, Options: ms.ServerOptions()}` |
| gRPC client | `append(tc.DialOptions(), mc.DialOptions()...)` |
| pgx | `clientpgx.OpenDB(dsn, multitracer.New(tr, mt))` |
| Redis | `client.AddHook(tracingHook); client.AddHook(metricsHook)` |
| NATS | `tr.MsgHandler(mt.Handler(h))`; `mt.Publish(ctx, subj, func(ctx) error { return tr.PublishMsg(ctx, nc, msg) })` |
