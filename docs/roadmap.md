# Roadmap

The order is by what removes the most risk from the consuming services
soonest. Each phase ships tagged modules with READMEs and proof tests before
the next starts. "Done" means merged, tagged, and used by at least one service.

## Phase 1 — resiliency and events (done)

| Module | Status |
|--------|--------|
| `resilience` (retry, breaker, bulkhead, deadline, ratelimit, shed, workpool, hedge, coalesce, stack, httpx) | done |
| `resilience/otel` | done |
| `events` core | done |
| `events/natsjs` | done |
| `events/pg` outbox + inbox | done |
| `events/otel` | done |
| `templates/proto` hermetic codegen | done |
| ADRs 0001–0007, pattern template, Shaffra review and migration roadmap | done |

## Phase 2 — the service edge (mostly done)

Goal: a service's `main.go` is wiring only; config, logging, HTTP server,
health, shutdown and telemetry come from here. ADRs 0008 and 0009 record
the design; `examples/` holds one compiled `main` per package as the proof
that the modules compose.

| Module | Contents | Replaces in Shaffra | Status |
|--------|----------|---------------------|--------|
| `otelx` | tracing SDK bootstrap: resource, W3C propagators, OTLP/HTTP trace exporter with parent-based sampling, `LogAttrs` (ADR 0010) | no tracing | done |
| `metrics`, `metrics/{http,grpc,pgx,sql,redis,nats,runtime}` | meter provider with Prometheus `/metrics` (Go/process collectors) or OTLP push, shared buckets, cardinality `Guard`, business-metric `Builder`; one metrics hook per integration (ADR 0011) | six `observability/observability.go` copies, promauto business metrics | done |
| `otelx/server/http`, `otelx/server/grpc` | when you own the server: route-named HTTP spans; gRPC tracing interceptors, keepalive and shutdown defaults, `grpc.health.v1` from `health` | chi servers with no spans; future `grpcx` | done |
| `otelx/client/{http,grpc,pgx,redis,nats}` | when you call out: one tracing hook per integration, paired with its `metrics/<system>` hook; `clientpgx.OpenDB` replaces `sql.Open("pgx")` | 30 bare `http.Client` sites, `sql.Open("pgx", dsn)`, robot's Redis, `shaffra_events` raw NATS | done |
| `alarms` | application alarms as Prometheus alerts: `alarms_total` events, `alarms_active` states, log line, span event, generated alerting rules; hooks for breaker open, dead letter, readiness lost (ADR 0012) | log lines nobody greps, hand-written rules | done |
| `logx` | `slog` setup with validated level, key-based redaction, `Secret`, `ContextHandler` hooks | six `utility/logger.go` copies with the "level ignored outside dev" bug | done |
| `logx/zerolog` | `slog.Handler` over the existing zerolog logger | two loggers during migration | done (zap: use upstream `zapslog`) |
| `httpserver` | `http.Server` with timeouts and bounded shutdown; request id, recover, access log, metrics port, trusted-proxy real IP, CORS, constant-time API key, body cap, timeout, problem+json, pagination guard; `httpx.Shed`/`RateLimit` from resilience | six `runServer.go` and `middleware/` copies | done (JWT stays in the service) |
| `health` | liveness vs readiness registry, `/healthz/live`, `/healthz/ready`, startup gate, drain flag, cached concurrent checks | `/health` that never checks the DB | done |
| `lifecycle` | signal → unready → pre-stop delay → stop in reverse order (HTTP, consumers, relays, DB, telemetry) under one budget → exit code | per-service `signal.Notify` with exit code 0 on failure | done |
| `config` | typed struct loading from env with defaults and validation, `logx.Secret` for credentials, no Viper; optional file overlay | six 400–900-line `utility/config.go` copies | next |
| `fxmod` (nested) | `fx.Module`s for everything above for services that adopt Uber Fx | lazy `services.GetX()` singletons | later |

## Phase 3 — data and RPC

| Module | Contents |
|--------|----------|
| `pgxx` | pool presets (size from `max_connections × 0.7 / pods`, lifetimes, PgBouncer transaction-mode profile with simple protocol), `TxManager`, statement and lock timeouts via `SET LOCAL`, serialization-failure retry, migrations runner with `lock_timeout`, OTel + metrics tracer, keyset pagination helpers |
| `httpclient` | per-dependency client built on `httpx.NewTransport` with presets; OpenAPI codegen template (oapi-codegen) so clients stop being hand-written |
| `grpcx` | server and client interceptors (otel, metrics, recovery, deadline propagation, auth), keepalive presets, buf-based template with `protoc-gen-go-grpc` pinned |
| `testkit` | testcontainers helpers (Postgres, NATS, Redis), embedded NATS, load harness with percentile sampling, `goleak` TestMain helper, golden fixtures |

## Phase 4 — more adapters and platform jobs

| Module | Contents |
|--------|----------|
| `events/kafka` | franz-go adapter with manual commit, cooperative rebalancing, pause-based backpressure, the same envelope headers and DLQ |
| `redisx` | client bootstrap with otel, cache with soft TTL + `coalesce`, distributed sliding-window limiter with fail-to-local (`ratelimit/redis`), lease with fencing token |
| `jobs` | Postgres-backed scheduler with leader lease (one replica runs a cron), task worker with heartbeat-extended claims (the `support` task worker, generalised) |
| `resilience/adaptive` | AIMD / gradient concurrency limit for dependencies whose capacity moves |

## Phase 5 — service template and migrations

- `cmd/newservice` generator: a service skeleton wired to the modules above, CI, Dockerfile, Helm values with probes and `preStop`.
- Migrate Shaffra services in the order given in [shaffra/roadmap.md](shaffra/roadmap.md).
- Add `depguard` rules in service repos: no `http.Client{}`, no `signal.Notify` outside `lifecycle`, no `promauto` outside `metrics`.

## Non-goals

- A web framework. `chi` or stdlib routers stay in the services.
- An ORM. `pgx` plus SQL stays.
- Exactly-once delivery. Idempotent consumers instead.
