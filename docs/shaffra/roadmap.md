# Shaffra — improvement roadmap

Ordered by risk removed per week of work. Every step names the module in
this repo that replaces a copy, and the proof that the step is done.

## Week 1 — stop the bleeding (no library needed)

| Change | Repos | Proof |
|--------|-------|-------|
| `os.Exit(1)` on fatal errors in `main.go`; fix the `driver, err :=` shadow so `m.Up()` failures surface | trainer, datalake, robot, customers | a failing migration fails the Job |
| Apply `MaxOpenConns`/`MaxIdleConns`/`ConnMaxLifetime` from config; `Exec` instead of `Query` for `CREATE EXTENSION` | trainer, support, datalake (+ the four with the leak) | `pg_stat_activity` stays flat under load test |
| One `pgxpool` per process for RAG/agents, closed on shutdown | trainer, robot | pgvector connection count flat |
| Remove the unauthenticated test-data delete; re-enable SNS signature verification; key the LLM manager cache by tenant | support | tests for each |
| Read-only role + `statement_timeout` + statement allow-list for LLM-generated SQL | datalake | a `DELETE` from the builder is refused |
| Fix the `newThread == nil` log after exhausted retries | trainer, support | an LLM outage returns an error, not a panic |
| `ReadHeaderTimeout: 10s`, `MaxBytesReader` on JSON bodies, `CheckOrigin` allow-list, WebSocket read limit | trainer, datalake, robot, support | slowloris test |
| Hard maximum on page size; `il=true` only for admin routes | 4 services | a request for 1e6 rows returns 400 |
| Delete the super-admin bypass token; stop printing API keys; constant-time compare | support, customers, 5 services | grep |

## Month 1 — events done right

1. **Adopt `events/natsjs`** in one consumer first (datalake: 4 subscribe
   sites), then trainer, customers, robot, support. Per subscription:
   durable name, `MaxDeliver`, `DeadLetterSubject`, `events.Inbox` where the
   handler is not naturally idempotent. Proof: kill the pod mid-handler; the
   message is redelivered once and processed once.
2. **One stream owner.** Move `EnsureStream` out of the services into a
   `streams` job in `shaffra_events` (or the producing service), with
   explicit `MaxAge`, `MaxBytes`, `Replicas: 3`, plus the `DLQ` stream.
3. **Envelope headers.** Publish with `events.New(type, source, data,
   WithTenant(...))`; keep the protobuf payload. Introduce typed names
   (`customers.customer.created.v1`) alongside the old subjects via
   `SubjectFor` until every consumer moved.
4. **Outbox** for the events that must not be lost (customer, account,
   conversation lifecycle): `eventspg.Enqueue` in the business transaction,
   one `Relay` per replica. Delete the 46 `go publish()` sites as they move.
5. **Hermetic codegen** in `shaffra_events`: `templates/proto`, `buf
   breaking` against main, `reserved` fields, protos grouped by domain,
   `.gitattributes`. Proof: a fresh clone's `make check` is green on macOS
   and Linux.

## Month 2 — one edge for all services (phase 2 modules)

| Replace | With | Order |
|---------|------|-------|
| six `config.go` | `config` typed loader (next module); until then keep the loader and wrap credentials in `logx.Secret` | storage gateway first (smallest), then customers, datalake, trainer, robot, support |
| six `logger.go` | `logx.New` (JSON, `ParseLevel` fails on a typo) or, keeping zerolog, `logx/zerolog.NewHandler`; both wrapped in `logx.ContextHandler(h, httpserver.LogAttrs, otelx.LogAttrs)` so every line carries `request_id`, `trace_id` | with config |
| six `runServer.go` + `middleware/` | `httpserver.New` (timeouts, `Listen` before ready, bounded `Shutdown`), chain `RealIP(PrivateNetworks) → RequestID → Metrics → AccessLog → Recover → serverhttp.Tracing → httpx.Shed → httpx.RateLimit → CORS → NoCache → MaxBytes → Timeout → APIKey`; the chi `Route` func for route labels; `health.New` with `AddReadiness("postgres", db.PingContext)` and `health.Bool(nc.Healthy, …)` on an admin port with `/metrics` | with config |
| per-service `signal.Notify` | `lifecycle.New(OnShutdown: hc.SetDraining)`; `Add` telemetry, db, consumers, relay, http in that order; `os.Exit(lifecycle.ExitCode(app.Run(ctx)))` | with httpserver |
| promauto in `observability.go` | `metrics.New` with `Prometheus.Registry` set to the existing registry so old and new series share `/metrics`; `otelx.New` for traces with the same resource; `resilience/otel`, `events/otel`, `metricshttp.NewObserver` observers | with httpserver |
| `sql.Open("pgx", dsn)` | `clientpgx.OpenDB(dsn, multitracer.New(tr, mt))` + `metricssql.RegisterStats`; `clientpgx.NewPool` + `metricspgx.RegisterPoolStats` for the RAG pools | with otelx |
| outbound `http.Client{}` | `httpx.NewTransport` per dependency wrapped in `clienthttp.Transport(tr, {PeerService: name})` | month 3 |
| robot's `redis.NewClient` | `client.AddHook(clientredis.NewHook(...))`, `client.AddHook(metricsredis.NewHook(...))` + `metricsredis.RegisterPoolStats`; readiness `client.Ping` | with otelx |
| `shaffra_events` publish/subscribe | `clientnats.PublishMsg` / `MsgHandler` so a message continues the request's trace, `metricsnats` nested inside for counts and durations, until the typed `events` path replaces the client | month 1–2 |
| promauto business metrics (`customers_usage_events_total`, …) | `metrics.NewBuilder(Namespace: service)` definitions with closed labels and caps; same names after export, so dashboards survive | with metrics |
| nothing | `alarms.New` with the service's `Definitions`; `ResilienceObserver`, `EventsObserver`, `OnReadinessChange: al.ReadinessChanged`; `al.Raise`/`al.Set` where the code already knows something is wrong; `alarms.Rules` output committed to the chart so Alertmanager routes on `severity` and `service` | with metrics |
| Helm: liveness only | readiness probe on `/healthz/ready`, `preStop: sleep 5`, `terminationGracePeriodSeconds` ≥ drain budget, PDB, `replicas ≥ 2` | per service |

## Month 3 — outbound calls and data (phase 3 modules)

- Every outbound HTTP dependency (LLM providers, trainer↔support↔robot
  clients, S3, Twilio, LiveKit) goes through `httpx.NewTransport` with a
  `stack.Preset` per dependency class. The three skeleton clients are
  regenerated from OpenAPI with `oapi-codegen` on top of that transport;
  the storage-gateway client already fits.
- `pgxx`: pool presets sized from `max_connections × 0.7 / pods`,
  `TxManager`, `SET LOCAL statement_timeout`/`lock_timeout`, serialization
  retry, migration runner with `lock_timeout`; PgBouncer transaction-mode
  profile (simple protocol, no `LISTEN`). Then enable PgBouncer in devenv
  and staging. Keyset pagination for the hot lists; indexes on the FK
  filters, created `CONCURRENTLY`.
- `jobs`: leader lease for the customers token-reset scheduler; the support
  task worker generalised with heartbeat-extended claims.

## Quarter — platform hygiene

- CI: `golangci-lint` with the shared config, `go test -race`, `govulncheck`
  pinned, `buf lint`/`breaking` in events, CI for libraries and clients,
  `depguard`: no `http.Client{}`, no `signal.Notify`, no `promauto` outside
  the platform modules.
- Dockerfiles: multi-stage build inside Docker, pinned base images, one CA
  bundle from the image, no test keys, no SPA bundles in API images.
- `shaffra_lib_workflows`: delete the legacy `pkg/roles` engine and the
  duplicated evaluators; break the cycle with `support` by moving the
  client bootstrap out; add CI; stop the version skew (one minor per
  release train).
- Load test each service to find `MaxInFlight` for `httpx.Shed` and the
  bulkhead sizes for its dependencies; write the numbers next to the stacks.

## How to know it worked

| Signal | Before | Target |
|--------|--------|--------|
| messages lost on deploy | unknown (auto-ack) | 0, measured by inbox duplicates vs published |
| `events.messages{outcome="dead_lettered"}` | n/a | alerted, < 1/day |
| p99 under 3× load test | pod melts | shed 503s, p99 flat for admitted requests |
| `resilience_breaker_state` | n/a | opens within 10 s of an LLM provider outage; callers fail closed in ms |
| pool saturation | unbounded | `pg_stat_activity` ≤ 70 % of `max_connections` |
| time to fix a bug in the edge | 6 PRs | 1 PR + 6 version bumps |
