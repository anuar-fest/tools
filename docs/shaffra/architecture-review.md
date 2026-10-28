# Shaffra service repositories — architecture review

**Scope.** The Go repositories under `shaffra/`: six services
(`shaffra_ai_trainer_api`, `shaffra_datalake_api`, `shaffra_robot_api`,
`shaffra_svc_assistants_support_api`, `shaffra_svc_customers_api`,
`shaffra_svc_storage_gateway_api`), the shared contracts repo
(`shaffra_events`), the domain library (`shaffra_lib_workflows`), the agent
engine (`shaffra_agents`), the four hand-written clients, and `shaffra_devenv`.
The two `-SP-*` worktrees are branch checkouts of `support` and were skipped.

**Method.** The audit lenses from `.claude/rules/architecture` and
`.claude/rules/golang`, applied in audit mode (Pass / Warn / Fail with
`path:symbol` evidence). Load and security findings with severities are in
[load-and-resiliency-findings.md](load-and-resiliency-findings.md); the fix
order is in [roadmap.md](roadmap.md).

## 1. Summary

The platform is **one service template copied six times**, and the copies
have drifted. Bootstrap, config, logging, database setup, HTTP server,
middleware, the NATS wrapper, the health endpoint, Dockerfiles and CI are
each a separate copy per repo. The storage gateway, the newest copy, fixed
several bugs (exit codes, `ReadHeaderTimeout`, readiness, constant-time key
compare, bounded publish queue) that the other five still carry. There is no
shared plumbing library at all; the only shared infrastructure code is the
NATS client inside `shaffra_events`, and it is the single largest source of
reliability risk.

What is good: the services are small and readable; chi and `database/sql`
with raw SQL are sane choices; `shaffra_events` has real contracts;
`shaffra_client_storage_gateway` is a production-grade client with retries,
replay and 22 test files; the support task worker is a solid design
(claims, per-type limits, dead-letter); testcontainers are used where tests
exist. The raw material for a platform library is already in the repos;
it has never been extracted.

## 2. How a service is built today

| Concern | Implementation (all six unless noted) | Verdict |
|---------|----------------------------------------|---------|
| Bootstrap | `urfave/cli` with a `Before` hook building config, logger, DB, services; `serve` → `cmd.StartServer` | Warn: four of six exit 0 on fatal errors (`main.go` in trainer `:229`, datalake `:126`, robot `:184`, customers `:166`), so Kubernetes restarts nothing and migrate jobs "succeed" |
| Dependency wiring | package-level lazy singletons `services.GetX()` with plain nil checks; only robot uses `sync.Once` (3 places) | **Fail** (architecture/05, golang/03): 60+ unsynchronised lazy singletons across trainer and support; concurrent first use is a race; `GetMessagingService` panics after 5×1 s if NATS is down (`shaffra_ai_trainer_api/internal/services/bootstrap.go:500-512`) |
| Config | `internal/utility/config.go`, 363–872 lines per repo: `envOrDefault*`, godotenv, infrahub or JSON overlay that only prints on failure | Warn: six struct copies of the same keys; silent overlay failure; `DB_MAX_OPEN_CONNS` parsed but never applied in trainer/support/datalake |
| Logging | zerolog global; `InitializeLogger` with a diode file writer | Warn: trainer, support, customers, datalake return early outside dev, so `LOG_LEVEL` is ignored in production; `srvName = "<service name>"` placeholder left in four repos |
| Database | `database/sql` + pgx stdlib, golang-migrate at boot, seeders | **Fail** (golang/06): no `MaxOpenConns` in trainer, support, datalake; `driver, err :=` shadows `err` so migration failures are dropped in trainer, customers, datalake (`persistance/bootstrap.go:54/:60/:51`); `db.Query("CREATE EXTENSION …")` leaks rows in four repos; two Postgres drivers (`lib/pq` and pgx) in trainer |
| HTTP server | plain `http.Server`, chi, `middleware.Timeout(60s)` | Warn: `ReadHeaderTimeout` only in customers and storage gateway; `Timeout(60s)` cancels nothing downstream because ~1 360 repository calls use non-context `Query`/`Exec` against ~72 context-aware ones |
| Middleware | chi defaults + custom CORS `*`, `APIKEY` compare, JWT via `shaffra_lib_authentication` | **Fail** (golang/13): five copies print the presented API key with `fmt.Printf`, compare it non-constant-time, and skip the check in `Dev` (`middleware/apikey.go:17-24`); hard-coded super-admin bypass token in support and customers (`middleware/authenticated.go:30`) |
| Events | `shaffra_events/pkg.Client`; `Subscribe` (auto-ack) at ~114 call sites vs ~25 `SubscribeWithManualAck`; publishes in fire-and-forget goroutines after commit (46 sites) | **Fail** (golang/09, 01): see §4 |
| Health | `/health` liveness only; `/ready` with a DB ping only in robot and storage gateway | Warn: readiness never reflects DB or NATS; Helm has liveness only, `replicas: 1` |
| Shutdown | `signal.Notify`, `srv.Shutdown(15s)`; NATS subscriptions never drained; robot's drain loop ignores its deadline (`internal/cmd/runServer.go:119-141`) | **Fail** (golang/01): half shutdown |
| Observability | promauto business metrics, `/metrics` on the public router; no tracing | Warn: no request/dependency RED metrics, no trace ids in logs, metrics endpoint unauthenticated |
| Retries / breakers | ad-hoc `time.Sleep(2^i)` loops in trainer, support, robot (LLM polling); no breakers; no budgets | **Fail** (golang/10): the `CreateThread` retry loops log `newThread.ID` on a nil `newThread` after exhausting retries, so an LLM outage becomes a panic (`shaffrallm/manager.go:86`, `openai/manager.go:111`, trainer `doRequestAnswerLoop.go:44-45`) |
| Transactions | Begin/Rollback inside each repository method; only support has `WithTransaction` | Warn (architecture/01): use cases cannot span repositories atomically |
| Pagination | `?il=true` lets a client disable the limit; 70+ server-side `IgnoreLimit: true` | Warn: unbounded result sets on demand |
| Tests | trainer, robot, customers, storage gateway have tests; support's are disabled in CI; datalake has none; libraries and clients have no CI | Warn |
| Lint | none anywhere; `-race` only in `shaffra_events` | Warn |

## 3. Duplication

Normalised MD5 fingerprints confirm byte-identical or near-identical copies
of: CLI bootstrap, `runServer.go`, config loader, logger, DB bootstrap,
`database.go` CLI, API-key and auth middleware, REST helpers and error
catalog, query-option parsing, `utility/ip.go`, NATS wrapper plus
`EnsureStream` lists, scheduler, observability init, health controller,
Dockerfiles with vendored CA bundle and public key, `ci.yaml`. The three
`shaffra_client_*` skeletons share byte-identical `types/config.go`,
`errors.go`, `pong.go`; `shaffra_client_ai_trainer/pkg/types/httpClient.go`
is a copy of `shaffra_agents/pkg/types/httpClient.go`.

The cost is not the bytes. It is that every fix lands in one copy: the
storage gateway's `os.Exit(1)`, `ReadHeaderTimeout`, constant-time compare,
readiness check and bounded publish queue exist in one repo out of six.

Within `shaffra_lib_workflows` the duplication is internal: `pkg/roles`
(legacy) and `pkg/shagroles` (current) implement the same role GUIDs;
`pkg/{evaluators,extractors,generators}` duplicate `pkg/agents/{…}`; 31
`init()` registrations through blank imports; a dependency cycle with
`support` (the library imports the support client, support imports the
library); consumers pin 24 minor versions apart.

## 4. `shaffra_events`: contracts are fine, the client is not

**Contracts.** 135 protos, all flattened into one Go package
(`go_package = "go/messages;messages"`), a hand-written `subjects.go` inside
the generated directory, no `reserved` fields anywhere, no breaking-change
check. Two incompatible identification styles: 43 messages carry
`UUID event_id`, 39 carry `EventMetadata`. No envelope: the type is implied
by the subject, there is no message id header, no tenant, no correlation,
no trace context. Subject naming mixes `customers.token.limit.reached`,
`cust_int.status.changed` and `conversations.robot_talk_latency_summary`.

**Generation.** The Makefile downloads `protoc` with `curl` and no checksum,
pins `protoc-gen-go` to v1.36.6 while the README says v1.35.2 and
recommends the system `protoc`; CI uses `setup-go: stable`; generated files
have been hand- and editor-edited (commits `a653048`, `a211ac8`, `749a96b`);
`.pb.go` files are not marked generated; the drift check only sees changed
files, never stale ones. The developer is blamed for an environment they
cannot reproduce. See [ADR 0005](../adr/0005-hermetic-proto-codegen.md) and
[codegen/proto.md](../codegen/proto.md) for the replacement.

**Client** (`shaffra_events/pkg/client.go`):

| Line | Problem | Effect |
|------|---------|--------|
| 85–88, 120 | URL built as `nats://user:pwd@host` and logged at Info | every service logs the broker password at boot |
| 221–231 | `Subscribe` acks **before** the handler runs, then queues the message in memory | any crash, deploy or panic loses messages; this is the method at ~114 call sites |
| 143–159, 176, 255 | unbounded linked-list queue per subject drained by one goroutine, no `recover` | a slow handler buffers without limit; one panic kills the pod |
| 198–202, 289–293 | fixed `AckWait` 20 s, no `InProgress`, `Nak()` with no delay, `MaxDeliver` set on 4 subscriptions | queued messages pass 20 s under load and are redelivered → duplicates pile up; no consumer deduplicates |
| 336–351 | `EnsureStream` ignores `StreamInfo` errors, uses `InterestPolicy` with no limits or replicas, and **rewrites the subject list** from the caller's pinned version | an older service removes subjects a newer one publishes on; the team's own `foundations.md` records "some missing streams panic the process" |
| 378–404 | `Publish` retries inline for up to 30 s | a request handler blocks for 30 s when NATS is slow; 46 handlers hide this behind `go publish()` with the error dropped |
| — | no `Close`/drain, no shutdown of subscriber goroutines | half shutdown |

The replacement is `events/natsjs` plus `events/pg` in this repo; the
mapping is in `events/natsjs/README.md`.

## 5. Clients

Three of four clients (`ai_trainer`, `robot`, `datalake`) share one
skeleton: a new `http.Client{Timeout: 30s}` per call (falling back to
`DefaultTransport` with 2 idle connections per host → connection churn), no
`context` parameter, no retries, no request-id propagation, `Ping` via
`http.DefaultClient` with no timeout (so `PingOnBoot` can hang a service's
startup), DTOs already diverging between trainer and robot. None is
generated from a spec; Swagger exists for customers and storage gateway and
nothing consumes it.

`shaffra_client_storage_gateway` is the exception: one pooled client, a
`RetryPolicy` with full jitter that refuses to retry a timed-out PUT,
`GetBody` replay, a redirect guard, context-aware waits. Its `retry.go` is
the ancestor of `resilience/httpx`.

## 6. Tooling, CI, deployment

- `ci.yaml` wraps a shared workflow; support omits `test`, datalake has no
  Makefile and no tests, libraries and clients have no CI.
- No `golangci-lint`, no `go vet` target, `-race` only in events.
- Swagger generation has the same drift as protos: `swag@latest` unpinned
  in two repos, `which swag && swag init || $GOPATH/bin/swag` runs whatever
  is first on PATH, `--parseDependency` makes output depend on dependency
  versions.
- Dockerfiles: scratch image with a binary built outside Docker; unpinned
  `golang:alpine` stage; each repo vendors its own CA bundle (robot's
  differs) and token public key; `shaffra.test.key` committed in five repos;
  trainer and robot ship `test/spa/dist` into production images.
- Helm: liveness probe only, `replicas: 1`, no readiness with a DB check.
- `shaffra_devenv`: the PgBouncer phase-1 document describes pool limits and
  `simple_protocol` settings that exist in no checked-out branch; the
  pgbouncer service is commented out; the support task worker uses
  `LISTEN/NOTIFY`, which a transaction pooler cannot serve.

## 7. Lens scorecard

| Lens | Score | Evidence |
|------|-------|----------|
| architecture/01 layered boundaries | Warn | handlers call repositories directly in places; transactions per repository method |
| architecture/02 dependency direction | Fail | `shaffra_lib_workflows` ↔ `support` cycle through the clients package |
| architecture/03 contracts and ports | Warn | interfaces exist (`domain/interfaces`) but are fat and mirror concrete types |
| architecture/05 state ownership | Fail | unsynchronised lazy singletons; robot `Session` fields mutated from several goroutines (`entities/session.go:49`) |
| architecture/08 side effects | Warn | `os.Getenv` confined to config, good; `time.Sleep` retry loops in services |
| architecture/09 testing | Warn | testcontainers used; support tests disabled; datalake none |
| architecture/12 minimal deps | Warn | two Postgres drivers, two UUID libraries, two chi majors in datalake |
| golang/01 goroutine ownership | Fail | fire-and-forget publishes, untracked `go c.processApplication` on an unauthenticated endpoint, no consumer shutdown |
| golang/03 sync and races | Fail | singletons; session fields |
| golang/05 HTTP edge | Fail | no `ReadHeaderTimeout` in four services, no `MaxBytesReader`, per-call `http.Client`, `Ping` without timeout |
| golang/06 pools and SQL | Fail | unlimited pools; leaked rows; non-context queries |
| golang/09 outbox and workers | Fail | publish-after-commit in goroutines; no outbox; auto-ack consumer |
| golang/10 overload | Fail | no breakers, budgets, shedding; `il=true` unbounded pages; rate limit only in storage gateway |
| golang/13 security edge | Fail | API keys printed; SNS signature check disabled with SSRF (`webhooks/transcriptionComplete.go:42-47`); LLM-generated SQL executed as is (`aiQueryBuilder/queryBuilder.go:121-143`); unauthenticated test-data delete (`restapi/bootstrap.go:1009`); tenant id leaks across customers through the LLM manager cache key (`services/bootstrap.go:343-360`) |
| golang/14 proof | Warn | no lint, `-race` only in events |

## 8. What to keep

- chi, `database/sql`/pgx with raw SQL, golang-migrate, zerolog: all fine;
  the problem is the copies, not the choices.
- The storage gateway service and client as the reference for the rest.
- The support task worker's claim/lease/dead-letter design, generalised into
  the `jobs` module.
- The protobuf contracts, once regenerated hermetically and given headers.
- The `.claude/rules` packs: the audit above is them, applied.
