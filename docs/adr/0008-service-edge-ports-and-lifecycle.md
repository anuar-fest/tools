# ADR 0008 — The service edge is ports plus a lifecycle, not a framework

**Status:** accepted · **Date:** 2026-10-16

## Context
Six Shaffra services carry their own `runServer.go`, `logger.go`,
`observability.go` and `signal.Notify`, each slightly different and each
with the same defects: no HTTP timeouts, no readiness drain, no tracing,
exit code 0 on a failed shutdown. The earlier catalog service solved the
same problems with Echo, echo-contrib Prometheus and Jaeger middleware,
Sentry middleware and go-redis hooks, which tied every service to that
stack and its versions.

## Decision
Phase 2 ships the edge as small stdlib-only modules with ports, and nested
`*/otel` modules that implement the ports on the OpenTelemetry API:

| Module | Port / hook | Adapter |
|--------|-------------|---------|
| `logx` | `ContextAttrs func(ctx) []slog.Attr` | `httpserver.LogAttrs`, `otelx.LogAttrs`; `logx/zerolog` bridges the existing logger |
| `httpserver` | `Observer{ObserveInFlight, ObserveRequest}`, `RouteFunc` | `otelx/server/http` (metrics and server spans) |
| `health` | `Check func(ctx) error` | `db.PingContext`, `pool.Ping`, `health.Bool(nc.Healthy)` |
| `lifecycle` | `Component{Run, Stop}` | every module's `Run`/`Stop`/`Shutdown`/`Close` |
| `otelx` | — (the one SDK install) | OTLP/HTTP traces, Prometheus or OTLP metrics |
| `otelx/client/pgx` | pgx's own tracer interfaces | spans, durations, pool stats for pgx and database/sql |

Routers, JWT libraries, DI containers and error trackers stay in the
service. `lifecycle` stops components in reverse order of registration
under one budget and returns an exit code; `health.SetDraining` runs first
so the load balancer stops sending traffic before the listener closes.

## Consequences
- A service's `main` is wiring: logger → telemetry → health → components → `lifecycle.Run`.
- Any router works; the matched route reaches metrics and spans through `RouteFunc` and `Routed`/`SetRoute`.
- Error tracking (Sentry) is not a module: with traces and `error.type` on
  metrics the same questions are answered by the tracing backend; a service
  that still wants Sentry adds its middleware after `Recover`.
- The go directive stays at 1.25 and dependency lines are pinned to what
  builds with it; `make go-directive-check` fails a bump.

## Alternatives rejected
- **Adopt one framework (Echo, chi + contrib) across the fleet.** Fast to
  start, but every service inherits the framework's version cadence and the
  edge cannot be tested without it. Rejected.
- **Uber Fx modules first.** Fx solves ordering with a DI container; the
  services do not use it yet, and a container hides the order that
  `lifecycle` makes explicit. Deferred to `fxmod` for services that adopt Fx.
