# ADR 0010 — otelx layout: server/, client/, metrics/, alarms/

**Status:** superseded by [0011](0011-metrics-module-one-hook-per-integration.md) for `metrics/` and [0012](0012-alarms-are-prometheus-alerts.md) for `alarms/` · **Date:** 2026-10-17

## Context
The first cut placed instrumentation next to the core it served
(`httpserver/otel`, `otelx/pgx`) and left the client transport in the
otelx root. Three things were wrong with that. The earlier library family
(`tracer` with `http/server`, `http/client`, `grpc/server`, `grpc/client`,
`db/redis`; `metrics` with `core`, `builder`, `standard`, `integrations`;
`alarm`) had already taught the team a vocabulary that answers the first
question anyone asks: do I own this server, or am I calling someone? Second,
"metrics" meant two things, the Prometheus exporter and the business
metrics builder, and had no home. Third, alarms, the one piece every
service team asked for, had none.

## Decision
`otelx` is laid out by the question the integrator asks:

- `otelx/server/<proto>`: you own the server. `http` (chi, net/http) and
  `grpc` (interceptors plus a Server with keepalive, size and shutdown
  defaults, plus `grpc.health.v1` backed by `health.Registry`).
- `otelx/client/<system>`: you call out. `http`, `grpc`, `pgx` (pgx and
  database/sql), `redis`, `nats` (raw nats.go; the typed events path keeps
  `events/otel`).
- `otelx/metrics`: the Prometheus exporter and everything the hooks share:
  bucket sets, the cardinality `Guard` view, the business-metric `Builder`
  with validation and caps, runtime instruments, `ErrorType`.
- `otelx/alarms`: raise a condition the code recognised; counter, log,
  span event, deduplicated notification; hooks for resilience breakers,
  events dead letters and readiness.

Every hook emits spans and metrics together (one API, ADR 0002); there is
no separate metrics-only integration. Directories with SDK dependencies
are separate modules; `metrics`, `client/http` and `server/http`-free
packages live in the root module so `otelx` itself stays at the OTel SDK
plus the Prometheus client.

## Consequences
- The old paths `httpserver/otel` and `otelx/pgx` are gone; the examples
  and the Shaffra roadmap use the new ones.
- Adding an integration means adding `otelx/client/<system>` with the same
  shape: `New(Config)`, spans and metrics on the shared buckets, a
  `Register*Stats` for its pool or connection, a README in the pattern.
- `resilience/otel` and `events/otel` stay where they are: they implement
  this repository's own ports, not a third-party client.

## Alternatives rejected
- **One module per signal (`tracing/`, `metrics/`).** Doubles every hook
  and contradicts ADR 0002. Rejected.
- **Everything in one `otelx` module.** A Redis hook would pull gRPC into
  every consumer. Rejected (ADR 0001).
- **Alarms as log lines plus Loki rules.** Works until the log pipeline is
  the thing that is down; a counter and a direct notifier do not depend on
  it. Rejected as the only path.
