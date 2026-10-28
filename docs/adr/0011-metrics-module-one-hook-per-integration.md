# ADR 0011 — metrics is its own module family: `metrics/` with one hook per integration

**Status:** accepted · **Date:** 2026-10-28 · supersedes the metrics part of [0010](0010-otelx-layout-server-client-metrics-alarms.md)

## Context
ADR 0010 put the Prometheus exporter and the metrics toolbox under
`otelx/metrics` and had every hook emit spans and metrics together. Two
things turned out wrong. A service that wants metrics and no tracing (most
of the fleet today) still had to install the tracing SDK and reason about
`otelx` to get `/metrics`. And the per-integration metric instruments were
scattered: pool stats next to the pgx tracer, the HTTP observer next to the
tracing middleware, so "which metrics does the Redis client give me" had no
single answer. The earlier library family already had the vocabulary that
works: `metrics` with a core, a builder and one integration per system,
separate from `tracer`.

## Decision
Metrics are a top-level module family, independent of `otelx`:

- `metrics/`: the meter provider (`metrics.New`: Prometheus pull with the
  Go and process collectors, or OTLP push; the cardinality `Guard` view;
  runtime instruments), the business-metric `Builder`, the three bucket
  sets and `ErrorType`.
- `metrics/<system>`: one metrics-only hook per integration, in its own
  module when it needs an SDK: `http` (server observer and client
  transport), `grpc` (server and client interceptors), `pgx` (tracer and
  pool stats), `sql` (database/sql pool stats), `redis` (hook and pool
  stats), `nats` (publish/consume instrumentation and connection stats),
  `runtime`.
- `otelx/` installs the tracing SDK only; `otelx/server/*` and
  `otelx/client/*` are tracing-only hooks. A service wires both signals
  independently and shares the resource through `metrics.Config.Resource`.
- The closed-set vocabulary both signals label with (`error.type`,
  `db.operation.name`, pool names) lives on the metrics side
  (`metrics.ErrorType`, `metricspgx.Operation`, `metricsredis.ErrorType`);
  the tracing hooks import it, never the reverse.
- Pairing is by composition, never by a combined hook: `multitracer.New`
  for pgx, two `AddHook` calls for go-redis, nested handlers for NATS,
  chained interceptors for gRPC, the metrics transport inside the tracing
  transport for HTTP so the sample carries the span as an exemplar.
- Metric names, units, buckets and attribute sets are unchanged from 0010:
  dashboards survive.

## Consequences
- `otelx` drops the Prometheus client, the metrics SDK and the OTLP metric
  exporter from its dependency graph; `otelx.Config.Metrics`,
  `Telemetry.MeterProvider`, `Telemetry.MetricsHandler` and
  `Telemetry.Registry` are gone, replaced by `metrics.New` and
  `Metrics.Handler`.
- Adding an integration means two small modules with the same shape, or
  only the metrics one when a system has no useful span.
- `examples/` is one module with one `main` per package, so each hook has
  a compiled wiring example instead of one large reference service.
- `resilience/otel` and `events/otel` stay as they are: they implement this
  repository's own observer ports, which carry both signals by design.

## Alternatives rejected
- **Keep `otelx/metrics` and add `metrics/<system>` folders inside otelx.**
  Still forces the tracing SDK on metrics-only services. Rejected.
- **A combined hook per system that emits both signals (0010).** One
  constructor, but no way to take one signal without the other, and no
  single home for the metric contract. Rejected.
- **Prometheus client directly in `metrics`.** Two APIs again (ADR 0002)
  and no exemplars. Rejected.
