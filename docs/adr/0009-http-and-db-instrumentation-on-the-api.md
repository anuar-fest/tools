# ADR 0009 — HTTP and database instrumentation on the OpenTelemetry API, Prometheus client for the runtime

**Status:** accepted · **Date:** 2026-10-16

## Context
`otelhttp` is the contrib instrumentation for `net/http`; `otelsql` and
`otelpgx` exist for databases; `contrib/instrumentation/runtime` exports Go
runtime metrics. Using them is the default choice. Three facts pushed the
other way: routers know the matched pattern only after the handler ran,
and `otelhttp` names spans before; the services' dashboards are built on
`go_goroutines`, `process_resident_memory_bytes` and friends from the
Prometheus client; and every contrib module follows its own version line
that has to stay in step with the pinned OTel 1.45.

## Decision
- `otelx/server/http` writes the server span and the `http.server.*`
  metrics directly on the API with the HTTP semantic conventions. The span
  is renamed to `METHOD route` after the handler, status and panic come
  from the shared `ResponseRecorder`, and the route reaches outer
  middleware through a context holder (`Routed`, `SetRoute`).
- `otelx.Transport` writes the client span the same way, wrapping the
  `resilience/httpx` transport so one span covers all attempts.
- `otelx/client/pgx` implements pgx's `QueryTracer`, `BatchTracer`,
  `ConnectTracer` and `pgxpool.AcquireTracer`, and `OpenDB` makes
  `database/sql` users traced with a one-line change.
- `otelx` serves Prometheus with the client's Go and process collectors on
  the same registry as the OTel exporter, so existing panels keep working
  and `promauto` metrics can share the endpoint during migration. The OTel
  runtime instrumentation is not pulled in.
- Query text is recorded by default (pgx sends parameters separately) with
  a switch for SQL built from user input; arguments are never recorded;
  URLs lose their query string.

## Consequences
- We own semantic-convention drift for two small instrumentations; the
  attribute set is tested.
- No contrib modules in the graph; one OTel line to bump.
- Exemplars link histogram buckets to traces because the Prometheus
  handler speaks OpenMetrics.

## Alternatives rejected
- **otelhttp + otelhttp.WithRouteTag per route.** Correct attributes, but
  route tagging must be repeated on every registration and the span name
  stays `METHOD`. Rejected.
- **otelpgx (exaring).** Good library; it pins its own OTel line and does
  not cover `database/sql` or pool stats. Rejected for now.
- **Dropping the Prometheus client, OTLP only.** Breaks every existing
  dashboard on day one. Rejected until the collector is in place.
