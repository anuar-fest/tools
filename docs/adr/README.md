# Architecture decision records

One file per decision, numbered, never edited after acceptance except to
mark it superseded. Format: Context → Decision → Consequences → Alternatives
rejected. Keep each under a page.

| ADR | Title | Status |
|-----|-------|--------|
| [0001](0001-multi-module-monorepo.md) | Multi-module monorepo with nested adapter modules | accepted |
| [0002](0002-observability-through-opentelemetry.md) | Metrics and traces through the OpenTelemetry API, Prometheus as exporter | accepted |
| [0003](0003-logging-port-is-slog.md) | `*slog.Logger` is the logging port | accepted |
| [0004](0004-resilience-policy-order.md) | Canonical resilience order and per-attempt breaker accounting | accepted |
| [0005](0005-hermetic-proto-codegen.md) | Hermetic protobuf generation with buf and `go tool` | accepted |
| [0006](0006-event-envelope-and-naming.md) | CloudEvents-shaped envelope, typed naming, at-least-once with inbox | accepted |
| [0007](0007-conservative-retry-classification.md) | Unknown errors are not retried | accepted |
| [0008](0008-service-edge-ports-and-lifecycle.md) | The service edge is ports plus a lifecycle, not a framework | accepted |
| [0009](0009-http-and-db-instrumentation-on-the-api.md) | HTTP and database instrumentation on the OpenTelemetry API, Prometheus client for the runtime | accepted |
| [0010](0010-otelx-layout-server-client-metrics-alarms.md) | otelx layout: `server/`, `client/`, `metrics/`, `alarms/` | superseded by 0011 for `metrics/`, 0012 for `alarms/` |
| [0011](0011-metrics-module-one-hook-per-integration.md) | metrics is its own module family: `metrics/` with one hook per integration | accepted |
| [0012](0012-alarms-are-prometheus-alerts.md) | alarms are Prometheus alerts: metric plus generated rule, no notifier in the library | accepted |
