# tools — shared Go platform libraries

Reusable, framework-free building blocks for Go services: resiliency, event-driven
messaging, observability hooks, and (later) HTTP/gRPC/DB bootstrap.

## Layout and module rules

- **Multi-module monorepo.** Every top-level directory with a `go.mod` is its own module
  and is tagged separately (`resilience/v0.3.0`). Nested adapter modules
  (`resilience/otel`, `events/natsjs`) keep heavy dependencies out of the core.
- **Core modules depend on the standard library** plus `golang.org/x/*` at most.
  SDKs (NATS, pgx, OTel, chi) live only in nested adapter modules.
- **Consumer-owned ports.** Core packages define small interfaces (`Observer`,
  `Publisher`, `InboxStore`); adapters implement them. No framework types in core APIs.
- **Logging port is `*slog.Logger`.** `nil` means discard. Services wire their logger
  (zerolog, zap) through an `slog.Handler` adapter.
- **Metrics and traces go through the OpenTelemetry API** in `*/otel`, `otelx/*` and
  `metrics/*` modules only. Core packages emit through the `Observer` hook and never import OTel.
- **No package-level globals, no `init()` side effects, no `os.Getenv` in library code.**
  Everything is constructed with `New(Config)` and has an explicit `Close`/`Stop`.
- **The service edge** (`logx`, `otelx`, `metrics`, `alarms`, `health`, `httpserver`, `lifecycle`) is wired
  in a service's `main` in that order; `examples/` holds one `main` per package and must keep building.
- **Traces and metrics are separate module families** (ADR 0011). `otelx/` installs the tracing
  SDK; `otelx/server/<proto>` when you own the server, `otelx/client/<system>` when you call out.
  `metrics/` installs the meter provider and holds the shared
  buckets/guard/builder; `metrics/<system>` is the metrics hook for that integration. A new
  integration is a new `metrics/<system>` module, plus an `otelx/client/<system>` module when it
  has useful spans; the two pair by composition, never by a combined hook.
- **Alarms are Prometheus alerts** (ADR 0012): `alarms/` turns a recognised condition into
  `alarms_total`/`alarms_active` plus a log line and span event, and renders the alerting rules
  from declared definitions. No notifiers in the library: Alertmanager routes.
- **The concept is the service *edge***: use that word for it, no other.
- **Examples are one module** (`examples/`), one directory with a `main` per package it shows;
  no single large reference service.
- **Dependency versions are pinned to what builds with Go 1.25** (OTel 1.45.x, Prometheus
  exporter 0.67.x). Never run `go mod tidy` before every import exists in the module, never
  `go get` without a version, and run `make go-directive-check` after touching a `go.mod`.

## Documentation contract

Every package ships a `README.md` in the format of `docs/pattern-template.md`:
problem, when to use / when not, defaults, composition order, failure modes,
metrics it emits, "refuse list", and the proof tests. Architecture decisions go
in `docs/adr/`. Every PR that touches Go code ends with a
**Decisions & trade-offs** block (see `.claude/rules/golang/00-agent-usage.md`).

## Rules that apply

- `.claude/rules/architecture/` — boundaries, ports, purity, naming, dependencies.
- `.claude/rules/golang/` — goroutine ownership, channels, context, HTTP edge,
  pools, outbox/workers, overload, proof. P0 failures: leak, race, half shutdown,
  unbounded retry, silent error discard.
- This is a **library** repo: the "domain / usecase / infra" layering of an
  application does not apply literally. The equivalent is **core package
  (pure, stdlib) → adapter module (SDK)**, wired by the consuming service.

## Before finishing a change

```
make fmt-check vet build lint go-directive-check test   # or: make check
```

Integration tests use testcontainers and need Docker; `make test-short` skips them.
