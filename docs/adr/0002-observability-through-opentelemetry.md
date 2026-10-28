# ADR 0002 — Metrics and traces through the OpenTelemetry API, Prometheus as exporter

**Status:** accepted · **Date:** 2026-10-16

## Context
Existing code uses `prometheus/client_golang` directly (promauto globals in
every service) and no tracing at all. The older `metrics` library had a
good idea, a cardinality guard, bolted onto Prometheus-specific types.
Traces and metrics need to correlate (exemplars, span events on policy
decisions), and the instrumentation API must not leak into core packages.

## Decision
Core packages expose a narrow `Observer` port and never import an SDK.
Adapter modules (`*/otel`) implement the port over the OpenTelemetry
**API**. Services install the OpenTelemetry **SDK** once, with the
Prometheus exporter serving `/metrics` (or OTLP to a collector), and pass
one observer to every policy. Metric names follow OTel conventions
(`resilience.decisions`); the exporter renders Prometheus names
(`resilience_decisions_total`). Labels are closed sets plus one free-text
dependency name; high-cardinality identifiers go to spans and logs.

## Consequences
- One instrumentation API for traces and metrics; span events link
  rejections to traces.
- Cardinality control moves from a runtime guard to a convention: closed
  label sets in code, reviewed in PRs, enforceable with OTel Views.
- The OTel line is pinned to what the consumers' toolchain supports.

## Alternatives rejected
- **Prometheus client directly.** Less migration today, two APIs forever,
  no trace correlation. Rejected.
- **A custom metrics facade.** Reinvents OTel's API with fewer exporters.
  Rejected.
