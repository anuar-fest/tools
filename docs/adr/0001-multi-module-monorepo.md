# ADR 0001 — Multi-module monorepo with nested adapter modules

**Status:** accepted · **Date:** 2026-10-16

## Context
The libraries will be consumed by a dozen services that already pin
`shaffra_events`, `shaffra_lib_*` and `shaffra_client_*` at different
versions. A service that needs a retry policy must not inherit NATS, pgx,
OpenTelemetry and their transitive graphs. At the same time, docs, CI, lint
rules and the pattern template should live in one place and evolve together.

## Decision
One git repository. Every top-level directory with a `go.mod` is a module
tagged on its own (`resilience/v0.3.0`, `events/natsjs/v0.1.2`). Core modules
(`resilience`, `events`) depend on the standard library only. Adapters that
need an SDK are nested modules (`resilience/otel`, `events/natsjs`,
`events/pg`) with a `replace` to the sibling core for development; consumers
ignore `replace` and resolve the tagged core. A root `go.work` makes the
workspace build as one; CI runs per module.

## Consequences
- A service importing `resilience` gets zero third-party modules.
- Tagging is per module (`make release MODULE=resilience VERSION=v0.3.0`).
  A change spanning core and adapter needs two tags, core first.
- The Go directive of each module is the oldest toolchain the consumers run
  (1.25). Dependency versions are chosen to respect it (OTel 1.45.x).

## Alternatives rejected
- **Single module.** Simplest to tag; every consumer's `go.sum` and module
  graph grows with every adapter. Rejected for dependency hygiene.
- **One repository per library** (the previous `tools/*` family). Docs and
  CI drift; cross-cutting changes become five PRs. Rejected.
