# ADR 0003 — `*slog.Logger` is the logging port

**Status:** accepted · **Date:** 2026-10-16

## Context
Services use zerolog through a global logger; older code used zap; each
earlier library defined its own private `loggerI` interface. A library that
logs must accept a logger without forcing a logging framework.

## Decision
Library packages accept `*slog.Logger` and treat `nil` as discard. They log
rarely (recovered panics, dead-lettered messages, stream changes) and never
log payloads or credentials. Services keep their framework and wire it in
through an `slog.Handler` adapter (zerolog and zap both have one).

## Consequences
- Zero logging dependencies in any module.
- Context-aware logging (`InfoContext`) carries trace ids when the handler
  supports it.

## Alternatives rejected
- **zerolog everywhere.** Locks every consumer to one framework. Rejected.
- **A custom `Logger` interface per library.** That is what caused four
  incompatible interfaces last time. Rejected.
