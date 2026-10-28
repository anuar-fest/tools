# ADR 0007 — Unknown errors are not retried

**Status:** accepted · **Date:** 2026-10-16

## Context
The previous executor retried every error, including validation failures
and non-idempotent writes with unknown outcomes, and its HTTP ignore list
treated 502/503/504 as "not a failure". Retrying blindly is how a 10 %
failure becomes a 30 % load and how a payment is charged twice.

## Decision
`retry.DefaultClassifier` retries only what is known transient: errors
marked `Transient`, per-attempt timeouts, `net.Error` timeouts, connection
refused/reset, unexpected EOF. Everything else is final unless the adapter
marks it (`retry.Transient`, `retry.WithRetryAfter`) or the caller supplies a
classifier. `retry.Always` exists for operations idempotent by construction.
For HTTP, `httpx` classifies by status (502/503/504/429 retryable, all 5xx a
breaker failure, 429 not) and by method and body replayability.

## Consequences
- Adapters must say what is transient. That is a feature: it is written
  next to the dependency, reviewed, and tested.
- A new dependency's unknown errors fail fast until classified.

## Alternatives rejected
- **Retry everything but a deny list.** The deny list is never complete.
  Rejected.
