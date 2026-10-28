# ADR 0004 — Canonical resilience order and per-attempt breaker accounting

**Status:** accepted · **Date:** 2026-10-16

## Context
The previous executor nested `breaker(retry(timeout(op)))`. Its per-attempt
timeout cause was attached inside the retry layer, so the breaker, checking
the outer context, never counted a timeout: a hung dependency could not trip
it. Its breaker also saw one failure per call, not per attempt, and retries
went into an open circuit. Training material disagreed with itself on the
order (timeout→retry→breaker versus deadline→bulkhead→limit→breaker→retry).

## Decision
```
deadline → bulkhead → limit → retry( breaker( attempt-timeout( op ) ) )
```
The breaker sits **inside** retry and is consulted and recorded **per
attempt**. `ErrBreakerOpen` is not retryable, so a retry loop stops the
moment the circuit opens. Attempt timeouts are marked with
`ErrAttemptTimeout` by both the retry layer and the stack before the breaker
records them, so a hung dependency trips. The bulkhead and limiter are
outside retry: one slot and one token per call, not per attempt. The
deadline wraps everything.

For HTTP, `httpx` owns the timeouts: the attempt timeout is a timer-armed
cancel disarmed when headers arrive, because a deadline would keep ticking
and abort body reads.

## Consequences
- Every attempt, including a timed-out one, counts toward the breaker.
- A rejected call is reported twice on purpose: by the policy that refused
  (policy view) and by the stack (call view) with different `policy` labels.
- Six layers is the maximum on a hot path; more needs an ADR.

## Alternatives rejected
- **Breaker outside retry, counting once per call.** Hides N failed
  attempts behind one record and retries into an open circuit. Rejected.
- **Letting services compose by hand.** Produced three incompatible stacks
  before. Rejected; presets plus `depguard`.
