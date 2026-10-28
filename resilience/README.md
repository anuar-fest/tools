# resilience — microservice resiliency policies for Go

`github.com/anuar-fest/tools/resilience` is a set of small, composable policies
that keep one slow or failing dependency from taking the whole process down.
The core module depends on the standard library only. Observability goes
through a tiny `Observer` port; the OpenTelemetry adapter lives in the nested
module `resilience/otel`.

| Package | Pattern | One line |
|---------|---------|----------|
| [retry](retry/) | Retry with budget | Re-run transient failures with jittered backoff, an explicit classifier and a fleet-wide budget. |
| [breaker](breaker/) | Circuit breaker | Stop calling a dependency that is failing; probe it carefully before trusting it again. |
| [bulkhead](bulkhead/) | Bulkhead | Cap concurrent calls per dependency so a slow upstream cannot hold every goroutine. |
| [deadline](deadline/) | Deadline budget | Give each hop a child deadline that leaves the caller time to react; detach after the commit point. |
| [ratelimit](ratelimit/) | Token bucket | Shape egress and ingress; per-key limits with bounded memory; never wait on a request path. |
| [shed](shed/) | Load shedding | Refuse low-priority work at the ingress before the process melts. |
| [workpool](workpool/) | Bounded worker pool | Fixed workers, bounded queue, panic recovery, drain-or-cancel close. |
| [hedge](hedge/) | Hedged requests | Send a second copy of a slow idempotent read; use the first answer. |
| [coalesce](coalesce/) | Request coalescing | Fold concurrent cache-miss fills for one key into one; the leader is detached. |
| [stack](stack/) | Canonical composition | One reviewed order for all of the above, with presets per dependency class. |
| [httpx](httpx/) | HTTP edge | A `RoundTripper` that composes the stack with HTTP-aware classification; 503/429 middleware. |
| [otel](otel/) | Adapter (own module) | Exports decisions, durations, breaker state and saturation through OpenTelemetry. |

## Composition order

```
deadline → bulkhead → limit → retry( breaker( attempt-timeout( op ) ) )
```

| Placed elsewhere | What goes wrong |
|------------------|-----------------|
| Retry outside the breaker | Retries hammer an open circuit; `ErrBreakerOpen` is not retryable here, so retries stop at once. |
| Breaker outside retry | The breaker sees one failure per *call* instead of per *attempt*; a dependency that times out every attempt never trips. |
| Bulkhead inside retry | Each retry acquires a slot; N attempts hold N× the capacity. |
| Limiter before bulkhead | A mis-configured limiter can still exhaust goroutines; the bulkhead fails closed. |
| Deadline innermost | Retries and waits run unbounded; the deadline must wrap everything. |

## Quick start

```go
cfg, _ := stack.Preset("fraud", stack.ReadFast, stack.PresetOptions{MaxConcurrent: 32, Observer: obs})
fraud, _ := stack.New(cfg)

score, err := stack.DoValue(ctx, fraud, func(ctx context.Context) (Score, error) {
    return client.Score(ctx, payment)
})
switch {
case errors.Is(err, resilience.ErrBreakerOpen), errors.Is(err, resilience.ErrBulkheadFull):
    // fail closed on the money path; the dependency is not usable right now
}
```

For HTTP, use `httpx.NewTransport` instead of wrapping `http.Client` calls by
hand; it knows about status codes, idempotent methods, body replay and the
body-outlives-the-attempt problem.

## Decisions every policy reports

`success failure timeout canceled bulkhead_full breaker_open limit_exceeded
retry retry_budget retries_exhausted shed pool_full hedge_started hedge_won
hedge_lost coalesced panic`

Two SLIs come out of them. `dependency_error_rate` (decision `failure` or
`timeout` on policy `stack`) says the upstream is unhealthy.
`policy_induced_reject_rate` (`breaker_open`, `bulkhead_full`,
`limit_exceeded`, `shed`) says *we* are protecting ourselves. A spike in the
second with a flat first is a mis-tuned policy, not an outage.

## Rules of use

- One stack, one breaker, one bulkhead **per dependency**. Sharing couples failure domains.
- Wrap I/O only. Validation, CPU work and in-memory lookups get no policy.
- At most one stack on a hot path. Nesting stacks multiplies attempts and hides which layer refused.
- `WriteOnce` for non-idempotent writes: no retries, breaker and bulkhead still apply.
- Fail-open versus fail-closed is a product decision per dependency. Write it down next to the stack.

## Proof

Every package runs under `go test -race`; `workpool`, `hedge` and `coalesce`
run under `goleak`. `httpx` has a fault-injection table against `httptest`
servers (503 then 200, exhausted retries, non-repeatable POST, 500 trips the
breaker, 429 does not, headers in time but body late, hung headers).

## References

- AWS Builders' Library, *Timeouts, retries and backoff with jitter*.
- Google SRE Book, ch. 22 *Addressing Cascading Failures* (retry budgets, load shedding).
- Netflix, *Performance under load* (adaptive concurrency limits).
- Dean & Barroso, *The Tail at Scale* (hedged requests).
- Nygard, *Release It!* (bulkheads, circuit breakers).
