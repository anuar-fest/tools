# 10 — Overload and backpressure

## Intent

Under load and dependency failure, the process must **bound**, **limit**, **shed**, and **isolate** — not amplify with unbounded retries and shared fate.

## Author mode

- Bound every async edge (queue depth, worker count, semaphore). Define overflow: block, drop, or shed with explicit status.
- Retries: budget, jitter, idempotency, max attempts — no retry storms on non-idempotent or always-fail deps.
- Breakers/bulkheads: fail-open vs fail-closed is a product decision; document it.
- Rate limit ingress vs egress vs abuse separately; local vs distributed limiter trade-off stated.
- singleflight / hedging: allowlist keys; hedging only when tail latency matters and cost is understood.
- Prefer shedding low-priority work over melting the DB pool ([06](06-pools-sql-lifecycle.md)).

## Audit mode

Checklist:

- [ ] Unbounded goroutine or channel growth under spike?
- [ ] Retry without budget or on non-idempotent write?
- [ ] Shared limiter/pool hides noisy neighbor?
- [ ] Cache stampede unprotected?

Red flags: `for { retry() }` without sleep/budget; infinite client timeout + infinite server queue; hedging thundering herd.

## Go specifics / anti-patterns

```go
// Good: admission
if !sem.TryAcquire(1) {
    writeShed(w) // 503 / 429
    return
}
defer sem.Release(1)
```

## Non-goals

- Not a mandate to install every middleware — compose the minimum stack for the surface.
- Cluster isolation (K8s) is complementary, not a substitute for in-process bounds.
