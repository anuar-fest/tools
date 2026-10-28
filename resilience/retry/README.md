# retry — bounded retries with jitter, classification and a budget

## Problem
A dependency blips for 200 ms. Without retries every request in that window
fails. With naive retries (every error, fixed delay, no cap) a 10 % failure
rate becomes 30 % load, the fleet retries in lockstep, and a 4xx decline is
retried three times as if it might change its mind.

## When to use / when not to
- Use around idempotent I/O whose failures are known to be transient.
- Do not use around validation, non-idempotent writes without an idempotency
  key, or an operation that already sits inside another retry (driver, SDK, mesh).
- Do not use `Always` unless the operation is idempotent by construction.

## API in 10 lines
```go
p := retry.Policy{Dependency: "ledger", MaxAttempts: 3, AttemptTimeout: 300 * time.Millisecond,
    Budget: budget /* one per dependency */, Observer: obs}
v, err := retry.DoValue(ctx, p, func(ctx context.Context) (Balance, error) {
    return ledger.Balance(ctx, id)
})
// adapters say what is transient:
return retry.Transient(err)          // driver-specific "try again"
return retry.Permanent(err)          // validation, 4xx, unknown outcome
return retry.WithRetryAfter(err, d)  // server asked for a wait
```

## Defaults
| Parameter | Default | Why |
|-----------|---------|-----|
| MaxAttempts | 3 | Two retries catch a blip; more is a storm. 1 disables. |
| AttemptTimeout | 0 (off) | Set it. A timed-out attempt is retryable; the caller's deadline is not. |
| InitialBackoff / MaxBackoff / Multiplier | 100ms / 2s / 2 | Standard exponential curve. |
| Jitter | Full | Uniform in [0, backoff]; best at de-synchronising a fleet. |
| MaxElapsed | 0 (ctx only) | The call deadline bounds it; set when there is no deadline. |
| MaxRetryAfter | 10s | A server asking for a minute is telling you to stop, not to wait. |
| RetryIf | `DefaultClassifier` | Conservative: known-transient only. Unknown errors are not retried. |

`DefaultClassifier` retries: `Transient`-marked errors, `ErrAttemptTimeout`,
`net.Error` timeouts, connection refused/reset/EPIPE, unexpected EOF. It never
retries `Permanent`, `context.Canceled`, the caller's deadline, or another
policy's rejection.

## Budget
`NewBudget` is a sliding-window budget shared by all calls to one dependency:
retries ≤ `Ratio` × requests + `MinRetriesPerWindow` (defaults 20 %, 10 per
10 s). A dependency failing every call then costs at most 1.2× its normal
load, not `MaxAttempts`×.

## Composition order
Retry sits inside the deadline, bulkhead and limiter and outside the breaker.
See the module README for what breaks otherwise.

## Failure modes
| Situation | Behaviour | Error | Decision |
|-----------|-----------|-------|----------|
| Attempt times out | retried | `ErrAttemptTimeout` wraps `DeadlineExceeded` | `retry` |
| Caller deadline passes | stop at once | caller's ctx error | `timeout` |
| Error not retryable | stop | the error | `failure` |
| Attempts exhausted | stop | `after N attempts: …` | `retries_exhausted` |
| Budget empty | stop | `ErrRetryBudget` wrapping the error | `retry_budget` |
| Caller cancels during backoff | stop | `Canceled` wrapping the error | `canceled` |

## Refuse list
- No unbounded attempts: there is no "forever" setting. Use a large count plus `MaxElapsed`.
- No retry of unknown errors by default. Explicitness beats a surprise retry of a double charge.
- No `Retry-After` above the cap.

## Proof
`retry_test.go`: attempt-timeout vs caller-deadline, Retry-After cap, budget
refusal, MaxElapsed, cancellation during sleep, backoff table for all jitter
modes. Sleeps are injected; nothing in the tests waits on a real clock.

## References
AWS Builders' Library, *Timeouts, retries and backoff with jitter*; Finagle
`RetryBudget`; Google SRE Book ch. 22.
