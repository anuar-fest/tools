# breaker — circuit breaker per dependency

## Problem
Fraud scoring starts answering in 9 s instead of 90 ms. Every checkout
goroutine waits on it, the connection pool fills, the HTTP server stops
accepting, and the pod is "down" although only one dependency is. A breaker
turns 9 s of waiting into an immediate `ErrBreakerOpen`, so the caller can
fail closed, fail open, or degrade, in microseconds.

## When to use / when not to
- One breaker per dependency with its own failure domain. Never one global breaker.
- Trip criteria differ per dependency: consecutive failures for connection-level
  failures (refused, DNS); error rate for busy HTTP dependencies; slow-call
  threshold where latency is the failure.
- Do not put a breaker around validation or around calls that return 4xx for
  caller mistakes without marking them `breaker.Ignore`.

## API in 10 lines
```go
b, _ := breaker.New(breaker.Config{Name: "fraud",
    Trip: breaker.ErrorRate(10*time.Second, 20, 0.5), OpenFor: 10 * time.Second,
    SlowCallThreshold: 800 * time.Millisecond, Observer: obs})

permit, err := b.Allow(ctx)
if err != nil { return ErrFraudUnavailable } // fail closed on money
score, err := fraud.Score(ctx, p)
permit.Record(err)                            // exactly once
// or: score, err := breaker.DoValue(ctx, b, func(ctx) (Score, error) {...})
```

## Defaults
| Parameter | Default | Why |
|-----------|---------|-----|
| Trip | `ConsecutiveFailures(5)` | Safe for low volume; switch to `ErrorRate` when busy. |
| OpenFor | 30s ± 20 % jitter | Long enough for a restart; jitter stops replicas probing together. |
| MaxHalfOpenProbes | 1 | A recovering dependency gets one probe, not the herd. |
| SuccessesToClose | 1 | Raise it for flappy dependencies. |
| SlowCallThreshold | 0 (off) | Set it where "slow" is "down" for your callers. |
| IsFailure | `DefaultIsFailure` | Everything except: `Ignore`d, `Canceled`, the caller's own deadline, other policies' rejections. Attempt timeouts **do** count. |

## State machine
```
closed --(Trip says so)--> open --(OpenFor passed, next Allow)--> half-open
half-open --(SuccessesToClose)--> closed
half-open --(one failure)--> open (new OpenFor window)
```
Every transition bumps a generation; a permit from an older generation is
ignored on `Record`, so a slow in-flight call cannot close a breaker that
opened after it started.

## Composition order
Inside retry, outside the attempt timeout. `ErrBreakerOpen` is not retryable,
so a retry loop stops the moment the breaker opens.

## Failure modes
| Situation | Behaviour | Decision |
|-----------|-----------|----------|
| Open and `OpenFor` not passed | `Allow` returns `ErrBreakerOpen` | `breaker_open` |
| Half-open with a probe in flight | second `Allow` is rejected | `breaker_open` |
| Dependency hangs | attempt timeout counts as failure, trips | transition `closed→open` |
| Caller gives up (`Canceled`) | not counted | — |
| Server says 429 | wrap with `Ignore`: it is our quota, not their health | — |

## Observability
Decisions: `breaker_open`. Transitions via `ObserveBreakerState` with
from/to; the OTel adapter exports `resilience.breaker.state` (0/1/2) and
`resilience.breaker.transitions`.

## Refuse list
- No shared breaker across dependencies.
- No "fail open by default". Open means the caller decides; on a money path that is usually fail closed.
- `FailuresInWindow` exists only to migrate from the old absolute-count breaker; it trips on noise.

## Proof
`breaker_test.go`: full state walk with a fake clock, probe limit, stale
permit, error-rate minimum and sliding window, slow call, ignored errors,
concurrent use under `-race`.

## References
Nygard, *Release It!*; Microsoft, *Circuit Breaker pattern*; resilience4j
`CircuitBreaker` (count- and time-based sliding windows).
