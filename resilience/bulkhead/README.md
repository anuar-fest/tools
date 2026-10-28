# bulkhead — bounded concurrency per dependency

## Problem
A breaker notices a dependency is failing; it does not stop 2 000 goroutines
from already waiting on it. A bulkhead caps how many calls to one dependency
can be in flight, so the worst case is `MaxConcurrent` stuck goroutines and
connections, not all of them.

## When to use / when not to
- Around every outbound dependency, sized with the HTTP transport's
  `MaxConnsPerHost` (or the DB pool share) for that dependency.
- Separate bulkheads for sync and batch traffic to the same store.
- Not a queue: a full bulkhead is a signal to shed. `MaxWait` is for absorbing
  a few milliseconds of jitter, not for parking requests.

## API in 10 lines
```go
bh, _ := bulkhead.New(bulkhead.Config{Name: "fraud", MaxConcurrent: 32, MaxWait: 20 * time.Millisecond})
release, err := bh.Acquire(ctx)
if err != nil { return err } // ErrBulkheadFull or ctx.Err()
defer release()
// or: bulkhead.DoValue(ctx, bh, op)
```

## Defaults
| Parameter | Default | Why |
|-----------|---------|-----|
| MaxConcurrent | required | Derive it: `(p99 latency × target RPS)` with headroom, then match the transport cap. |
| MaxWait | 0 (fail fast) | Waiting converts overload into latency; keep it under the attempt timeout if set. |

## Composition order
Inside the deadline, outside the limiter and retry. Each call acquires once;
retries inside it reuse the slot.

## Failure modes
| Situation | Error | Decision |
|-----------|-------|----------|
| No slot and `MaxWait` 0 | `ErrBulkheadFull` | `bulkhead_full` |
| No slot within `MaxWait` | `ErrBulkheadFull` | `bulkhead_full` |
| ctx ends while waiting | `ctx.Err()` | `canceled` |

## Observability
`InFlight()` and `Capacity()` are read by the OTel adapter as
`resilience.bulkhead.inflight` / `.capacity`; utilisation and the
`bulkhead_full` rate are the USE signals.

## Refuse list
- No unbounded `MaxWait`.
- No global bulkhead shared by unrelated dependencies.

## Proof
`bulkhead_test.go`: fail fast, brief wait, wait timeout, ctx while waiting,
peak-concurrency under 32 goroutines, idempotent release.
