# hedge — hedged requests for tail latency

## Problem
A replica is garbage-collecting; 1 % of reads take 800 ms instead of 20 ms.
Across a fan-out of 10 that 1 % becomes 10 % of user requests. Sending a
second copy after the p95 and taking the first answer trims the tail at a
few percent of extra load.

## API
```go
cfg := hedge.Config{Name: "catalog", Delay: 40 * time.Millisecond, MaxHedges: 1, Budget: hedgeBucket /* ~5 % of RPS */}
item, err := hedge.DoValue(ctx, cfg, func(ctx context.Context) (Item, error) { return catalog.Get(ctx, id) })
```

## Rules
- Idempotent reads only. A hedged write is a duplicate write.
- `Delay` is the dependency's measured p95, not a guess. Below it you double the load; above it you hedge nothing.
- Always set a `Budget`: a failing dependency must not receive 2× load.
- The loser is cancelled through its context; the operation must honour cancellation, or the loser keeps running.
- Failures are not hedged: a fast error returns at once; only slowness starts a hedge.

## Decisions
`hedge_started`, `hedge_won`, `hedge_lost`, `success`, `failure`, `canceled`.

## Proof
`hedge_test.go` under `goleak`: fast first attempt never hedges, slow first
loses to the hedge, failures are not hedged, budget refuses, `MaxHedges`
bounds attempts.

## References
Dean & Barroso, *The Tail at Scale*, CACM 2013.
