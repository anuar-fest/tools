# workpool — fixed workers, bounded queue, honest close

## Problem
`go process(job)` in a handler is unbounded: a spike creates 50 000
goroutines, the heap doubles, GC takes the CPU, and the pod is OOM-killed
with the work lost. A pool bounds both the workers and the queue, reports a
full queue instead of growing, recovers panics, and has a shutdown story.

## API
```go
p, _ := workpool.New(workpool.Config{Name: "webhooks", Workers: 8, QueueSize: 256, Observer: obs, Logger: log})
if err := p.TrySubmit(func(ctx context.Context) { deliver(ctx, hook) }); errors.Is(err, resilience.ErrPoolFull) {
    deadLetter(hook) // the overflow policy is yours and explicit
}
// shutdown: stop intake → drain → join; or cancel+join if ctx ends first
err := p.Close(shutdownCtx)
```

## Defaults
| Parameter | Default | Why |
|-----------|---------|-----|
| Workers | 4 | Set from the work's nature: CPU-bound ≈ GOMAXPROCS, I/O-bound = the downstream's bulkhead. |
| QueueSize | 2 × Workers | A burst buffer, not a backlog. |

## Guarantees
- `Submit` blocks for queue space until ctx ends or the pool closes; `TrySubmit` never blocks.
- No send on a closed channel: intake is fenced by a lock and a `closing` signal that wakes blocked submitters first.
- A panicking job is recovered, logged with its stack, counted (`Panics()`, decision `panic`), and the worker keeps going.
- `Close` is idempotent and always joins the workers. If ctx ends first, running jobs see their context cancelled and `Close` returns `ctx.Err()` after they exit; jobs must honour their context.

## Refuse list
- No unbounded queue option.
- No "fire and forget" helper.

## Proof
`workpool_test.go` runs under `goleak`: drain on close, full queue, blocked
submit released by close, cancel-and-join on close deadline, panic recovery,
concurrent submit/close.
