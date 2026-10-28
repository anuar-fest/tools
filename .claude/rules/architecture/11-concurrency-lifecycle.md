# 11 — Concurrency and lifecycle

## Intent

Every goroutine has an **owner**, a **cancellation path**, and a **shutdown story**. Shared state is synchronized or confined. Leaks and deadlocks are Fail.

## Author mode

- Pass `context.Context` as the first argument on effectful and long-running calls; honor cancel/deadline.
- Start goroutines only when a type or function is responsible for stopping them (`Stop`, `Close`, `errgroup`, `context` cancel).
- Prefer bounded queues and worker pools over unbounded fan-out.
- Document lock ordering; keep critical sections small; avoid holding locks across I/O when possible.
- On shutdown: stop intake → drain or cancel in-flight → close resources → exit.
- Never fire-and-forget goroutines in request handlers without tracking.

## Audit mode

Checklist:

- [ ] Does every `go func` have a clear stop condition?
- [ ] Are contexts derived (`WithCancel`, `WithTimeout`) and canceled to avoid leaks?
- [ ] Can `-race` tests cover the shared structures?
- [ ] Deadlock risk: multiple mutexes, channel send without buffer/receiver, wait on self?
- [ ] Is `Close` idempotent and safe under concurrent close?

Red flags:

- Goroutines started in `init` with no stop.
- Ignoring `ctx.Done()` in select loops.
- Request-scoped work escaping the request without a supervisor.
- `time.After` in tight loops leaking timers (prefer `NewTimer` carefully or tickers with stop).

## Go adaptations

```go
// Good: owned workers
func (w *Worker) Run(ctx context.Context) error {
    for {
        select {
        case <-ctx.Done():
            return ctx.Err()
        case job := <-w.jobs:
            if err := w.handle(ctx, job); err != nil { return err }
        }
    }
}

// Bad
go process(job) // in HTTP handler, no tracking
```

## Non-goals

- Not every program needs actors or CSP purity.
- Channels are tools, not a religion — mutexes are fine for protecting maps.
