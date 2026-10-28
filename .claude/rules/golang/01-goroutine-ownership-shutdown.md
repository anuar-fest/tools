# 01 — Goroutine ownership and shutdown

## Intent

Every goroutine has an **owner**, a **cancellation path**, and a **join**. Shutdown drains handlers and workers under a budget — half-done stop is Fail.

Boundary companion: [`../architecture/11-concurrency-lifecycle.md`](../architecture/11-concurrency-lifecycle.md) — this lens goes deeper on ownership and stop order.

## Author mode

- Never fire-and-forget in a request handler; track with WaitGroup, errgroup, or a supervisor type.
- `WaitGroup.Add` before `go`; never Add inside the goroutine you are waiting on.
- Panic: recover at the edge of that goroutine (or supervised wrapper); do not rely on another goroutine’s recover.
- Shutdown order: stop intake → cancel in-flight → wait join → close resources.
- Prefer `errgroup` with derived context when N tasks share fate; document whether first error cancels siblings.

## Audit mode

Checklist:

- [ ] Can you name who starts and who stops each `go`?
- [ ] Does shutdown wait for workers, or only close the HTTP server?
- [ ] Are panics in workers contained?
- [ ] Does shared cancel accidentally cancel the whole process on one request?

Red flags: `go process(job)` with no WaitGroup; `Add` inside goroutine; `Shutdown` without waiting on background pollers.

## Go specifics / anti-patterns

```go
// Good: owned
g, ctx := errgroup.WithContext(parent)
g.Go(func() error { return work(ctx) })
return g.Wait()

// Bad: leak under load
go work(context.Background())
```

## Non-goals

- Not every program needs a full actor runtime.
- Cross-link architecture purity; this lens is about lifecycle, not layer names.
