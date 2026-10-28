# 04 — Context and time

## Intent

Context is a **deadline and cancel budget**, not a DI bag. Timers must be stopped; `time.After` / `Tick` in loops leak.

## Author mode

- Always `defer cancel()` after `WithCancel` / `WithTimeout` / `WithDeadline`.
- Derive per-dependency timeouts from a parent budget; do not share one forever-context for all egress.
- Honor `ctx.Done()` in workers and selects.
- Prefer `time.NewTimer` / `time.NewTicker` with `Stop` (and drain when required) over `time.After` in loops.
- Never use `time.Tick` in production code (unstoppable ticker).
- `context.Value`: only request-scoped metadata (trace/correlation); not optional dependencies or DB handles.

## Audit mode

Checklist:

- [ ] Is every derived context canceled?
- [ ] Do timeouts exist on HTTP clients and outbound calls?
- [ ] Timer/ticker Stop on all exit paths?
- [ ] Does cancel of one request tear down process-wide workers?

Red flags: `context.Background()` in handlers; After in `for`; ignored ctx on Query/Exec; sleep-as-retry without jitter budget.

## Go specifics / anti-patterns

```go
// Good
ctx, cancel := context.WithTimeout(parent, 2*time.Second)
defer cancel()

t := time.NewTimer(d)
defer t.Stop()

// Bad
for {
    select {
    case <-time.After(time.Second): // leaks timers
    case <-ctx.Done():
        return
    }
}
```

## Non-goals

- Not a ban on `context.Value` for trace IDs.
- Cross-link architecture side-effect isolation for injectable clocks in pure domain tests.
