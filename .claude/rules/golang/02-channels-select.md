# 02 — Channels and select contracts

## Intent

Channel **close ownership**, **nil-channel disable**, and **select** behavior must be intentional. Busy `select`+`default` spins and wrong closers deadlock or panic.

## Author mode

- Exactly one closer for a channel (or document fan-in close protocol).
- Receivers must tolerate closed channels; never close from multiple senders without sync.
- Use `nil` channel in select to disable an arm after close/cancel.
- Bound buffers with an overflow policy (block, drop, shed) — never “buffer forever” as a plan.
- Prefer `select` on `ctx.Done()` over spinning `default` loops.

## Audit mode

Checklist:

- [ ] Who closes? Who sends after close?
- [ ] Is there a `for { select { default: } }` busy loop?
- [ ] Does `range ch` exit on close, or block forever on an open abandoned channel?
- [ ] Fan-in: are all producers joined before close?

Red flags: close on shared channel from workers; send on closed; unbuffered rendezvous used as “queue” under load without backpressure story.

## Go specifics / anti-patterns

```go
// Good: cancel disables send path
case ch <- v:
case <-ctx.Done():
    return ctx.Err()

// Bad: burns CPU
for {
    select {
    case <-ch:
    default:
    }
}
```

## Non-goals

- Channels are not mandatory for all sync — mutexes are fine (see [03](03-sync-and-races.md)).
- Not a ban on buffered channels; require an explicit bound + policy.
