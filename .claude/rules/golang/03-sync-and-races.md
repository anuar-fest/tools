# 03 — Sync and races

## Intent

Shared memory needs a **clear sync story**: mutex lifetime with data, no copied locks, no lock-across-I/O, safe publish of config/maps, no TOCTOU.

## Author mode

- Never copy a `sync.Mutex` / `WaitGroup` / `Once` (pass pointers; embed carefully).
- Keep mutex and protected data in the same struct; do not return interior pointers that escape the lock.
- Check-then-act under the same critical section (maps, caches, idempotency keys).
- Do not hold `RWMutex` across network/DB/file I/O.
- `sync.Once`: never “reset” by assigning a new Once over an in-use one; redesign if re-init is required.
- Publish immutable snapshots with atomic pointer swap or RWMutex; readers must not mutate shared maps.
- No outer-loop variable capture in concurrent closures — pass `v := v` or bind as argument (pre-loopvar fix mental model still for clarity).

## Audit mode

Checklist:

- [ ] Does `-race` cover the hot path?
- [ ] Lock scope minimal? Any I/O inside `Lock`?
- [ ] Config/map publish race-free?
- [ ] TryLock paths still `Unlock` only when locked?

Red flags: mutex value copy; `Once` reassignment; append to shared slice without sync; TOCTOU on map then insert.

## Go specifics / anti-patterns

```go
// Good: atomic publish of immutable map
atomic.StorePointer(&p, unsafe.Pointer(newMap)) // prefer atomic.Pointer[T] in modern Go

// Bad
mu.RLock()
row, _ := db.Query(...) // I/O under lock
mu.RUnlock()
```

Prefer `atomic.Pointer[T]` over `unsafe` when available.

## Non-goals

- Not every map needs a shard; profile contention first ([11](11-measure-before-mutate.md)).
- Cond is allowed when slot/wait patterns need it — document spurious-wake handling.
