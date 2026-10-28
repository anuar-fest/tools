# 07 — Composition over inheritance

## Intent

Reuse and variation come from **composition**, small interfaces, and decoration — not type hierarchies or “base class” embedding abuse.

## Author mode

- Build complex behavior from small types wired in constructors.
- Use embedding when the outer type *is* a thin extension and method promotion is intentional — not to share a grab-bag of fields.
- Prefer interfaces + adapters for swapping implementations over deep embed chains.
- Avoid “BaseService” structs that every feature embeds to get logging, DB, and config.
- Prefer wrapping (`type RetryingStore struct { inner Store }`) for cross-cutting retry/metrics.

## Audit mode

Checklist:

- [ ] Does embedding promote methods that break encapsulation or leak internals?
- [ ] Is there a god embed (`type X struct { *App; *DB; *Cache; *Metrics }`)?
- [ ] Are variants expressed as separate types implementing a port, or as `switch kind` forests?
- [ ] Does “reuse” create tight coupling between unrelated features?

Red flags:

- Embedding concrete `*sql.DB` into domain types.
- Three-level embed chains just to share `log` and `cfg`.
- Copy-paste hierarchies simulated with embedding and overridden methods via name shadowing confusion.

## Go adaptations

```go
// Good: decorator
type LoggedStore struct {
    inner OrderStore
    log   *slog.Logger
}

// Smell: kitchen-sink embed
type OrderService struct {
    *Server
    *Repository
    *Publisher
}
```

## Non-goals

- Embedding `sync.Mutex` or small utility types is fine when intentional.
- No Java-style “everything final” — unexported structs and interfaces already limit extension.
