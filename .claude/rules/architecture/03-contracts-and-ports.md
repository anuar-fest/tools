# 03 — Contracts and ports

## Intent

Public behavior at boundaries is expressed as **small contracts** (interfaces, schemas, API DTOs). Callers depend on contracts; implementers fulfill them.

## Author mode

- Define ports with few methods (often 1–3) named for business capability: `RefundOrder`, `LoadAccount`, not `Do`.
- Keep transport DTOs at the edge; map to domain types before use-case logic.
- Validate untrusted input at the adapter (schema / explicit checks); do not trust JSON into domain blindly.
- Prefer returning concrete domain types from constructors; accept ports as parameters.
- Do not export broad “kitchen sink” interfaces that mix unrelated operations.

## Audit mode

Checklist:

- [ ] Are use cases testable by substituting a fake port without mocking frameworks?
- [ ] Do interfaces match consumer needs, or copy a fat concrete type method set?
- [ ] Are request/response shapes validated before domain work?
- [ ] Do exported packages leak driver-specific types (`pgx.Row`, `*redis.Client`) in signatures?

Red flags:

- `type Repository interface { /* 40 methods */ }`.
- Domain functions taking `map[string]any` from the wire.
- Interfaces defined only “for mocks” and never for design.

## Go adaptations

**Exported cross-package behavior** that has multiple implementations or must be substituted in tests should be a port. Unexported helpers and leaf structs need no interface.

```go
// Good: consumer-owned port
type OrderStore interface {
    Save(ctx context.Context, o Order) error
}

// Bad: interface mirroring an entire postgres package
```

## Non-goals

- No requirement that every struct method appear on an interface.
- No mandate for a separate `ports` package name — ports may live next to the use case.
