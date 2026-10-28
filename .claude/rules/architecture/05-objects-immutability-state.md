# 05 — Objects, immutability, and state

## Intent

Prefer **immutable values** and clear ownership of mutable state. Avoid anemic data bags plus external “managers” that mutate them from everywhere.

## Author mode

- Prefer value types for domain snapshots; mutate through methods that enforce invariants, or return a new value.
- Unexport fields that must stay consistent; expose behavior, not a field zoo.
- Give every shared mutable structure a single owner (one goroutine, or a type that serializes access).
- Avoid public mutable globals and package-level caches without lifecycle.
- Constructors should assemble valid objects — avoid half-built structs that callers must “init” later (`NewX(...) (X, error)`).

## Audit mode

Checklist:

- [ ] Who is allowed to mutate this struct? Is that documented by API (unexported fields, methods)?
- [ ] Are there getter/setter pairs with no invariant — just public fields in disguise?
- [ ] Is shared state protected, or raced across goroutines?
- [ ] Do “DTO-only” types leak into the core and get mutated ad hoc?

Red flags:

- Large exported struct with twenty fields and no methods, mutated in five packages.
- `sync.Mutex` forgotten on a map used as a cache.
- Constructors that leave required fields zero and rely on callers to set them.

## Go adaptations

**Do not expose internals that let callers break invariants.** Read-only accessors are fine when they preserve encapsulation. Package-level functions are idiomatic Go when they operate on values without needing polymorphic objects.

```go
// Good: invariant held inside
func (a Account) Debit(amount Money) (Account, error)

// Smell: everyone pokes fields
acc.Balance -= amount
```

## Non-goals

- Not every type must be an “object” with methods.
- Immutability is a default preference, not a ban on buffers, builders, or performance-oriented mutation behind a tight API.
