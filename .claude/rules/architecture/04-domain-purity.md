# 04 — Domain purity

## Intent

Business rules must be expressible without databases, networks, clocks, randomness, or web frameworks. Domain code answers “what is allowed / what is true,” not “how we talk to Postgres.”

## Author mode

- Encode invariants in constructors / factory functions that return `(T, error)` or refuse invalid states.
- Prefer pure functions for calculations (fees, eligibility, state transitions).
- Pass time, IDs, and randomness in as values or via narrow ports when the rule depends on them — do not call `time.Now()` deep in policy if tests need determinism (see [08](08-side-effects-isolation.md)).
- Keep domain free of struct tags that exist only for ORM/JSON unless they are truly domain serialization (prefer mapping at the edge).

## Audit mode

Checklist:

- [ ] Can domain tests run without Docker, network, or env files?
- [ ] Do policy functions open connections, publish messages, or sleep?
- [ ] Are money, IDs, and statuses typed, or raw `string`/`float64` soup?
- [ ] Does “domain” import logging SDKs that force infrastructure config?

Red flags:

- `if err := db.Exec(...)` inside `func (o Order) Refund(...)`.
- Global mutable config read mid-decision.
- Domain package importing OpenTelemetry exporters or HTTP routers.

## Go adaptations

Purity is about **effects**, not about “no methods.” Methods on domain types are fine when they only transform state or enforce rules.

```go
// Good
func (o Order) CanRefund(now time.Time) bool { ... }

// Bad
func (o Order) Refund(ctx context.Context, db *sql.DB) error { ... }
```

## Non-goals

- Domain need not be a separate module or directory named `domain`.
- Occasional stdlib use (`time.Time`, `encoding` for value objects) is allowed when it does not perform I/O.
