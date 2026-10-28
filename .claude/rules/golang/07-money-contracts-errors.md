# 07 — Money, contracts, and errors

## Intent

Money and wire contracts must be **exact**. Errors must be checkable. Nil interfaces and `omitempty` must not silently drop zeros or invent success.

Companion: [`../architecture/06-errors-absence-null-spirit.md`](../architecture/06-errors-absence-null-spirit.md).

## Author mode

- Store and compute money in minor units (`int64`) or a dedicated decimal type — never `float64` for currency.
- JSON: avoid `omitempty` on amount fields where zero is valid; use pointers or explicit presence when needed.
- Wrap errors with `%w`; compare with `errors.Is` / `errors.As`, not `==` on wrapped trees or string match.
- Never return a typed-nil pointer inside a non-nil error interface (or the reverse success lie).
- Validate untrusted JSON at the edge before domain math.

## Audit mode

Checklist:

- [ ] Any `float` on money paths?
- [ ] Zero amounts omitted from JSON unintentionally?
- [ ] Discarded errors on ledger writes?
- [ ] nil interface traps on error returns?

Red flags: `json:"amount,omitempty"` on `int`; `err == ErrNotFound` after wrap; panics for validation.

## Go specifics / anti-patterns

```go
// Good
type Amount struct{ Minor int64 `json:"amount_minor"` }

// Bad
type Payment struct{ Amount float64 `json:"amount,omitempty"` }
```

## Non-goals

- Not a full accounting ruleset — focus on representation and error honesty.
- Third-party decimal libs allowed if invariants stay in one type.
