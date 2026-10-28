# 06 — Errors, absence, and the null spirit

## Intent

Failure and absence must be **explicit**. Do not paper over problems with zero values, panic-as-control-flow, or discarded errors.

## Author mode

- Return `(T, error)` for operations that can fail; check every error.
- Wrap with `%w` and context (`fmt.Errorf("refund order %s: %w", id, err)`).
- Use sentinel or typed errors for branches callers must handle (`ErrNotFound`, `ErrConflict`).
- When zero value is a valid result, document it; when zero means “missing,” use `(T, bool)`, a pointer, or a small optional type — do not overload silently.
- Do not panic for expected business failures; panic only for programmer bugs (invariant broken).
- Prefer `errors.Is` / `errors.As` over string matching.

## Audit mode

Checklist:

- [ ] Are there `_ =` or ignored `err` on I/O paths?
- [ ] Do APIs return bare `nil` slices/maps with unclear meaning vs empty?
- [ ] Are errors logged and swallowed without return?
- [ ] Does nil dereference surface as a runtime panic in “happy” paths?

Red flags:

- `result, _ := store.Load(ctx, id)` on a critical path.
- Returning `nil` error with invalid zero struct that looks successful.
- Using panic/recover for validation of user input.

## Go adaptations

**Nil is allowed when the type’s contract says so; ambiguity is not.** Nil interface holding typed nil remains a Fail-level footgun — avoid returning interfaces that can be typed-nil.

```go
// Good
order, err := repo.ByID(ctx, id)
if errors.Is(err, ErrNotFound) { ... }

// Bad
order := repo.MustByID(id) // panics on miss used as control flow
```

## Non-goals

- Not a ban on nil pointers or nil slices (empty vs nil slice nuance is acceptable if call sites are clear).
- Not a requirement for Result monads or third-party optional libraries.
