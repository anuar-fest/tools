# 12 — Minimal surface and dependencies

## Intent

Keep the **exported API small** and the **dependency graph thin**. Domain and use cases must not be locked to frameworks. Prefer stdlib and a few trusted libraries.

## Author mode

- Export only what other modules must use; keep the rest unexported.
- Add a third-party module only with a reason (security, correctness, maintenance); prefer stdlib for HTTP utilities, JSON, and testing when enough.
- Do not let web frameworks, ORMs, or DI containers dictate domain types.
- Pin versions; avoid `replace` hacks without a local, verified copy strategy.
- Watch transitive size and license; prefer well-maintained libraries with clear owners.
- Public functions should be hard to misuse — defaults safe, errors explicit.

## Audit mode

Checklist:

- [ ] Does `go.mod` grow with each feature without justification?
- [ ] Are framework tags/types present in core packages?
- [ ] Is the exported surface a coherent facade or an accident of `New*` sprawl?
- [ ] Could a stdlib or existing port replace a new dependency?

Red flags:

- Domain importing a mega-framework for one helper.
- Multiple overlapping libraries (several HTTP routers, several loggers) without migration plan.
- Exported package that re-exports infrastructure clients.

## Go adaptations

```text
Pass: domain free of chi/gin/pgx; main wires pgx adapter
Fail: type Order struct { /* gorm tags */ } used as enterprise model everywhere
```

Minimal surface also means fewer ways to break invariants — see [05](05-objects-immutability-state.md).

## Non-goals

- Not a ban on solid infrastructure libraries.
- Not a requirement to vendor the world or rewrite crypto/http stacks.
