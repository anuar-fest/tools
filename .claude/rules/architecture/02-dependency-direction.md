# 02 — Dependency direction

## Intent

Dependencies must point **inward**: outer (transport, infra) know about inner (domain, ports); inner never know about outer. Cycles are Fail.

## Author mode

- Domain packages import only stdlib (and, rarely, pure shared kernels with no I/O).
- Use cases depend on **interfaces they define or own** (ports), not on adapter packages.
- Adapters import domain types and implement ports; they may import infra SDKs.
- Composition root imports everyone and constructs the graph once.
- Prefer defining interfaces where they are used (Go proverb: accept interfaces, return structs), especially for driven ports.

## Audit mode

Checklist:

- [ ] Does any domain/use-case package import `database/sql`, HTTP frameworks, cloud SDKs, or generated API stubs?
- [ ] Are there import cycles (`go list` / compiler cycle errors, or circular package refs)?
- [ ] Do adapters depend on other adapters when a shared port or domain type would suffice?
- [ ] Is the concrete store constructed inside domain logic?

Red flags:

- `domain` → `postgres` or `domain` → `httpapi`.
- Shared “models” package that imports both ORM tags and OpenAPI types used everywhere.
- Init-time side effects in leaf packages that force import order.

## Go adaptations

Evaluate with the **import graph**, not path prefixes:

```text
Pass:  billing/rules  <-  billing/app  <-  billing/http , billing/pg
Fail:  billing/rules  ->  billing/pg
```

A package named `infra` that is only imported from `main` can Pass; a package named `domain` that imports Redis can Fail.

## Non-goals

- No required ordering like `cmd → infra → adapters → usecases → domain`.
- No ban on feature-oriented packages that contain both port and unexported adapter if the **exported** API stays clean (document the exception in review).
