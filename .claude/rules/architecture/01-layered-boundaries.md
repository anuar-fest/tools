# 01 — Layered boundaries

## Intent

Separate **domain**, **application (use cases)**, **adapters (driving/driven)**, and **infrastructure** by *what the code is responsible for*, not by where files sit on disk.

## Author mode

- Put business rules in types and functions that do not know HTTP, SQL drivers, or message broker SDKs.
- Put orchestration (validate → load → decide → persist) in use-case / service types that depend on ports, not concrete stores.
- Put HTTP handlers, gRPC stubs, CLI, cron entrypoints in driving adapters: parse, authorize, call use case, map response.
- Put SQL, Redis, Kafka, S3, SMTP in driven adapters that implement ports.
- Wire concrete types once at the composition root (main / infra bootstrap).

## Audit mode

Checklist:

- [ ] Can you name each package’s layer role without looking at its path?
- [ ] Do handlers contain business rules, or only transport mapping?
- [ ] Do “repository” types leak into unrelated UI/API packages as concrete structs?
- [ ] Is wiring concentrated, or scattered `NewX(concreteY)` through domain code?

Red flags:

- SQL strings or `http.Request` inside domain decision functions.
- One package that is simultaneously router, use case, and DB access.
- “Utils” that reach into the database and also format JSON errors.

## Go adaptations

Layers are **roles of packages and types**. Valid layouts include flat modules, feature folders, or classic `internal/...` — all Pass if roles are clear.

```text
Good (any path):  Handler -> UseCase(port) -> Domain
Bad (any path):   Handler embeds *sql.DB and encodes refund policy inline
```

## Non-goals

- Do not require `domain/`, `usecase/`, `adapter/` directory names.
- Do not reject a mono-package service solely for being mono-package if boundaries inside it are clear (unexported helpers, ports at the edge).
