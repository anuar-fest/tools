# 10 — Naming and responsibility

## Intent

Names should **scream the business capability** and match what the code does. Each package and type should have one primary reason to change.

## Author mode

- Prefer package names like `billing`, `refunds`, `identity` over `services`, `utils`, `common`, `manager`, `misc`.
- Name files after responsibilities (`create_order.go`, `payment_repository.go`), not catch-alls.
- Methods: verbs that state outcomes (`Authorize`, `Capture`, `Cancel`); avoid `Handle`, `Process`, `Do` without object.
- Split types that accumulate unrelated methods; do not grow god objects.
- Put request/response converters in a dedicated converters area when mapping is non-trivial — still without requiring a fixed path.

## Audit mode

Checklist:

- [ ] Could you guess the business from package names alone?
- [ ] Does `utils` import half the system?
- [ ] Are there `Manager`/`Helper`/`Service` types that only delegate without a domain concept?
- [ ] Do names still match behavior after refactors (drift)?

Red flags:

- `internal/helpers/helpers.go` with DB + JWT + email.
- `ProcessData(data any)`.
- Packages named after technologies only (`redis`, `kafka`) used as the home for business rules.

## Go adaptations

Technology adapters may be named after the tech (`postgres`, `stripe`) **when they only adapt**. Business policy must not hide there.

Naming guidance is evaluative: rename when cohesion suffers — do not reshuffle the whole tree for aesthetics.

## Non-goals

- No enforced Screaming Architecture directory layout.
- No ban on a small `internal/platform` for truly generic plumbing (clock, logging setup) if it stays free of business rules.
