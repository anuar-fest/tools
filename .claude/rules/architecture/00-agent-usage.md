# 00 — Agent usage

## Intent

Tell agents when and how to apply the architecture lenses: **author mode** while writing Go, **audit mode** while reviewing it. Severity triage keeps feedback actionable.

Pair with production craft: [`../golang/00-agent-usage.md`](../golang/00-agent-usage.md). Architecture answers boundaries/purity; golang answers hang/race/leak/cascade/p99/money. When producing Go code, also emit the craft pack’s **Decisions & trade-offs** block.

## Author mode

Use when creating or changing Go code.

1. Skim [01](01-layered-boundaries.md)–[04](04-domain-purity.md), [06](06-errors-absence-null-spirit.md), [08](08-side-effects-isolation.md), [10](10-naming-responsibility.md) before writing non-trivial logic.
2. Pull in [05](05-objects-immutability-state.md), [07](07-composition-over-inheritance.md), [11](11-concurrency-lifecycle.md), [12](12-minimal-surface-deps.md) when the change touches state, types, concurrency, or new dependencies.
3. Prefer fixing the design at the boundary over inventing a folder tree to “look Clean.”
4. Keep packages cohesive by concern; do not rename trees solely to match a template.
5. Apply matching [`../golang/`](../golang/) craft lenses for concurrency, HTTP, pools, money, and proof.

## Audit mode

Use when reviewing a diff, PR, or existing module.

1. Walk applicable lenses (all of them for a new service; a subset for a narrow fix).
2. For each lens, emit one of:
   - **Pass** — evidence that the concern is handled
   - **Warn** — smell, not yet breaking
   - **Fail** — violates a core constraint; should block or be fixed before merge
3. Cite evidence as `path:symbol` or a short snippet reference.
4. Do not fail a change for “wrong folder names.”

### Severity triage (Fail first)

| Priority | Typical Fail triggers |
|----------|----------------------|
| P0 | Dependency inversion broken; domain does I/O; silent error discard; goroutine leak / no shutdown |
| P1 | Fat interfaces; anemic mutable bags with no owner; framework types in core logic |
| P2 | Naming drift; extra third-party dep without need; missing injectable clock in hard-to-test path |

Warn on style debt that does not yet corrupt boundaries. Pass when the concern is clearly satisfied even if layout is unconventional.

## Go adaptations

- Lenses evaluate **import graphs, types, and side effects**, not directory conventions.
- A flat `package payments` with clear ports can Pass every lens; a perfect `internal/domain` tree that imports `database/sql` in “domain” can Fail.

## Non-goals

- Not a CI linter config.
- Not a mandate to restructure the whole repo when auditing one package.
- This pack is self-contained within `rules-generic/`; it requires no other project folders.
