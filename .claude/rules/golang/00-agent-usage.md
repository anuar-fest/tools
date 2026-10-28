# 00 — Agent usage (Go craft)

## Intent

Tell agents when and how to apply the **golang** production-craft lenses, and enforce a mandatory **Decisions & trade-offs** narration whenever Go code is produced or substantially edited.

Pair with [`../architecture/00-agent-usage.md`](../architecture/00-agent-usage.md) for boundary/purity lenses.

## Author mode

Use when creating or changing Go code.

1. Apply craft lenses that match the change surface (concurrency → 01–04; HTTP → 05; DB → 06; money/JSON → 07; files → 08; async → 09; overload → 10; perf → 11–12; security/obs → 13; proof → 14).
2. Prefer the smallest correct fix over a rewrite.
3. Do not invent folder trees to satisfy craft rules.
4. **Before finishing**, emit the Decisions & trade-offs block (required below).

## Audit mode

Use when reviewing a diff, PR, or module.

1. Score applicable lenses Pass / Warn / Fail with `path:symbol` evidence.
2. Fail first on: goroutine leaks, races, money/contract lies, half shutdown, shared-pool contagion, unbounded retry, silent error discard.
3. Warn on missing profiles before micro-opts, lock granularity smells, incomplete observability.
4. Missing Decisions & trade-offs on a code-producing change → **Fail** this lens (`00`).

### Severity triage

| Priority | Typical Fail triggers |
|----------|----------------------|
| P0 | Leak/race/deadlock; money float/omitempty trap; no cancel/join; SQL rows/body leak; dual-write outbox |
| P1 | Shared pool blast radius; lock held across I/O; retry storm; PII in logs |
| P2 | Opt without budget; unsafe/cgo without bar; missing CI `-race` on touched concurrent pkgs |

## Mandatory: Decisions & trade-offs

**Whenever the agent produces or substantially edits Go code**, the response MUST include a short block titled **Decisions & trade-offs** (3–8 bullets) covering:

1. **Chose X over Y** — at least one concrete alternative rejected (mutex vs channel, buffer size, timeout placement, shared vs partitioned pool, timer vs `time.After`, etc.).
2. **Cancel / shutdown ownership** — who cancels, who joins, who closes — if any concurrency or long I/O exists; else state “no new concurrency.”
3. **Cost at 10× / failure** — one sentence on overload, dependency down, or complexity tax.
4. **Deliberately did not** — premature opt, `unsafe`, extra deps, framework wrappers, or scope creep.

Small decisions count. Architecture-only waffle without craft trade-offs is incomplete.

Example shape:

```markdown
## Decisions & trade-offs
- Chose bounded worker queue (size N) over unbounded goroutine-per-request to cap memory under spike.
- Parent ctx canceled on shutdown; workers exit via select on ctx.Done; WaitGroup joins before Close.
- At 10× load, excess work sheds with 503 rather than melting the DB pool.
- Did not add sync.Pool or unsafe; no profile justifying them.
```

## Relationship to architecture pack

| Pack | Question |
|------|----------|
| [`../architecture/`](../architecture/) | Boundaries, ports, purity, naming |
| This pack | Hang, race, leak, cascade, p99, money — and proof |

Use both. Do not re-litigate Clean Arch inside these lenses.

## Non-goals

- Not a CI config file (see [14](14-prove-correctness.md) for what to demand).
- Not mandatory package layout.
- This pack is self-contained within `rules-generic/`; it requires no other project folders.
