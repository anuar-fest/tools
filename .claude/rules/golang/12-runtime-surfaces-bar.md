# 12 — Runtime surfaces bar

## Intent

`unsafe`, cgo, finalizers, and GC knobs are **privileged surfaces**. Use only with a clear machine-level reason and review bar — refuse otherwise.

## Author mode

- Prefer safe Go. Justify `unsafe` with a measured win and invariants (alignment, lifetime, no GC pointer smuggling mistakes).
- cgo: account for thread cost and latency; keep boundaries thin.
- Finalizers: last resort for native resources; prefer explicit `Close`; never for ordinary app logic.
- GOGC / GOMEMLIMIT: set from container memory policy and profiles — not folklore.
- Know when stack growth, mark assist, or syscalls explain a stall — then fix the code shape, not only knobs.

## Audit mode

Checklist:

- [ ] Is unsafe necessary or premature?
- [ ] Pointer escape / keep-alive correctness?
- [ ] cgo on request path without budget?
- [ ] GC flags changed without before/after metrics?

Red flags: `unsafe.Pointer` for “faster map”; finalizer instead of `Close`; copying mutex via unsafe; ignoring memory model for publication.

## Go specifics / anti-patterns

```text
Pass:  documented atomic.Pointer publish of immutable config; systems note in Decisions
Fail:  uintptr stored past use; string→[]byte via unsafe shared mutably
```

## Non-goals

- Not a full Go runtime textbook — refuse low-level surfaces without a measured reason.
- Not banning GOMEMLIMIT in containers — require a policy story.
