# 11 — Measure before mutate

## Intent

Performance changes need a **profile → budget → change** story. Optimization without evidence buys complexity forever.

## Author mode

- Name the metric: latency, throughput, or cost — and which one you are buying.
- Capture evidence (`pprof`, `testing.B`, allocs/op, pool wait) before micro-optimizing.
- Set an allocation/GC budget for money paths; refuse `sync.Pool` without reuse proof and lifetime clarity.
- Lock sharding, atomics, false-sharing fixes: only after contention shows up.
- Prefer algorithmic/structure fixes (right ADT, less work) before unsafe tricks ([12](12-runtime-surfaces-bar.md)).
- Document what you will pull back if the win is < noise.

## Audit mode

Checklist:

- [ ] Change justified by profile or load test?
- [ ] Benchmark honesty (wrong `b.N`, missing `ReportAllocs`)?
- [ ] `sync.Pool` of incorrect types / missing Put?
- [ ] Readability destroyed for <5% gain?

Red flags: “make it faster” PR with no numbers; GOGC twiddling as first move; pooling tiny short-lived objects blindly.

## Go specifics / anti-patterns

```text
Pass:  pprof CPU → hot JSON encode → switch encoder; bench shows -40% allocs
Fail:  sprinkle sync.Pool and atomics without a profile
```

## Non-goals

- Not anti-performance — anti-unmeasured complexity.
- Continuous profiling / CI regression gates encouraged for platform maturity.
