# 14 — Prove correctness

## Intent

Concurrency and lifecycle fixes are not done until **proven**: `-race`, goleak (when relevant), tests/gates that fail on the bug class, and honest repro notes.

## Author mode

- For any shared-memory or goroutine change: run `go test -race` on the package.
- For shutdown/worker code: goleak or explicit join assertions.
- Prefer tests that fail before the fix (regression) over mock theater.
- Integration: real Postgres/Redis/brokers via testcontainers when testing adapters ([architecture/09](../architecture/09-testing-fakes-integration.md)).
- Document how to repro: command lines in Decisions or PR text.

## Audit mode

Checklist:

- [ ] Concurrent change without `-race` evidence?
- [ ] “Fixed” leak without join/goleak proof?
- [ ] Flaky sleep-based tests masking races?
- [ ] Perf claim without bench numbers?

Red flags: “should be fine”; ignored `vet`; deleting the failing test to green CI.

## Go specifics / anti-patterns

```bash
go test ./... -race -count=1
# + goleak in TestMain / per-test where workers start
```

## Non-goals

- Not requiring goleak on every pure function package.
- Not replacing code review — proof complements lenses 01–13.
