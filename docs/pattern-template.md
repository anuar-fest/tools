# Pattern documentation template

Every package `README.md` follows this shape. Keep each section short; a reader
should be able to decide in two minutes whether to use the package and how.
The format mirrors the training material this repo grew out of: decision →
failure walkthrough → defaults → proof.

```markdown
# <package> — <one-line pattern name>

## Problem
What goes wrong without it. One concrete failure story (what the pod, the pool,
the dependency looked like).

## When to use / when not to
Bullets. Include the "do not wrap" cases: validation, pure CPU work, calls that
already sit inside another instance of this policy.

## API in 10 lines
A minimal, compilable example.

## Defaults
A table: parameter | default | why. Defaults must be safe for a money path.

## Composition order
Where this sits in the canonical stack
(`deadline → bulkhead → limit → retry(breaker(attempt-timeout(op)))`)
and what breaks if it is placed elsewhere.

## Failure modes
A table: situation | behaviour | what the caller sees (error) | metric/decision.

## Observability
Exact `Decision` values and metric names emitted through the Observer.

## Refuse list
Things this package will not do, with the reason (e.g. "no `Wait()` on a request
path: it converts overload into latency").

## Proof
Which tests prove the guarantees: race, goleak, fault-injection table.

## References
External sources the design leans on.
```

Rules:

- Explain **why**, not what. The code says what.
- Numbers live in tables, not prose.
- Every parameter named in the README exists in `Config`. Keep them in sync; the
  README is part of the review.
- If a package has a "Decisions & trade-offs" history worth keeping, add
  `DECISIONS.md` next to the README.
