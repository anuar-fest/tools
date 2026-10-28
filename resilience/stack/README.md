# stack — the canonical composition

One `Stack` per dependency applies the module's policies in a fixed order:

```
deadline → bulkhead → limit → retry( breaker( attempt-timeout( op ) ) )
```

Services do not compose policies by hand; they pick a preset and override a
field with a reason. `depguard` can ban `http.Client{}` and ad-hoc wrappers.

## Presets
| Class | Timeout | Attempts | Attempt timeout | Backoff | Use for |
|-------|---------|----------|-----------------|---------|---------|
| `ReadFast` | 500ms | 3 | 200ms | 20–100ms | flags, caches, small internal APIs |
| `ReadSlow` | 5s | 2 | 2.5s | 100–500ms | searches, reports |
| `WriteIdempotent` | 3s | 3 | 1s | 100ms–1s | PUT, keyed POST |
| `WriteOnce` | 3s | 1 | 2.5s | — | unkeyed writes: no retry, breaker and bulkhead still apply |

Every preset includes a bulkhead (`MaxConcurrent` from `PresetOptions`,
default 32) and a breaker (`ErrorRate(10s, 20, 0.5)`, open 10 s). Reserve is
50 ms: the caller keeps that much of its deadline.

## Outcome mapping
`Outcome(err)` maps a final error to one decision: `success`, `bulkhead_full`,
`limit_exceeded`, `breaker_open`, `retry_budget`, `shed`, `timeout`,
`canceled`, `failure`. The stack reports it once per call with the total
elapsed time; inner policies report their own decisions with their own policy
label, so a rejected call shows up twice on purpose (policy view and call view).

## Refuse list
- No reordering. If the order is wrong for a case, that case is the exception that needs an ADR.
- No nesting of stacks.
- HTTP goes through `httpx`, not through `stack.DoValue` around `client.Do`: the response body outlives the attempt.

## Proof
`stack_test.go`: every attempt recorded by the breaker, hung dependency
trips, bulkhead/limiter reject before retry, call timeout bounds retries,
presets build runnable stacks.
