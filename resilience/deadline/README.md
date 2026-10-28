# deadline — per-hop budgets and detaching after the commit

## Problem
A handler has 2 s. It calls the ledger with the same 2 s. The ledger times
out at 2 s and the handler has 0 ms left to record the failure, so the
client gets a connection reset and support gets a ticket. The reverse
mistake: after the money moved, the client disconnects, the request
context is cancelled, and the receipt is never sent.

## API
```go
ctx, cancel := deadline.Child(ctx, 500*time.Millisecond, 50*time.Millisecond) // own cap, parent minus reserve
defer cancel()

if !deadline.Enough(ctx, 100*time.Millisecond) { return ErrNoBudget } // refuse rather than start and fail

bg, cancel := deadline.Detach(ctx, 5*time.Second) // after commit: keeps trace/tenant values, drops cancel
defer cancel()
go sendReceipt(bg, order)
```

## Rules
| Function | Semantics |
|----------|-----------|
| `Child(ctx, timeout, reserve)` | min(now+timeout, parent − reserve). Already expired with cause `ErrNoBudget` if the parent has less than `reserve` left. |
| `Attempt(ctx, d)` | A per-attempt deadline whose cause is `ErrAttemptTimeout` (retryable). |
| `Detach(ctx, budget)` | `context.WithoutCancel` plus a budget; values survive, cancellation does not. |
| `Remaining` / `Enough` | Read the budget before starting work. |

## Refuse list
- No `context.Background()` on a request path "to be safe". Use `Detach` with a budget and a reason.
- No child timeout longer than the parent: it is silently the parent's anyway.

## Proof
`deadline_test.go`: clamp, reserve, no-budget cause, detached values and
cancellation, attempt cause.

## References
AWS Builders' Library, *Timeouts*; Go blog, *Context and structured concurrency*.
