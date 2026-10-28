# coalesce — one fill per key under a stampede

## Problem
A hot cache key expires; 3 000 requests miss at once and 3 000 identical
queries hit the database. Coalescing runs the fill once and hands the
result to everyone who asked while it ran.

## API
```go
g := coalesce.New[Merchant](coalesce.Config{Name: "merchant", FillTimeout: 2 * time.Second})
m, err := g.Do(ctx, "tenant:"+id, func(ctx context.Context) (Merchant, error) {
    return repo.Load(ctx, id) // runs once; result shared
})
```

## Why the leader is detached
With `singleflight` as commonly used, the leader runs with its own request
context; when that caller gives up, the fill is cancelled and every waiter
gets the leader's `context.Canceled`. Here the fill runs on
`deadline.Detach(ctx, FillTimeout)`: values (trace, tenant) are kept,
cancellation is not, and `FillTimeout` bounds a stuck fill. Each waiter still
honours its own ctx.

## Rules
- Coalesce in the repository, next to the cache, not in the handler.
- Key by tenant and id; never by something a client controls unbounded.
- A panic in the fill becomes an error for all callers; the key is released.

## Proof
`coalesce_test.go` under `goleak`: 16 callers one fill, leader cancel does
not fail waiters, waiter honours its ctx, fill timeout, panic, `Forget`.
