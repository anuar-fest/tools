# ratelimit — token buckets that only answer Allow

## Problem
One tenant's batch job sends 5 000 requests per second; the other 400
tenants share what is left. Or: a card network allows 100 TPS and bills
overages; every retry storm costs money. Limits belong at the ingress (per
key), at the egress (per dependency) and at the abuse layer (per IP), each
separately.

## API
```go
egress, _ := ratelimit.NewBucket(ratelimit.BucketConfig{Rate: ratelimit.PerSecond(100), Burst: 100})
if !egress.Allow() { return resilience.ErrRateLimited }

perTenant, _ := ratelimit.NewKeyed(ratelimit.KeyedConfig{Name: "ingress", Rate: ratelimit.PerSecond(50), Burst: 100, TTL: 10 * time.Minute, MaxKeys: 100_000})
ok, retryAfter := perTenant.Allow(tenantID)  // → 429 + Retry-After via httpx.RateLimit
```

## Defaults
| Parameter | Default | Why |
|-----------|---------|-----|
| Burst | required | Keep ≤ 2× rate; a large burst defeats shaping. |
| TTL (keyed) | 10m | Idle keys are swept lazily; no goroutine. |
| MaxKeys (keyed) | 100 000 | Bounded memory; beyond it idle keys are swept, then one random key is evicted. |

## Refuse list
- No `Wait()`. A limiter that sleeps turns overload into latency and then into timeouts; refuse and let the client back off.
- No distributed limiter here. A Redis-backed sliding window with fail-to-local is a later module; local limits are the floor it falls back to.

## Proof
`ratelimit_test.go`: burst then refill with a fake clock, all-or-nothing
`AllowN`, Retry-After, key isolation, TTL sweep and MaxKeys eviction.
