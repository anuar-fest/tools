# httpx — the HTTP edge: outbound transport, inbound shedding and limiting

## Outbound: `NewTransport`
An `http.RoundTripper` that composes `deadline → bulkhead → limit →
retry(breaker(attempt))` with what HTTP adds:

| Concern | Behaviour |
|---------|-----------|
| Repeatable requests | Retried only if the method is idempotent (GET, HEAD, OPTIONS, TRACE, PUT, DELETE) or carries `Idempotency-Key` (or `AllowNonIdempotentRetry`), **and** the body can be replayed (`GetBody` set, as for `bytes.Reader`/`strings.Reader` bodies). |
| Status classification | 502/503/504/429 are retryable; every 5xx is a breaker failure; 429 is not (`breaker.Ignore`); 4xx is a normal response. `Retry-After` is honoured up to `Retry.MaxRetryAfter`. |
| RoundTripper contract | A 5xx/429 that cannot be retried, or still fails after the last attempt, is returned as a **response** (with up to 64 KiB of the body), not an error. Policy rejections (`ErrBreakerOpen`, `ErrBulkheadFull`, `ErrRateLimited`) are errors. |
| Attempt timeout | Bounds dial + TLS + headers. It is a *cancel armed by a timer*, disarmed when headers arrive, so it cannot abort a body read that started in time. |
| Call timeout | A deadline over everything including the body read; cancelled when the body is closed. |
| Per-dependency transport | `NewBaseTransport` sets `MaxConnsPerHost` (matched to the bulkhead), `MaxIdleConnsPerHost` equal to it (net/http's default of 2 churns connections), 3 s dial and TLS timeouts, HTTP/2. Never share a transport between dependencies. |

```go
tr, _ := httpx.NewTransport(httpx.TransportConfig{
    Name: "fraud", CallTimeout: 2 * time.Second, AttemptTimeout: 500 * time.Millisecond,
    Bulkhead: bh, Breaker: br, Retry: retry.Policy{MaxAttempts: 3, Budget: budget}, Observer: obs,
})
client := &http.Client{Transport: tr} // no client.Timeout: the transport owns time
```

Defaults: `CallTimeout` 30 s, `AttemptTimeout` 10 s, `RetryStatuses` 502/503/504/429.
`Retry.AttemptTimeout` must be 0; use the transport field.

## Inbound: `Shed`, `RateLimit`, `Problem`
```go
r.Use(httpx.Shed(shedder, classify, time.Second))          // first: 503 + Retry-After
r.Use(httpx.RateLimit(perTenant, tenantKey))               // 429 + Retry-After
httpx.Problem(w, 503, "Service overloaded", detail, retryAfter) // RFC 9457 body
```
`Retry-After` is jittered by up to 20 % so refused clients do not return together.

## Proof
`transport_test.go` fault table: 503→200, exhausted retries return the last
response, streaming POST not retried / keyed POST retried, 500 trips the
breaker and is still returned, 429 ignored by the breaker and Retry-After
honoured, headers in time but body late, hung headers retried and tripping,
call timeout cuts a body read, bulkhead full. `middleware_test.go`: 503 and
429 contracts.
