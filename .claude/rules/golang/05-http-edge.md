# 05 — HTTP edge hardening

## Intent

HTTP **server and client** lifecycles must be bounded: timeouts, body drain/close, middleware order, immutable shared clients, graceful shutdown that actually completes.

## Author mode

- Set server `ReadHeaderTimeout` / `ReadTimeout` / `WriteTimeout` / `IdleTimeout` deliberately.
- Shared `http.Client`: set `Timeout` or transport deadlines; treat client fields as immutable after first use.
- Always close or fully drain response bodies on egress; defer close once.
- Middleware onion: recover → logging → auth → handler; log auth failures; never `http.Error` after body write started.
- Recover in the same goroutine that serves the request.
- Graceful shutdown: `Shutdown(ctx)` with budget; ignore `http.ErrServerClosed` on `ListenAndServe`; still join background work ([01](01-goroutine-ownership-shutdown.md)).
- Proxies: know Hijacker/Flush; CORS and upstream errors must not leak internals.

## Audit mode

Checklist:

- [ ] Client without timeout?
- [ ] Double-close or unclosed body?
- [ ] Middleware order wrong (auth after handler side effects)?
- [ ] Shutdown leaves workers running?

Red flags: default `http.Get` in hot paths; mutating `client.Transport` after share; body logged as secrets.

## Go specifics / anti-patterns

```go
// Good
defer resp.Body.Close()
io.Copy(io.Discard, resp.Body) // when body unused but connection reuse needed

// Bad
http.Get(url) // no timeout, body leak on error paths
```

## Non-goals

- Not a full API design guide — see architecture contracts.
- Framework routers allowed if timeouts and body rules still hold.
