# httpserver — the inbound HTTP edge without a framework

## Problem
Six `runServer.go` copies: `http.Server{}` with no timeouts (a slow client
holds a goroutine forever), `middleware.RealIP` trusting any
`X-Forwarded-For`, CORS `*`, recover that answers 500 after headers were
sent, request logs without a request id, a shutdown that returns before
in-flight requests finish and exits 0 when the port was busy.

## When to use / when not to
- Every service: `New` for the listener, the middleware chain around chi or `http.ServeMux`.
- Shedding and rate limiting come from `resilience/httpx`; tracing and metrics export from `otelx/server/http`.
- Not a router, not a JSON binding layer, not JWT (services keep their library; `APIKey` covers service-to-service).

## API in 10 lines
```go
mux := http.NewServeMux()                       // or chi; Route: func(r) string { return chi.RouteContext(r.Context()).RoutePattern() }
mux.Handle("GET /items/{id}", getItem)
apiKey, _ := httpserver.APIKey(httpserver.APIKeyConfig{Keys: map[string]string{"robot": cfg.RobotKey.Reveal()}})
h := chain(mux, httpserver.RealIP(httpserver.PrivateNetworks()), httpserver.RequestID(httpserver.RequestIDConfig{}),
    httpserver.Metrics(obs, nil), httpserver.AccessLog(httpserver.AccessLogConfig{Logger: logger}),
    httpserver.Recover(logger), serverhttp.Tracing(serverhttp.TracingConfig{}),
    httpx.Shed(shedder, nil, time.Second), httpserver.NoCache(), httpserver.MaxBytes(1<<20), httpserver.Timeout(30*time.Second), apiKey)
// chain(h, mws...) applies mws outermost-first; the mux itself is httpserver.Routed(mux)
srv, _ := httpserver.New(httpserver.Config{Addr: ":8080", Handler: h, Logger: logger})
if err := srv.Listen(ctx); err != nil { return err } // busy port fails the boot, not the first probe
app.Add(lifecycle.Component{Name: "http", Run: srv.Run})
```

## Defaults
| Parameter | Default | Why |
|-----------|---------|-----|
| `ReadHeaderTimeout` | 10 s | slowloris |
| `IdleTimeout` | 120 s | idle keep-alives hold file descriptors |
| `ReadTimeout` / `WriteTimeout` | 0 | uploads and downloads stream for minutes; bound per route with `MaxBytes` and `Timeout` |
| `MaxHeaderBytes` | 1 MiB | net/http default made explicit |
| `ShutdownTimeout` | 15 s | under the lifecycle budget |
| `RequestID` inbound | trusted if valid | one id from the gateway through every service |
| `RealIP` | no-op without trusted prefixes | a forwarding header from an untrusted peer is caller-supplied |
| `CORS` | fails on `*` + credentials | browsers refuse it anyway |
| `ParsePage` | limit 50, max 200 | a request for 1e6 rows is a 400, not a clamp |

## Composition order
```
RealIP → RequestID → Metrics → AccessLog → Recover → Tracing
→ httpx.Shed → httpx.RateLimit → CORS → NoCache → MaxBytes → Timeout → APIKey → Routed(mux)
```
`Routed` publishes the `ServeMux` pattern to the middleware outside it (a
middleware that copies the request would otherwise hide it). With chi,
register the chain inside the router with `r.Use` and pass
`Route: func(r) string { return chi.RouteContext(r.Context()).RoutePattern() }`.
| Placed elsewhere | What goes wrong |
|------------------|-----------------|
| Recover outside Metrics/AccessLog | a panic is neither counted nor logged as a 500 |
| Shed after APIKey | refusing costs a SHA-256 and a log line per request |
| MaxBytes after a body-reading middleware | the cap is bypassed |
| RealIP after RateLimit | the limiter keys on the proxy address |

All middleware share one `ResponseRecorder` per request (`Record`), so
status and byte counts agree and WebSocket upgrades (`Hijack`) still work.

## Failure modes
| Situation | Behaviour | Client sees |
|-----------|-----------|-------------|
| handler panics before writing | logged with stack, body headers dropped | 500 problem+json with `request_id` |
| handler panics after writing | logged, connection left to close | truncated body, original status |
| body over `MaxBytes` | 413 before reading, or `*http.MaxBytesError` on read (`WriteError` → 413) | 413 |
| `Timeout` reached | context canceled with `ErrRequestTimeout` (`WriteError` → 504) | 504 |
| client gone | nothing written; access log and metrics say 499 | — |
| wrong API key | constant-time mismatch, Warn log without the key | 401 + `WWW-Authenticate` |
| shutdown budget exceeded | remaining connections closed hard, error returned | reset |

## Observability
`Observer` port: `ObserveInFlight(±1)` and `ObserveRequest(RequestStat{Method,
Route, Status, Elapsed, BytesIn, BytesOut, Panicked})`. `otelx/server/http`
exports `http.server.request.duration`, `http.server.active_requests` and
body sizes. `AccessLog` writes one line per request; `LogAttrs` puts
`request_id` and `consumer` on every line logged with the request context.

## Refuse list
- No `http.TimeoutHandler`: it buffers the whole response and breaks streaming; `Timeout` sets a deadline the handler must honour.
- No error text from `WriteError`'s default branch: internal errors become a generic 500.
- No `Access-Control-Allow-Origin: *` with credentials.
- No request/response body logging.
- No JWT: keep the service's library; put it after `APIKey` in the chain.

## Proof
`server_test.go`: in-flight request completes across shutdown and `Run`
returns nil; busy port fails at `Listen`; budget overrun closes hard.
`middleware_test.go`: request id validation, panic before/after headers,
`ErrAbortHandler` propagation, access-log fields and levels, metrics
route/status/in-flight/499, trusted-proxy table, CORS preflight matrix,
API key matrix, declared and streamed body caps, timeout → 504, pagination
table, problem+json shape, flush and hijack pass-through.
