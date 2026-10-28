# otelx/server/http — route-named server spans for the HTTP server you own

A separate module so `httpserver` stays SDK-free. Pinned to OpenTelemetry
v1.45.x (the last line that builds with Go 1.25). The `http.server.*`
metrics for the same requests are `metrics/http`'s Observer.

## Problem
A request that failed in the customers service and the span the fraud
service exported for it could not be joined: nothing extracted the
caller's trace context, and the span, when there was one, was named after
the raw path.

## Tracing
`Tracing` runs each request in a server span: extracts W3C trace context,
starts `GET`, renames to `GET /items/{id}` after the router matched, sets
`http.response.status_code`, marks 5xx and panics as errors, and echoes the
trace id in `X-Trace-Id` so a support ticket can quote it. `PublicEndpoint`
starts a new trace linked to the caller's instead of continuing it.

Written on the API instead of wrapping otelhttp: the route is only known
after the handler ran, and the status comes from the one
`ResponseRecorder` every `httpserver` middleware shares.

## Wiring
```go
obs, _ := metricshttp.NewObserver(metricshttp.Config{})            // metrics/http, global MeterProvider
chain := httpserver.Metrics(obs, route)(httpserver.AccessLog(cfg)(
    serverhttp.Tracing(serverhttp.TracingConfig{Route: route, Skip: isProbe})(
        httpserver.Recover(logger)(mux))))
```
Put `Tracing` outside `Recover` (the panic becomes a 500 on the span) and
inside `Metrics`/`AccessLog`, so the duration sample is recorded with the
span in its context and carries it as an exemplar.

## Defaults
| Parameter | Default | Why |
|-----------|---------|-----|
| `Route` | `httpserver.PatternRoute` | ServeMux patterns; chi passes its own `RouteFunc` |
| `Skip` | none | pass the probe and scrape paths |
| `NoTraceIDHeader` | off | support tickets quote the id |
| `PublicEndpoint` | off | internal callers' trace context is trusted |

## Refuse list
- No query string on spans: that is where tokens travel.
- No metrics here: one hook per signal, `metrics/http` records them.

## Proof
`serverhttp_test.go`: remote parent continued, span renamed after the
route, attributes, no query string on spans, panic → error status and
`error.type=panic`, public endpoint link, skip and header switches.
