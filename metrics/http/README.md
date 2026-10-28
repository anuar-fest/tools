# metrics/http — HTTP server and client metrics on the OpenTelemetry API

A separate module so `httpserver` stays SDK-free. Pinned to OpenTelemetry
v1.45.x (the last line that builds with Go 1.25). Spans for the same
requests come from `otelx/server/http` and `otelx/client/http`.

## Problem
Six services measured HTTP with six promauto copies: one by raw path (a
series per customer id), one without the route at all, none with the
request body size. Outbound calls were not measured, so a slow fraud check
looked like slow business code.

## API in 10 lines
```go
obs, _ := metricshttp.NewObserver(metricshttp.Config{})           // global MeterProvider from metrics.New
chain := httpserver.Metrics(obs, route)(httpserver.AccessLog(cfg)(
    serverhttp.Tracing(serverhttp.TracingConfig{Route: route})(
        httpserver.Recover(logger)(mux))))

mt, _ := metricshttp.Transport(httpxTransport, metricshttp.TransportConfig{PeerService: "fraud"})
client := &http.Client{Transport: clienthttp.Transport(mt, clienthttp.Config{PeerService: "fraud"})}
```

## What it emits
| Instrument | Type | Attributes |
|------------|------|------------|
| `http.server.request.duration` | histogram (s, `LatencyBuckets`) | `http.request.method`, `http.route`, `http.response.status_code`, `error.type` (5xx code or `panic`) |
| `http.server.active_requests` | up/down counter | — |
| `http.server.request.body.size` | histogram (By) | same, when `Content-Length` was declared |
| `http.server.response.body.size` | histogram (By) | same |
| `http.client.request.duration` | histogram (s, `LatencyBuckets`) | `http.request.method`, `server.address`, `peer.service`, `http.response.status_code`, `error.type` (status ≥ 400 or the transport error's class) |

Prometheus renders `http_server_request_duration_seconds_bucket{http_route="GET /items/{id}",…}`.
Route is a pattern, never a path; the client side never records the URL.

## Defaults
| Parameter | Default | Why |
|-----------|---------|-----|
| `Meter` | global provider | `metrics.New` set it; tests pass a manual reader |
| buckets | `metrics.LatencyBuckets` | server and client p99 panels line up |
| `PeerService` | none | without it the host is the only identity; set it per dependency |

## Composition
- Server: `httpserver.Metrics(obs, route)` outermost, so a panic recovered
  deeper still counts as a 500 with `error.type=panic`; `serverhttp.Tracing`
  inside it; `Recover` inside that.
- Client: `clienthttp.Transport(metricshttp.Transport(httpx, …), …)`. The
  metric transport sits inside the tracing one so the sample carries the
  span as an exemplar, and outside `httpx` so retries are one sample.

## Refuse list
- No path or query on any metric, with no option to add them: that is what `Guard` is for.
- No per-status counter: the histogram's `_count` by status is the same number.
- No body-read timing on the client: the transport returns at the headers.

## Proof
`metricshttp_test.go`: the middleware chain records one series per
attribute set, route on the panic series, active requests back to zero,
response sizes; attribute table for 503 with route and 200 without; the
transport records one sample for a 503 and one for a transport error,
never `url.full`; nil `next` falls back to the default transport.

## Dashboard starting points
- `histogram_quantile(0.99, sum by (le, http_route) (rate(http_server_request_duration_seconds_bucket[5m])))`
- `sum by (http_route) (rate(http_server_request_duration_seconds_count{error_type!=""}[5m]))` — 5xx and panics per route
- `http_server_active_requests` against the `httpx.Shed` limit
- `histogram_quantile(0.99, sum by (le, peer_service) (rate(http_client_request_duration_seconds_bucket[5m])))`
