# resilience/otel — OpenTelemetry adapter for the Observer port

A separate module so the core stays SDK-free. Pinned to the OpenTelemetry
line that still builds with Go 1.25 (v1.45.x); bump it together with the
consumers' toolchain.

## What it exports
| Instrument | Type | Attributes |
|------------|------|------------|
| `resilience.decisions` | counter | `resilience.dependency`, `resilience.policy`, `resilience.decision` |
| `resilience.call.duration` | histogram (s) | same; recorded for events that carry an elapsed time |
| `resilience.breaker.state` | gauge 0/1/2 | `resilience.dependency` |
| `resilience.breaker.transitions` | counter | dependency, `resilience.breaker.from`, `.to` |
| `resilience.bulkhead.inflight` / `.capacity` | observable gauge | dependency (via `RegisterBulkhead`) |
| `resilience.workpool.queued` / `.capacity` / `.panics` | observable | dependency (via `RegisterPool`) |

Rejections and retries (never successes) are also added as a
`resilience.decision` event on the active span, with the attempt number.

The Prometheus exporter renders these as `resilience_decisions_total`,
`resilience_call_duration_seconds_bucket`, and so on.

## Wiring with a Prometheus `/metrics`
```go
exporter, _ := prometheus.New()                       // go.opentelemetry.io/otel/exporters/prometheus
mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))
otel.SetMeterProvider(mp)
obs, _ := resilienceotel.New(resilienceotel.Config{})  // uses the global provider
reg, _ := obs.RegisterBulkhead(fraudBulkhead); defer reg.Unregister()
http.Handle("/metrics", promhttp.Handler())
```
Pass `obs` as the `Observer` of every policy and preset.

## Dashboard starting points
- `rate(resilience_decisions_total{decision=~"breaker_open|bulkhead_full|limit_exceeded|shed"}[5m])` — policy-induced rejects (us protecting ourselves).
- `rate(resilience_decisions_total{policy="stack",decision=~"failure|timeout"}[5m])` — dependency errors.
- `resilience_breaker_state` — page the dependency owner when open **and** their error rate is up; page the platform when open and their error rate is flat.
