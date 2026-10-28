# health — liveness vs readiness, drain flag, startup gate

## Problem
Six services expose one `/health` that returns 200 while the database is
gone, so Kubernetes keeps routing to a pod that fails every request; and
none goes unready before shutdown, so the last second of each deploy is a
burst of connection resets.

## When to use / when not to
- Readiness: the dependencies a request needs now (Postgres ping, NATS connected).
- Liveness: only inward signs (a wedged worker). Never a dependency: a database outage would restart the fleet.
- Call `SetDraining(true)` first thing on shutdown (`lifecycle.Config.OnShutdown`).
- Do not put business checks (queue depth, lag) here; they are metrics and alerts.

## API in 10 lines
```go
hc := health.New(health.Config{Info: map[string]string{"name": "customers", "version": version}, Logger: logger})
hc.AddReadiness("postgres", db.PingContext)                        // func(ctx) error fits directly
hc.AddReadiness("nats", health.Bool(nc.Healthy, "nats disconnected"))
admin.Handle("GET /healthz/live", hc.LiveHandler())
admin.Handle("GET /healthz/ready", hc.ReadyHandler())
app := lifecycle.New(lifecycle.Config{OnShutdown: func(string) { hc.SetDraining(true) }})
```

## Defaults
| Parameter | Default | Why |
|-----------|---------|-----|
| `Timeout` | 2 s | below the probe's `timeoutSeconds`; a slow ping is already a failure |
| `CacheTTL` | 1 s | several probers (kubelet, LB, mesh) cost one evaluation; negative disables |
| `StartupGate` | off | readiness fails until `MarkStarted` when on |

## Failure modes
| Situation | Behaviour | Response |
|-----------|-----------|----------|
| a check returns an error | `fail` with the error text | 503 |
| a check ignores ctx past `Timeout` | reported as timeout, left running | 503 |
| a check panics | `fail: panic: …` | 503 |
| draining | nothing runs | 503 `draining` |
| startup gate closed | nothing runs | 503 `starting` |

## Observability
Readiness transitions log at Warn (`readiness lost`, with the failed checks)
and Info (`readiness restored`). Probe handlers set `Cache-Control: no-store`.

## Refuse list
- No HTTP/TCP check helpers: a dependency check with a URL is an SSRF and a cardinality hazard; write the three lines yourself.
- No metrics from this package: readiness failures are visible in events and alerts on the dependency itself.

## Proof
`health_test.go`: per-check reporting and status codes, concurrent checks
under one deadline (a stuck check does not delay the others), panic
containment, draining and startup gate without running checks, 50
concurrent probes cost one evaluation, duplicate names refused.

## Kubernetes
```yaml
readinessProbe: {httpGet: {path: /healthz/ready, port: admin}, periodSeconds: 5, timeoutSeconds: 3, failureThreshold: 2}
livenessProbe:  {httpGet: {path: /healthz/live,  port: admin}, periodSeconds: 10, timeoutSeconds: 3, failureThreshold: 3}
lifecycle: {preStop: {exec: {command: ["sleep", "5"]}}}
terminationGracePeriodSeconds: 35   # preStop + lifecycle.ShutdownTimeout + margin
```
