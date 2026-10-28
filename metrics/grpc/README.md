# metrics/grpc — rpc.server.* and rpc.client.* on the OpenTelemetry API

Pinned to OpenTelemetry v1.45.x and grpc-go 1.8x (what builds with Go
1.25). Spans, propagation, panic recovery and keepalive for the same calls
are `otelx/server/grpc` and `otelx/client/grpc`.

## Problem
A gRPC server with no interceptors has no latency by method and no count
of in-flight calls; clients with none cannot tell a slow dependency from a
slow network. Shaffra owns no gRPC server yet; this is the metrics half of
the `grpcx` it will need.

## API in 10 lines
```go
ms, _ := metricsgrpc.NewServer(metricsgrpc.ServerConfig{})                           // global MeterProvider from metrics.New
tr, _ := servergrpc.New(servergrpc.Config{Logger: logger})                            // otelx/server/grpc: spans and recovery
srv, _ := servergrpc.NewServer(servergrpc.ServerConfig{Addr: ":9000", Interceptors: tr, Options: ms.ServerOptions(), Health: hc})

mc, _ := metricsgrpc.NewClient(metricsgrpc.ClientConfig{PeerService: "orders"})
tc, _ := clientgrpc.New(clientgrpc.Config{PeerService: "orders"})                      // otelx/client/grpc: spans and keepalive
cc, _ := grpc.NewClient(target, append(append(tc.DialOptions(), mc.DialOptions()...), grpc.WithTransportCredentials(creds))...)
```

## What it emits
| Instrument | Type | Attributes |
|------------|------|------------|
| `rpc.server.call.duration` | histogram (s, `LatencyBuckets`) | `rpc.system`, `rpc.service`, `rpc.method`, `rpc.grpc.status_code`, `error.type` (the code's name) |
| `rpc.server.request.size`, `rpc.server.response.size` | histogram (By) | `rpc.system`, `rpc.service`, `rpc.method` |
| `rpc.server.active_requests` | up/down counter | — |
| `rpc.client.call.duration` | histogram (s, `LatencyBuckets`) | server attributes plus `server.address`, `server.port`, `peer.service` |

A stream is one sample, recorded when it ends. A handler panic is recorded
as `Internal` and re-raised, so the tracing interceptor outside still logs
the original stack and answers the client.

## Defaults
| Parameter | Default | Why |
|-----------|---------|-----|
| `Meter` | global provider | `metrics.New` set it; tests pass a manual reader |
| `Skip` | the health service | probes do not need series |
| `PeerService` | none | set it per dependency so one histogram serves every client |

## Composition
Tracing outer, metrics inner, on both sides: the sample is recorded with
the span in its context (exemplars) and after the tracing recover turned a
panic into `Internal`. `servergrpc.ServerConfig.Options` is appended after
the tracing chain, so `ms.ServerOptions()` lands in the right place; on
the client append `mc.DialOptions()` after `tc.DialOptions()`. Keepalive
parameters come from the tracing client; this package sets none.

## Refuse list
- No per-message counters on streams: one sample per stream; messages are span events.
- No retries, credentials or load-balancing policy.
- No message body sizes on the client: the server's two histograms already see every byte.

## Proof
`metricsgrpc_test.go`: direct unary invocation records one series per
status, `error.type` only on failures, request sizes for every call and a
response size only when one exists, active requests back to zero, a panic
recorded as `Internal` and re-raised; the health service skipped by
default; a real server and client on loopback: unary OK and NotFound, a
canceled `Watch` recorded once on both sides as `Canceled` with
`server.address` and `peer.service`.

## Dashboard starting points
- `histogram_quantile(0.99, sum by (le, rpc_service, rpc_method) (rate(rpc_server_call_duration_seconds_bucket[5m])))`
- `sum by (rpc_method, error_type) (rate(rpc_server_call_duration_seconds_count{error_type!=""}[5m]))`
- `rpc_server_active_requests` against `MaxConcurrentStreams`
