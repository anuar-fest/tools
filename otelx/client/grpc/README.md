# otelx/client/grpc — traced gRPC clients with keepalive

## Problem
Service-to-service gRPC calls with no interceptors are invisible: no span
joins the caller's trace, and no keepalive, so a dead connection is found
by the next request's deadline. The `rpc.client.call.duration` histogram
for the same calls is `metrics/grpc`.

## API in 10 lines
```go
ic, _ := clientgrpc.New(clientgrpc.Config{PeerService: "orders"})
mc, _ := metricsgrpc.NewClient(metricsgrpc.ClientConfig{PeerService: "orders"})   // metrics/grpc
opts := append(append(ic.DialOptions(), mc.DialOptions()...), grpc.WithTransportCredentials(creds))
cc, _ := grpc.NewClient("dns:///orders.svc:9000", opts...)
client := pb.NewOrdersClient(cc)
```
`DialOptions` adds the unary and stream interceptors and keepalive (30 s /
10 s). Credentials are yours to add. Append the metrics options after
these so the sample is recorded inside the span.

## What it emits
| Signal | Name | Attributes |
|--------|------|------------|
| client span | `pkg.Service/Method` | `rpc.system`, `rpc.service`, `rpc.method`, `server.address`, `server.port`, `peer.service`, `rpc.grpc.status_code`, `error.type`; messages as events |

On the client every non-OK status is an error. A stream's span ends once:
on a receive error (a clean `io.EOF` is success), a send error, or the
stream context ending.

## Defaults
| Parameter | Default | Why |
|-----------|---------|-----|
| `KeepaliveTime` / `KeepaliveTimeout` | 30 s / 10 s | above the server's `MinClientPingInterval` of 1 m would be too slow to notice a dead peer; 30 s is what the server edge permits |
| `PeerService` | none | set it per dependency |

## Refuse list
- No retries: `resilience` owns them (wrap the call in a stack; the span then covers all attempts).
- No credentials, no load-balancing policy.
- No metrics here: one hook per signal, `metrics/grpc` records them.

## Proof
`clientgrpc_test.go`: direct invocation injects `traceparent`, records
status and `peer.service` on the span; over the wire the server sees the
context and a canceled `Watch` ends its span with both message directions.
