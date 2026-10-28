# otelx/server/grpc — the edge for a gRPC server you own

## Problem
A gRPC server with no interceptors has no spans, no protection against a
handler panic taking the process down, and no keepalive, so a layer-4
balancer keeps every client glued to the first pod. Shaffra owns no gRPC
server yet; `grpcx` is on the roadmap and this is its tracing and lifecycle
half. The `rpc.server.*` metrics are `metrics/grpc`.

## API in 10 lines
```go
ic, _ := servergrpc.New(servergrpc.Config{Logger: logger})                     // global tracer from otelx
ms, _ := metricsgrpc.NewServer(metricsgrpc.ServerConfig{})                     // metrics/grpc, chained after the tracing
srv, _ := servergrpc.NewServer(servergrpc.ServerConfig{Addr: ":9000", Interceptors: ic, Options: ms.ServerOptions(), Health: hc, Logger: logger})
pb.RegisterOrdersServer(srv.GRPC(), ordersService)
if err := srv.Listen(ctx); err != nil { return err }                            // busy port fails the boot
app.Add(lifecycle.Component{Name: "grpc", Run: srv.Run})
```

## What it emits
| Signal | Name | Attributes |
|--------|------|------------|
| server span | `pkg.Service/Method` | `rpc.system`, `rpc.service`, `rpc.method`, `client.address`, `rpc.grpc.status_code`, `error.type`; stream messages as `message` events |

Span status is Error only for the codes that mean the server failed
(`Unknown`, `DeadlineExceeded`, `Unimplemented`, `Internal`, `Unavailable`,
`DataLoss`); an `InvalidArgument` is the client's doing. A panic is logged
with its stack and answered with `Internal`.

## Defaults
| Parameter | Default | Why |
|-----------|---------|-----|
| `MaxConnectionAge` / grace | 5 m / 30 s | clients reconnect and spread after a scale-out |
| keepalive time / timeout | 2 m / 20 s | dead peers are noticed; idle connections stay cheap |
| `MinClientPingInterval` | 1 m | a client pinging faster is dropped (`PermitWithoutStream` on) |
| `MaxRecvMsgBytes` | 4 MiB | gRPC's own default, made explicit |
| `MaxConcurrentStreams` | 1024 | bounds one connection's share of the process |
| `ShutdownTimeout` | 15 s | `GracefulStop` then `Stop`; under the lifecycle budget |
| `Skip` | the health service | probes do not need spans |

## Composition
`Interceptors` go first in the chain; `ServerConfig.Options` is appended
after them, so `metricsgrpc.ServerOptions()` records its sample inside the
span and after the recover turned a panic into `Internal`.

## Health
`NewHealthServer(reg, interval)` answers `grpc.health.v1`: `""` and `ready`
from `health.Registry.Ready`, `live` from `Live`, anything else `NotFound`;
`Watch` streams the first status and every change. Kubernetes gRPC probes
and client-side health checking see the same readiness as `/healthz/ready`.

## Refuse list
- No TLS or auth policy: pass credentials in `Options`; the edge does not choose them.
- No reflection by default: `grpc/reflection` is one `Options` entry away.
- No message body on spans.
- No metrics here: one hook per signal, `metrics/grpc` records them.

## Proof
`servergrpc_test.go`: direct interceptor invocation (status mapping, panic
to `Internal`) and a real server on loopback: remote parent continued,
`client.address`, health `Check`/`Watch`/`NotFound`, stream message events,
clean `Run` shutdown.
