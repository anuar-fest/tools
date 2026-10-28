# metrics/nats — messages, failures and durations for raw nats.go

## Problem
The services publish and subscribe through the `shaffra_events` client with
`nats.Msg` directly: nobody knows how many messages a subject carries, how
many handlers fail, how long they take, or whether the connection is up.

`events/otel` covers the typed `events` path; this package covers code that
still works with `nats.Msg` and `jetstream.Msg`. Spans are
`otelx/client/nats`.

## When to use / when not to
- One `Instrumentation` per subscriber (its `Consumer` name) and one for publishing.
- Metrics only: `MsgHandler` / `JetStreamMsgHandler` guard the subscriber themselves.
- Both signals: nest `Handler` / `JetStreamHandler` inside the tracing wrappers and `Publish` around the tracing publish, so the sample lands inside the span (exemplars).
- Not for `events/natsjs` consumers: they emit through the `events` observer.

## API in 10 lines
```go
m, _ := metricsnats.New(metricsnats.Config{Consumer: "customers-usage", Logger: logger})   // global meter from metrics.New
_ = m.PublishMsg(ctx, nc, &nats.Msg{Subject: "usage.stt", Data: payload})                 // metrics only
sub, _ := nc.QueueSubscribe("usage.*", "customers", m.MsgHandler(func(ctx context.Context, msg *nats.Msg) error { … }))
// with tracing (otelx/client/nats):
tr, _ := clientnats.New(clientnats.Config{Consumer: "customers-usage", Logger: logger})
_ = m.Publish(ctx, msg.Subject, func(ctx context.Context) error { return tr.PublishMsg(ctx, nc, msg) })
sub, _ = nc.QueueSubscribe("usage.*", "customers", tr.MsgHandler(m.Handler(handle)))
cc, _ := consumer.Consume(tr.JetStreamMsgHandler(m.JetStreamHandler(handleJS)))
reg, _ := metricsnats.RegisterConnStats(nil, nc); defer reg.Unregister()
```

## What it emits
| Instrument | Type | Attributes |
|------------|------|------------|
| `messaging.client.sent.messages` | counter | `messaging.system=nats`, `messaging.destination.name`, `messaging.operation.type=send`, `error.type` |
| `messaging.client.consumed.messages` | counter | same with `messaging.operation.type=process`, plus `messaging.consumer.group.name`; `error.type=panic` for a recovered panic |
| `messaging.client.operation.duration` | histogram (s, `LatencyBuckets`) | same as the counter it accompanies |
| `messaging.nats.messages{direction}`, `.bytes{direction}`, `.reconnects`, `.connected` | counters, gauge | `messaging.client.id` |

Set `DestinationName` when subjects embed ids: the default label is the
subject itself, and `orders.123.created` would be a series per order.

## Defaults
| Parameter | Default | Why |
|-----------|---------|-----|
| `Meter` | global meter named `InstrumentationName` | works wherever `metrics.New` ran |
| `DestinationName` | identity | subjects are usually a closed set; opt in when they are not |
| `Logger` | discard | only the metrics-only wrappers log; the tracing hook logs in the nested setup |

## Failure modes
| Situation | `Handler` / `JetStreamHandler` (nested) | `MsgHandler` / `JetStreamMsgHandler` (metrics only) |
|-----------|------------------------------------------|------------------------------------------------------|
| handler returns an error | sample with `error.type`, error returned | sample with `error.type` |
| handler panics | sample with `error.type=panic`, panic re-raised for the outer guard | sample with `error.type=panic`, panic logged with its stack, subscriber survives |
| publish fails | `send` sample with `error.type`, error returned | same |

## Refuse list
- No acknowledgement policy: the handler owns ack/nak; the wrappers observe.
- No retries or dead-lettering: that is `events/natsjs`.
- No message id or payload size as labels: those are span attributes.

## Proof
`metricsnats_test.go` against an embedded nats-server: sent and consumed
counters and durations per subject and outcome with the consumer label;
`Publish` wraps any function and `DestinationName` collapses ids; `Handler`
records a panic and re-raises it without logging; the JetStream
metrics-only wrapper recovers, logs and records; connection stats.

## Dashboard starting points
- `sum by (messaging_destination_name) (rate(messaging_client_consumed_messages_total{error_type!=""}[5m]))` — failing handlers per subject
- `histogram_quantile(0.99, sum by (le, messaging_destination_name) (rate(messaging_client_operation_duration_seconds_bucket{messaging_operation_type="process"}[5m])))`
- `messaging_nats_connected == 0` — alert; `rate(messaging_nats_reconnects_total[15m])` — flapping
