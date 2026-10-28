# otelx/client/nats — tracing for raw nats.go publish and subscribe

## Problem
The services publish and subscribe through the `shaffra_events` client with
`nats.Msg` directly: a message handled in another service starts a new
trace, and a handler panic kills the subscription.

`events/otel` covers the typed `events` path; this package covers code that
still works with `nats.Msg` and `jetstream.Msg`. Message counts and
durations are `metrics/nats`.

## API in 10 lines
```go
n, _ := clientnats.New(clientnats.Config{Consumer: "customers-usage", Logger: logger})
_ = n.PublishMsg(ctx, nc, &nats.Msg{Subject: "usage.stt", Data: payload})          // producer span, traceparent in the header
ack, err := n.PublishJetStream(ctx, js, msg)                                        // same, with the ack's stream on the span
sub, _ := nc.QueueSubscribe("usage.*", "customers", n.MsgHandler(func(ctx context.Context, m *nats.Msg) error { … }))
cc, _ := consumer.Consume(n.JetStreamMsgHandler(func(ctx context.Context, m jetstream.Msg) error { defer m.Ack(); … }))
// with metrics (metrics/nats), nested inside so the sample is in the span:
m, _ := metricsnats.New(metricsnats.Config{Consumer: "customers-usage"})
_ = m.Publish(ctx, msg.Subject, func(ctx context.Context) error { return n.PublishMsg(ctx, nc, msg) })
sub, _ = nc.QueueSubscribe("usage.*", "customers", n.MsgHandler(m.Handler(handle)))
```

## What it emits
| Signal | Name | Attributes |
|--------|------|------------|
| producer span | `<subject> publish` | `messaging.system=nats`, `messaging.destination.name`, `messaging.operation.type=send`, `messaging.message.id` (Nats-Msg-Id), `messaging.message.body.size`, `messaging.nats.stream`, `error.type` |
| consumer span | `<subject> process` | same plus `messaging.consumer.group.name`, `messaging.delivery.attempt`; `error.type=panic` for a recovered panic |

## Defaults
| Parameter | Default | Why |
|-----------|---------|-----|
| `Tracer`, `Propagator` | the globals from `otelx.New` | the hook works without being handed providers |
| `Logger` | discard | pass the service logger: a recovered panic is logged here with its stack |

## Refuse list
- No acknowledgement policy: the handler owns ack/nak; the wrapper observes.
- No retries or dead-lettering: that is `events/natsjs`.
- No payload on spans.
- No metrics: one hook per signal; `metrics/nats` nests inside.

## Proof
`clientnats_test.go` against an embedded nats-server: core publish and
subscribe share one trace with the process span a child of the publish
span; a handler error marks the span with `error.type`; JetStream
publish/consume with message id, stream and delivery attempt; a handler
panic is recovered, recorded and labelled; a failed publish marks its span.
