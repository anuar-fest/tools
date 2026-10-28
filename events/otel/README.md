# events/otel — tracing and metrics for events

```go
pub := eventsotel.TracedPublisher(natsPublisher, nil)            // producer span, traceparent into the envelope
k, _ := nc.Consume(ctx, natsjs.ConsumerConfig{..., Middlewares: []events.Middleware{
    eventsotel.Traced(nil, "projector"),                            // consumer span, child of the producer's
    events.Inbox(inbox, "projector", obs),
}})
obs, _ := eventsotel.NewObserver(eventsotel.Config{})              // pass as Observer to publishers and consumers
```

| Signal | Name | Attributes |
|--------|------|------------|
| span | `<type> publish` (producer) | `messaging.system`, `messaging.destination.name`, `messaging.message.id`, `messaging.message.conversation_id`, `tenant.id` |
| span | `<type> process` (consumer) | the same plus `messaging.consumer.group.name`, `messaging.delivery.attempt`, `messaging.drop` on failure |
| counter | `events.messages` | `events.consumer` (or source), `events.type`, `events.outcome` |
| histogram | `events.message.duration` (s) | same |

The trace context travels in `traceparent`/`tracestate` envelope fields;
`Inject`/`Extract` are exported for transports that need them directly.

Outcomes: `published publish_failed acked nacked dropped dead_lettered
duplicate panic timeout undecodable`. Alert on `dead_lettered` and on a
rising `nacked`/`acked` ratio per consumer.
