# events/natsjs — NATS JetStream adapter

Replaces the hand-rolled JetStream client that auto-acked before processing,
kept an unbounded in-memory queue per subject, had no panic recovery, no
shutdown, logged the broker password at boot, and let every service rewrite
stream subjects from its own pinned list.

## Connect
```go
nc, err := natsjs.Connect(ctx, natsjs.Config{
    URL: cfg.NATS.URL, User: cfg.NATS.User, Password: cfg.NATS.Password, // never in the URL
    Name: "customers-api", Logger: log,
})
defer nc.Close(shutdownCtx) // drain: subscriptions stop, in-flight delivered, publishes flushed
ready.Add("nats", nc.Healthy)
```
`Connect` retries with backoff until `ctx` ends, so the startup budget is
the caller's. Reconnects are unlimited by default and logged without
credentials.

## Streams
```go
nc.EnsureStream(ctx, natsjs.StreamConfig{Name: "CUSTOMERS", Subjects: []string{"customers.>"},
    MaxAge: 7 * 24 * time.Hour, MaxBytes: 10 << 30, Replicas: 3})
nc.EnsureStream(ctx, natsjs.DeadLetterStream()) // "DLQ", dlq.>, 14 days
```
- Subjects are **merged**, never shrunk, unless the owner sets
  `ReplaceSubjects`. A service on an older contract cannot remove another
  service's subjects.
- Defaults: `MaxAge` 7 d, `Replicas` 1, `DuplicateWindow` 2 m, file storage.
  Set `MaxBytes` in production.
- One owner per stream (a migration job or the producing service).
  Consumers never call `EnsureStream`.

## Publish
```go
pub := nc.NewPublisher(natsjs.PublisherConfig{Timeout: 2 * time.Second, Observer: obs})
err := pub.Publish(ctx, e) // returns after the stream stored it; ErrNoStream if nothing captures the subject
```
`Nats-Msg-Id = e.ID`: a retried publish of the same envelope is deduplicated
by the server within the duplicate window. No retry loop inside: wrap with
`resilience/retry` for a short burst, or use the outbox for guarantees.

## Consume
```go
k, err := nc.Consume(ctx, natsjs.ConsumerConfig{
    Stream: "CUSTOMERS", Name: "projector", FilterSubjects: []string{"customers.customer.>"},
    Handler: handle, Middlewares: []events.Middleware{events.Inbox(inbox, "projector", obs)},
    Workers: 8, AckWait: 30 * time.Second, MaxDeliver: 5,
    DeadLetterSubject: natsjs.DeadLetterSubject("projector"), Observer: obs, Logger: log,
})
defer k.Stop(shutdownCtx) // before nc.Close
```

| Behaviour | Detail |
|-----------|--------|
| Ack | explicit, **after** the handler returns nil |
| Workers | `Workers` goroutines; the pull loop blocks when all are busy (backpressure), `MaxAckPending` = 2 × Workers |
| Long handlers | `InProgress` heartbeat every `AckWait/3`; a healthy 5-minute handler is not redelivered |
| Failure | `NakWithDelay` using `Backoff` per attempt (1 s, 5 s, 15 s, 30 s, ±20 % jitter) or the handler's `RetryAfter` |
| Poison | after `MaxDeliver` deliveries, or on `Drop`, or when the headers cannot be decoded: copied to `DeadLetterSubject` with `x-dlq-reason`, `x-dlq-error`, `x-dlq-consumer`, `x-dlq-stream`, `x-dlq-subject`, `x-dlq-deliveries`, then terminated |
| DLQ unavailable | the original is nak'd, not lost |
| Panic | recovered, logged with stack, nak'd |
| Stop | stops pulling, waits for in-flight handlers; if `ctx` ends first, cancels their context and still joins |

## Migration from `shaffra_events/pkg`
| Before | After |
|--------|-------|
| `client.Subscribe(subject, opts, func(payload []byte))` | `nc.Consume(ctx, ConsumerConfig{Handler: func(ctx, e) error})` — the payload is `e.Data` |
| `client.SubscribeWithManualAck` | same; return `nil` to ack |
| `client.Publish(subject, bytes)` | `pub.Publish(ctx, events.New(type, source, bytes, ...))` |
| `EnsureStream` in every service | once, by the owner; others `Consume` only |
| `Options{DurableName, QueueName, MaxDeliver}` | `ConsumerConfig{Name, MaxDeliver}`; one durable per logical consumer |

Subjects such as `customers.created` need a type `customers.customer.created.v1`.
Publish both during the migration, or map with `PublisherConfig.SubjectFor`.

## Proof
Embedded `nats-server` tests: connect retry, stream merge and replace,
round trip with dedup, nak then ack with `RetryAfter`, `Drop` → DLQ,
`MaxDeliver` → DLQ, undecodable → DLQ, panic recovery, heartbeat under a long
handler, stop drains and stop deadline cancels, publish without a stream.
