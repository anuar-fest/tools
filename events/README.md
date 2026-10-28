# events — event-driven messaging core

`github.com/anuar-fest/tools/events` is the transport-agnostic half of
event-driven messaging: the envelope, the ports, the acknowledgement
semantics and the handler middleware. It depends on the standard library
only. Transports and stores are nested modules:

| Module | What |
|--------|------|
| `events` (this) | `Envelope`, `Publisher`/`Handler`/`Consumer` ports, `Drop`/`RetryAfter`, middleware (`Recover`, `Timeout`, `Observe`, `Logging`, `Inbox`), `MemoryInbox`, JSON codec, type naming. |
| [`events/natsjs`](natsjs/) | NATS JetStream: connection, streams, publisher with dedup, bounded consumer with heartbeats, backoff and dead-lettering. |
| [`events/pg`](pg/) | PostgreSQL transactional outbox with a `SKIP LOCKED` relay, and an inbox (middleware form and in-transaction form). |
| [`events/otel`](otel/) | W3C trace context through the envelope; producer and consumer spans; message metrics. |

## The envelope

```go
e := events.New("customers.customer.created.v1", "customers-api", payload,
    events.WithTenant(tenantID), events.WithSubject("customer/"+id))
// inside a handler, for a follow-up event:
next := events.Derive(e, "customers.account.created.v1", "customers-api", payload)
```

| Field | Header | Why it exists |
|-------|--------|---------------|
| `ID` (UUID v7) | `ce-id`, `Nats-Msg-Id` | broker-side dedup within the duplicate window; inbox key |
| `Type` | `ce-type` | the subject; `{domain}.{entity}.{event}.v{major}`, validated |
| `Source` | `ce-source` | the producing service |
| `Subject` | `ce-subject` | the aggregate (`customer/42`), for ordering and debugging |
| `Time` | `ce-time` | when it happened, not when it was sent |
| `TenantID` | `ce-tenantid` | every multi-tenant bug starts with a missing tenant |
| `CorrelationID` / `CausationID` | `ce-correlationid` / `ce-causationid` | the whole flow / the direct cause |
| `ContentType`, `DataSchema` | `content-type`, `ce-dataschema` | how to decode `Data`; the minor schema version |
| `TraceParent`, `TraceState` | `traceparent`, `tracestate` | the trace continues across the broker |

Metadata travels in **headers**, so routing, dedup, tenancy and tracing never
decode the payload. Existing protobuf messages keep working as `Data`.

## Acknowledgement is the return value

```go
func handle(ctx context.Context, e events.Envelope) error {
    var p CustomerCreated
    if err := events.Decode(events.JSON{}, e, &p); err != nil { return err } // a Drop: bytes will not change
    if err := repo.Upsert(ctx, p); errors.Is(err, ErrRateLimited) {
        return events.RetryAfter(err, 10*time.Second)                       // nak with that delay
    } else if err != nil {
        return err                                                           // nak with the consumer's backoff
    }
    return nil                                                               // ack
}
```

| Return | Transport does |
|--------|----------------|
| `nil` | ack |
| `err` | redeliver after the consumer's backoff for this attempt |
| `events.RetryAfter(err, d)` | redeliver after `d` |
| `events.Drop(err)` | never redeliver; dead-letter with the reason |
| panic | recovered by the consumer; treated as `err` |

## Middleware

```go
h := events.Chain(handle,
    events.Logging(log),                 // outermost
    events.Timeout(30*time.Second),
    events.Inbox(inbox, "customers-projector", obs),
)
```
`Recover` is applied by every transport; `Observe` is applied by the
transports too, so most consumers need only `Inbox` and `Timeout`.

## Idempotency and atomicity

- **Inbox.** `events.Inbox(store, consumer, obs)` claims the event id before
  the handler and completes it after; a redelivery of a completed id is
  acked without running the handler. `MemoryInbox` for tests and single
  replicas; `eventspg.Inbox` for real. The crash window between effect and
  `Complete` still redelivers: handlers remain idempotent by design.
- **Strong form.** `eventspg.ClaimInTx` inside the handler's own transaction
  makes the claim and the effect atomic.
- **Outbox.** Publishing after commit is a dual write. `eventspg.Enqueue`
  in the business transaction plus `eventspg.Relay` is the supported path for
  events that must not be lost.

## Naming

`{domain}.{entity}.{event}.v{major}`, lowercase with underscores:
`customers.customer.created.v1`, `pmo_tasks.task.completed.v1`. A breaking
payload change is a new major version, published alongside the old one
until every consumer moved. Streams and subject filters derive from the
prefix (`customers.>`).

## Proof
`go test -race` on the core (envelope round trip through upper-cased
headers, UUID v7 ordering, middleware order, inbox lease and retention);
embedded `nats-server` tests in `natsjs`; a Postgres container in `pg`.
