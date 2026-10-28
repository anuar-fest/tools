# events/pg — PostgreSQL outbox and inbox

## Outbox: no more `go publish()` after commit
```go
tx, _ := pool.Begin(ctx)
repo.CreateCustomer(ctx, tx, c)
eventspg.Enqueue(ctx, tx, events.New("customers.customer.created.v1", "customers-api", payload, events.WithTenant(t)))
tx.Commit(ctx) // the event exists if and only if the customer does

// one goroutine per replica (several replicas are fine: SKIP LOCKED + lease)
relay, _ := eventspg.NewRelay(pool, publisher, eventspg.RelayConfig{InstanceID: podName, Observer: obs, Logger: log})
go relay.Run(ctx)
```

| Parameter | Default | Note |
|-----------|---------|------|
| BatchSize | 100 | rows claimed per poll |
| PollInterval | 500 ms | when the outbox is empty |
| Lease | 30 s | a claimed row is reclaimable after this; keep > PublishTimeout × BatchSize |
| PublishTimeout | 5 s | per event |
| MaxAttempts | 10 | then `dead = true` for an operator (`Dead`, `Revive`) |
| Backoff | 1 s, 5 s, 30 s, 2 m, 10 m | jittered ±20 % |

Guarantees: a row is published by at most one live relay at a time (claim is
`FOR UPDATE SKIP LOCKED` in one statement); a crash between publish and
"mark published" republishes after the lease, which consumers absorb via the
inbox or `Nats-Msg-Id` dedup. Order is by id (UUID v7 → time) within one
relay. The partial index keeps the claim query cheap at any table size;
`Prune` deletes published rows older than a retention.

No `LISTEN/NOTIFY`: it needs a session, which transaction-mode PgBouncer
does not give. Polling at 500 ms is fine; lower it for latency-sensitive flows.

## Inbox: idempotent consumers
```go
inbox, _ := eventspg.NewInbox(pool, 5*time.Minute) // lease > handler timeout
h := events.Chain(handle, events.Inbox(inbox, "projector", obs))
```
Claim is one `INSERT … ON CONFLICT DO UPDATE … WHERE status='processing' AND
claimed_at < now() - lease RETURNING 1`: a done id is refused forever, a
stale processing claim (crashed worker) is taken over.

Strong form, inside the handler's transaction:
```go
tx, _ := pool.Begin(ctx)
if ok, err := eventspg.ClaimInTx(ctx, tx, "projector", e.ID); err != nil || !ok { return err } // !ok: already done
... business writes on tx ...
tx.Commit(ctx)
```
Effect and claim commit or roll back together: no window.

`PruneInbox` deletes done rows older than a retention; keep it longer than
the broker could ever redeliver.

## Schema
`eventspg.Schema` is idempotent DDL; apply it with the service's migration
tool. Two tables: `event_outbox`, `event_inbox`.

## Proof
Postgres 16 in testcontainers: transactional enqueue, field round trip,
backoff and death and revive, three concurrent relays over 300 rows publish
each exactly once, inbox lease semantics, in-transaction claim rolls back
with the transaction. `-short` skips the container.
