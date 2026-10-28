# 09 — Async outbox and workers

## Intent

Async delivery must avoid **dual-write lies**: claim before send (or transactional outbox), poison handling, and drainable workers. At-least-once means consumers are idempotent.

## Author mode

- Persist intent and state in the same transaction as the business write (outbox row), then publish from a poller.
- Claim/lease rows before send; handle crash between claim and ack (retry with backoff).
- Poison messages: bounded retries → dead-letter / park with alert — not infinite hot loops.
- Workers: cancel + join on shutdown; do not lose in-flight claims without lease expiry.
- Consumers: idempotency keys; never assume exactly-once from Kafka/HTTP alone.

## Audit mode

Checklist:

- [ ] Business write + event write atomic?
- [ ] Poller races (double publish) prevented?
- [ ] Shutdown drains or leases expire safely?
- [ ] Backpressure when broker slow (bound queue)?

Red flags: `db.Exec` then `kafka.Publish` without outbox; fire-and-forget publish goroutine; no poison strategy.

## Go specifics / anti-patterns

```text
Pass:  BEGIN; update ledger; insert outbox; COMMIT; → poller sends → mark sent
Fail:  update ledger; go publish(); return 200
```

## Non-goals

- Not choosing Kafka vs HTTP — that is event/system design.
- Inbox/dedup patterns allowed; document the key.
