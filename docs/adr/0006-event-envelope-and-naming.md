# ADR 0006 — CloudEvents-shaped envelope, typed naming, at-least-once with inbox

**Status:** accepted · **Date:** 2026-10-16

## Context
Today an event is raw protobuf bytes on a subject; the type is implied by
the subject; there is no id header, no correlation, no tenant, no trace
context, no schema version. Two incompatible id styles exist across 80
messages. Subjects are named inconsistently. The client auto-acks before
processing and keeps an unbounded in-memory queue, so a crash loses
messages and a slow handler duplicates them. Streams are created by whoever
boots first, from that service's pinned subject list.

## Decision
- **Envelope.** Every message carries CloudEvents-style headers: `ce-id`
  (UUID v7), `ce-type`, `ce-source`, `ce-time`, `ce-tenantid`,
  `ce-correlationid`, `ce-causationid`, `content-type`, `ce-dataschema`,
  `traceparent`. The payload stays opaque bytes (JSON or protobuf).
- **Naming.** `{domain}.{entity}.{event}.v{major}`; lowercase; the major
  version is in the type. A breaking payload change is a new type.
- **Delivery.** At-least-once, explicit ack after the handler returns,
  bounded workers, heartbeat (`InProgress`) for long handlers, nak with
  backoff, `MaxDeliver` then dead-letter to a DLQ subject with the reason.
  `Drop(err)` dead-letters at once.
- **Idempotency.** The `Inbox` middleware (memory or Postgres) acknowledges
  duplicates without rerunning the handler.
- **Atomicity.** Publish-after-commit is a dual write; the Postgres outbox
  with a `SKIP LOCKED` relay is the supported path for events that must not
  be lost.
- **Stream ownership.** One owner (a migration job or the producing
  service) declares streams with explicit limits; `EnsureStream` merges
  subjects and never shrinks them; consumers never create streams.

## Consequences
- Consumers can dedupe, correlate and trace without decoding the payload.
- Existing proto messages keep working as payloads; the metadata moves to
  headers.
- `Nats-Msg-Id = ce-id` gives broker-side dedup within the duplicate window.

## Alternatives rejected
- **Metadata inside the payload** (the `EventMetadata` message). Requires
  decoding to route or dedupe; two id styles already exist. Rejected.
- **Exactly-once.** Not a property any broker gives end to end. Rejected;
  idempotent consumers instead.
