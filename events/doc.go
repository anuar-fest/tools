// Package events is the transport-agnostic core of event-driven messaging:
// a CloudEvents-shaped Envelope with the identifiers that make redelivery,
// tracing and multi-tenancy work; Publisher and Handler ports; acknowledgement
// semantics expressed through errors; handler middleware (recover, timeout,
// observe, idempotent inbox); and the naming rule for event types.
//
// Transports (NATS JetStream in events/natsjs) and stores (Postgres outbox
// and inbox in events/pg) live in nested modules so this package stays free
// of SDKs.
//
// Delivery is at-least-once everywhere. Consumers must be idempotent; the
// Inbox middleware gives them that for the price of one row per event.
package events
