// Package eventspg stores events in PostgreSQL: a transactional outbox so an
// event is written in the same transaction as the business change and
// relayed afterwards, and an inbox so a consumer acknowledges a redelivered
// event without re-running its handler.
package eventspg

// Schema creates the outbox and inbox tables. Apply it with the service's
// migration tool; it is idempotent.
const Schema = `
CREATE TABLE IF NOT EXISTS event_outbox (
    id              text PRIMARY KEY,
    type            text NOT NULL,
    source          text NOT NULL,
    subject         text NOT NULL DEFAULT '',
    occurred_at     timestamptz NOT NULL,
    tenant_id       text NOT NULL DEFAULT '',
    correlation_id  text NOT NULL DEFAULT '',
    causation_id    text NOT NULL DEFAULT '',
    content_type    text NOT NULL DEFAULT '',
    data_schema     text NOT NULL DEFAULT '',
    trace_parent    text NOT NULL DEFAULT '',
    trace_state     text NOT NULL DEFAULT '',
    headers         jsonb NOT NULL DEFAULT '{}'::jsonb,
    data            bytea NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    attempts        int NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    claimed_by      text,
    claimed_at      timestamptz,
    published_at    timestamptz,
    dead            boolean NOT NULL DEFAULT false,
    last_error      text
);
-- The relay scans only unpublished, live rows; the partial index keeps that cheap as the table grows.
CREATE INDEX IF NOT EXISTS event_outbox_pending_idx
    ON event_outbox (next_attempt_at, id)
    WHERE published_at IS NULL AND dead = false;

CREATE TABLE IF NOT EXISTS event_inbox (
    consumer     text NOT NULL,
    event_id     text NOT NULL,
    status       text NOT NULL CHECK (status IN ('processing', 'done')),
    claimed_at   timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    PRIMARY KEY (consumer, event_id)
);
CREATE INDEX IF NOT EXISTS event_inbox_completed_idx ON event_inbox (completed_at) WHERE status = 'done';
`
