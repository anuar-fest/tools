package eventspg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/anuar-fest/tools/events"
)

// Execer is what Enqueue needs: a pgx.Tx or a pool.
type Execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// Enqueue writes e to the outbox inside the caller's transaction. Commit the
// transaction and the relay publishes it; roll back and it never existed.
func Enqueue(ctx context.Context, tx Execer, e events.Envelope) error {
	if err := e.Validate(); err != nil {
		return err
	}
	headers, err := json.Marshal(e.Headers)
	if err != nil {
		return fmt.Errorf("eventspg: encode headers: %w", err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO event_outbox (id, type, source, subject, occurred_at, tenant_id, correlation_id, causation_id,
		                          content_type, data_schema, trace_parent, trace_state, headers, data)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`,
		e.ID, e.Type, e.Source, e.Subject, e.Time.UTC(), e.TenantID, e.CorrelationID, e.CausationID,
		e.ContentType, e.DataSchema, e.TraceParent, e.TraceState, headers, e.Data)
	if err != nil {
		return fmt.Errorf("eventspg: enqueue %s: %w", e.Type, err)
	}
	return nil
}

// RelayConfig configures the outbox relay.
type RelayConfig struct {
	// InstanceID names this relay in claims (pod name). Required.
	InstanceID string
	// BatchSize rows per claim (default 100).
	BatchSize int
	// PollInterval when the outbox is empty (default 500ms).
	PollInterval time.Duration
	// Lease after which another relay may reclaim a row (default 30s). It
	// must exceed PublishTimeout × BatchSize in the worst case.
	Lease time.Duration
	// PublishTimeout per event (default 5s).
	PublishTimeout time.Duration
	// MaxAttempts before a row is marked dead for an operator (default 10).
	MaxAttempts int
	// Backoff between attempts, jittered; the last value repeats
	// (default 1s, 5s, 30s, 2m, 10m).
	Backoff  []time.Duration
	Observer events.Observer
	Logger   *slog.Logger
}

func (c RelayConfig) withDefaults() (RelayConfig, error) {
	if c.InstanceID == "" {
		return c, errors.New("eventspg: relay InstanceID is required")
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 100
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 500 * time.Millisecond
	}
	if c.Lease <= 0 {
		c.Lease = 30 * time.Second
	}
	if c.PublishTimeout <= 0 {
		c.PublishTimeout = 5 * time.Second
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 10
	}
	if len(c.Backoff) == 0 {
		c.Backoff = []time.Duration{time.Second, 5 * time.Second, 30 * time.Second, 2 * time.Minute, 10 * time.Minute}
	}
	c.Observer = events.OrNop(c.Observer)
	if c.Logger == nil {
		c.Logger = slog.New(slog.DiscardHandler)
	}
	return c, nil
}

// Relay moves outbox rows to a Publisher. Several relays may run against one
// table: claims use FOR UPDATE SKIP LOCKED and a lease, so no row is
// published twice by two live relays. Ordering is by id (UUID v7, so by
// time) within one relay; across relays it is not guaranteed.
type Relay struct {
	pool *pgxpool.Pool
	pub  events.Publisher
	cfg  RelayConfig
}

// NewRelay validates cfg.
func NewRelay(pool *pgxpool.Pool, pub events.Publisher, cfg RelayConfig) (*Relay, error) {
	if pool == nil || pub == nil {
		return nil, errors.New("eventspg: relay needs a pool and a publisher")
	}
	cfg, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	return &Relay{pool: pool, pub: pub, cfg: cfg}, nil
}

// Run relays until ctx ends. It returns ctx.Err(); transient database errors
// are logged and retried after PollInterval.
func (r *Relay) Run(ctx context.Context) error {
	for {
		n, err := r.RelayBatch(ctx)
		if err != nil && ctx.Err() == nil {
			r.cfg.Logger.Error("outbox relay batch failed", "err", err)
		}
		if n > 0 && err == nil {
			continue
		}
		t := time.NewTimer(r.cfg.PollInterval)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}
}

type outboxRow struct {
	env      events.Envelope
	attempts int
}

// RelayBatch claims up to BatchSize due rows, publishes them in id order and
// records the outcome. It returns how many rows were claimed.
func (r *Relay) RelayBatch(ctx context.Context) (int, error) {
	rows, err := r.claim(ctx)
	if err != nil {
		return 0, err
	}
	for _, row := range rows {
		if ctx.Err() != nil {
			return len(rows), ctx.Err()
		}
		r.publishOne(ctx, row)
	}
	return len(rows), nil
}

func (r *Relay) claim(ctx context.Context) ([]outboxRow, error) {
	q, err := r.pool.Query(ctx, `
		UPDATE event_outbox o SET claimed_by = $1, claimed_at = now()
		WHERE o.id IN (
		    SELECT id FROM event_outbox
		    WHERE published_at IS NULL AND dead = false AND next_attempt_at <= now()
		      AND (claimed_at IS NULL OR claimed_at < now() - $2::interval)
		    ORDER BY id
		    LIMIT $3
		    FOR UPDATE SKIP LOCKED
		)
		RETURNING o.id, o.type, o.source, o.subject, o.occurred_at, o.tenant_id, o.correlation_id, o.causation_id,
		          o.content_type, o.data_schema, o.trace_parent, o.trace_state, o.headers, o.data, o.attempts`,
		r.cfg.InstanceID, r.cfg.Lease, r.cfg.BatchSize)
	if err != nil {
		return nil, fmt.Errorf("eventspg: claim: %w", err)
	}
	defer q.Close()

	var out []outboxRow
	for q.Next() {
		var row outboxRow
		var headers []byte
		if err := q.Scan(&row.env.ID, &row.env.Type, &row.env.Source, &row.env.Subject, &row.env.Time, &row.env.TenantID,
			&row.env.CorrelationID, &row.env.CausationID, &row.env.ContentType, &row.env.DataSchema, &row.env.TraceParent,
			&row.env.TraceState, &headers, &row.env.Data, &row.attempts); err != nil {
			return nil, fmt.Errorf("eventspg: scan: %w", err)
		}
		if len(headers) > 0 && string(headers) != "null" {
			if err := json.Unmarshal(headers, &row.env.Headers); err != nil {
				return nil, fmt.Errorf("eventspg: decode headers of %s: %w", row.env.ID, err)
			}
		}
		out = append(out, row)
	}
	if err := q.Err(); err != nil {
		return nil, fmt.Errorf("eventspg: claim rows: %w", err)
	}
	// The claim is visible to other relays only once Query is fully consumed
	// in autocommit mode, so rows are ordered by id as the statement returned them.
	return out, nil
}

func (r *Relay) publishOne(ctx context.Context, row outboxRow) {
	pctx, cancel := context.WithTimeout(ctx, r.cfg.PublishTimeout)
	err := r.pub.Publish(pctx, row.env)
	cancel()

	if err == nil {
		if _, dbErr := r.pool.Exec(ctx, `UPDATE event_outbox SET published_at = now(), claimed_by = NULL, last_error = NULL WHERE id = $1`, row.env.ID); dbErr != nil {
			// Published but not recorded: the lease expires and it is published
			// again; consumers deduplicate by id. Log it so it is visible.
			r.cfg.Logger.Error("outbox row published but not marked", "id", row.env.ID, "err", dbErr)
		}
		return
	}

	attempts := row.attempts + 1
	dead := attempts >= r.cfg.MaxAttempts
	delay := r.backoff(attempts)
	if _, dbErr := r.pool.Exec(ctx, `
		UPDATE event_outbox SET attempts = $2, last_error = $3, dead = $4, claimed_at = NULL, claimed_by = NULL,
		                        next_attempt_at = now() + $5::interval
		WHERE id = $1`, row.env.ID, attempts, err.Error(), dead, delay); dbErr != nil {
		r.cfg.Logger.Error("outbox row failure not recorded", "id", row.env.ID, "err", dbErr)
	}
	if dead {
		r.cfg.Logger.Error("outbox row marked dead; operator action needed", "id", row.env.ID, "type", row.env.Type, "attempts", attempts, "err", err)
		r.cfg.Observer.Observe(ctx, events.Stat{Consumer: row.env.Source, Type: row.env.Type, Outcome: events.OutcomeDeadLettered, Attempt: attempts, Err: err})
		return
	}
	r.cfg.Logger.Warn("outbox publish failed; will retry", "id", row.env.ID, "type", row.env.Type, "attempt", attempts, "in", delay, "err", err)
}

func (r *Relay) backoff(attempt int) time.Duration {
	i := min(max(attempt-1, 0), len(r.cfg.Backoff)-1)
	d := r.cfg.Backoff[i]
	return max(d+time.Duration(float64(d)*0.2*(2*rand.Float64()-1)), 0)
}

// Prune deletes published rows older than keep. Run it from a periodic job.
func Prune(ctx context.Context, pool *pgxpool.Pool, keep time.Duration) (int64, error) {
	tag, err := pool.Exec(ctx, `DELETE FROM event_outbox WHERE published_at IS NOT NULL AND published_at < now() - $1::interval`, keep)
	if err != nil {
		return 0, fmt.Errorf("eventspg: prune outbox: %w", err)
	}
	return tag.RowsAffected(), nil
}

// Dead lists rows marked dead, oldest first, for an operator tool.
func Dead(ctx context.Context, pool *pgxpool.Pool, limit int) ([]events.Envelope, error) {
	q, err := pool.Query(ctx, `SELECT id, type, source, attempts, last_error FROM event_outbox WHERE dead ORDER BY id LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("eventspg: list dead: %w", err)
	}
	defer q.Close()
	var out []events.Envelope
	for q.Next() {
		var e events.Envelope
		var attempts int
		var lastErr *string
		if err := q.Scan(&e.ID, &e.Type, &e.Source, &attempts, &lastErr); err != nil {
			return nil, err
		}
		e.Headers = map[string]string{"attempts": fmt.Sprint(attempts)}
		if lastErr != nil {
			e.Headers["last_error"] = *lastErr
		}
		out = append(out, e)
	}
	return out, q.Err()
}

// Revive clears the dead flag and attempts so the relay retries the row.
func Revive(ctx context.Context, pool *pgxpool.Pool, id string) error {
	tag, err := pool.Exec(ctx, `UPDATE event_outbox SET dead = false, attempts = 0, next_attempt_at = now() WHERE id = $1 AND dead`, id)
	if err != nil {
		return fmt.Errorf("eventspg: revive %s: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}
