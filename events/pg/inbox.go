package eventspg

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/anuar-fest/tools/events"
)

// Inbox implements events.InboxStore on PostgreSQL. Lease is how long a
// 'processing' claim blocks other workers before it is considered abandoned
// (default 5 minutes; set it above the handler timeout).
type Inbox struct {
	pool  *pgxpool.Pool
	lease time.Duration
}

// NewInbox builds an Inbox.
func NewInbox(pool *pgxpool.Pool, lease time.Duration) (*Inbox, error) {
	if pool == nil {
		return nil, errors.New("eventspg: inbox needs a pool")
	}
	if lease <= 0 {
		lease = 5 * time.Minute
	}
	return &Inbox{pool: pool, lease: lease}, nil
}

// Claim implements events.InboxStore.
func (i *Inbox) Claim(ctx context.Context, consumer, eventID string) (bool, error) {
	var one int
	err := i.pool.QueryRow(ctx, `
		INSERT INTO event_inbox (consumer, event_id, status, claimed_at) VALUES ($1, $2, 'processing', now())
		ON CONFLICT (consumer, event_id) DO UPDATE SET claimed_at = now()
		    WHERE event_inbox.status = 'processing' AND event_inbox.claimed_at < now() - $3::interval
		RETURNING 1`, consumer, eventID, i.lease).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("eventspg: inbox claim: %w", err)
	}
	return true, nil
}

// Complete implements events.InboxStore.
func (i *Inbox) Complete(ctx context.Context, consumer, eventID string) error {
	_, err := i.pool.Exec(ctx, `UPDATE event_inbox SET status = 'done', completed_at = now() WHERE consumer = $1 AND event_id = $2`, consumer, eventID)
	if err != nil {
		return fmt.Errorf("eventspg: inbox complete: %w", err)
	}
	return nil
}

// Release implements events.InboxStore.
func (i *Inbox) Release(ctx context.Context, consumer, eventID string) error {
	_, err := i.pool.Exec(ctx, `DELETE FROM event_inbox WHERE consumer = $1 AND event_id = $2 AND status = 'processing'`, consumer, eventID)
	if err != nil {
		return fmt.Errorf("eventspg: inbox release: %w", err)
	}
	return nil
}

// ClaimInTx is the strong form: call it inside the handler's own
// transaction before the business writes. If it returns false the event was
// already processed and the handler should return nil without doing
// anything; if the transaction rolls back, the claim rolls back with it, so
// there is no window where the effect happened without the record.
func ClaimInTx(ctx context.Context, tx pgx.Tx, consumer, eventID string) (bool, error) {
	var one int
	err := tx.QueryRow(ctx, `
		INSERT INTO event_inbox (consumer, event_id, status, claimed_at, completed_at) VALUES ($1, $2, 'done', now(), now())
		ON CONFLICT (consumer, event_id) DO NOTHING
		RETURNING 1`, consumer, eventID).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("eventspg: inbox claim in tx: %w", err)
	}
	return true, nil
}

// PruneInbox deletes completed rows older than keep. Keep it longer than the
// broker's maximum redelivery horizon.
func PruneInbox(ctx context.Context, pool *pgxpool.Pool, keep time.Duration) (int64, error) {
	tag, err := pool.Exec(ctx, `DELETE FROM event_inbox WHERE status = 'done' AND completed_at < now() - $1::interval`, keep)
	if err != nil {
		return 0, fmt.Errorf("eventspg: prune inbox: %w", err)
	}
	return tag.RowsAffected(), nil
}

var _ events.InboxStore = (*Inbox)(nil)
