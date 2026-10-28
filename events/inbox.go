package events

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// InboxStore remembers which events a consumer has processed, so a
// redelivered event is acknowledged without running the handler again.
//
// Claim returns claimed=false when the event is already done or is being
// processed by another worker whose claim has not expired. Complete marks it
// done. Release forgets a claim after a failed run so the redelivery can try
// again.
type InboxStore interface {
	Claim(ctx context.Context, consumer, eventID string) (claimed bool, err error)
	Complete(ctx context.Context, consumer, eventID string) error
	Release(ctx context.Context, consumer, eventID string) error
}

// Inbox makes a handler idempotent per consumer with an InboxStore. The
// window where an effect was applied but Complete did not run (crash in
// between) still redelivers, so handlers stay safe to repeat; the inbox
// removes the common duplicate, not every duplicate.
func Inbox(store InboxStore, consumer string, obs Observer) Middleware {
	obs = OrNop(obs)
	return func(next Handler) Handler {
		return func(ctx context.Context, e Envelope) error {
			claimed, err := store.Claim(ctx, consumer, e.ID)
			if err != nil {
				return fmt.Errorf("events: inbox claim %s: %w", e.ID, err)
			}
			if !claimed {
				d, _ := DeliveryFrom(ctx)
				obs.Observe(ctx, Stat{Consumer: consumer, Type: e.Type, Outcome: OutcomeDuplicate, Attempt: d.Attempt})
				return nil
			}
			if err := next(ctx, e); err != nil {
				if IsDrop(err) {
					// A dropped event is done as far as the inbox is concerned.
					_ = store.Complete(ctx, consumer, e.ID) // best effort; the dead-letter is the record
					return err
				}
				if relErr := store.Release(ctx, consumer, e.ID); relErr != nil {
					return fmt.Errorf("%w (and inbox release failed: %w)", err, relErr)
				}
				return err
			}
			if err := store.Complete(ctx, consumer, e.ID); err != nil {
				// The effect happened; a redelivery will run the handler again,
				// which is the at-least-once contract. Report it loudly.
				return fmt.Errorf("events: inbox complete %s after a successful run: %w", e.ID, err)
			}
			return nil
		}
	}
}

// MemoryInbox is an in-process InboxStore for tests and single-replica
// development. Claims expire after Lease so a crashed worker's claim does
// not block redelivery forever; completed ids are kept for Retention.
type MemoryInbox struct {
	Lease     time.Duration
	Retention time.Duration
	Now       func() time.Time

	mu      sync.Mutex
	entries map[string]memoryInboxEntry
}

type memoryInboxEntry struct {
	done bool
	at   time.Time
}

// NewMemoryInbox returns an inbox with a 1 minute lease and 24 hour retention.
func NewMemoryInbox() *MemoryInbox {
	return &MemoryInbox{Lease: time.Minute, Retention: 24 * time.Hour, Now: time.Now, entries: map[string]memoryInboxEntry{}}
}

func (m *MemoryInbox) key(consumer, id string) string { return consumer + "\x00" + id }

func (m *MemoryInbox) Claim(_ context.Context, consumer, eventID string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.Now()
	m.sweepLocked(now)
	k := m.key(consumer, eventID)
	if e, ok := m.entries[k]; ok {
		if e.done || now.Sub(e.at) < m.Lease {
			return false, nil
		}
	}
	m.entries[k] = memoryInboxEntry{at: now}
	return true, nil
}

func (m *MemoryInbox) Complete(_ context.Context, consumer, eventID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries[m.key(consumer, eventID)] = memoryInboxEntry{done: true, at: m.Now()}
	return nil
}

func (m *MemoryInbox) Release(_ context.Context, consumer, eventID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := m.key(consumer, eventID)
	if e, ok := m.entries[k]; ok && !e.done {
		delete(m.entries, k)
	}
	return nil
}

// Len is the number of tracked ids.
func (m *MemoryInbox) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.entries)
}

func (m *MemoryInbox) sweepLocked(now time.Time) {
	for k, e := range m.entries {
		if e.done && now.Sub(e.at) > m.Retention {
			delete(m.entries, k)
		}
	}
}

var _ InboxStore = (*MemoryInbox)(nil)
