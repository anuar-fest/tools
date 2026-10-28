// Package obstest records Observer calls for assertions in this module's tests.
package obstest

import (
	"context"
	"sync"

	"github.com/anuar-fest/tools/resilience"
)

// Recorder is a resilience.Observer that keeps every call. Safe for concurrent use.
type Recorder struct {
	mu      sync.Mutex
	events  []resilience.Event
	changes []resilience.StateChange
}

func (r *Recorder) ObserveDecision(_ context.Context, e resilience.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
}

func (r *Recorder) ObserveBreakerState(_ context.Context, c resilience.StateChange) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.changes = append(r.changes, c)
}

// Events returns a copy of recorded decisions.
func (r *Recorder) Events() []resilience.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]resilience.Event, len(r.events))
	copy(out, r.events)
	return out
}

// Decisions returns just the Decision of every recorded event, in order.
func (r *Recorder) Decisions() []resilience.Decision {
	evs := r.Events()
	out := make([]resilience.Decision, len(evs))
	for i, e := range evs {
		out[i] = e.Decision
	}
	return out
}

// Count returns how many events carry decision d.
func (r *Recorder) Count(d resilience.Decision) int {
	n := 0
	for _, e := range r.Events() {
		if e.Decision == d {
			n++
		}
	}
	return n
}

// Changes returns a copy of recorded breaker transitions.
func (r *Recorder) Changes() []resilience.StateChange {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]resilience.StateChange, len(r.changes))
	copy(out, r.changes)
	return out
}
