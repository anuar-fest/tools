package events

import (
	"context"
	"time"
)

// Outcome is the closed set of what happened to one message.
type Outcome string

const (
	OutcomePublished     Outcome = "published"
	OutcomePublishFailed Outcome = "publish_failed"
	OutcomeAcked         Outcome = "acked"
	OutcomeNacked        Outcome = "nacked"
	OutcomeDropped       Outcome = "dropped"
	OutcomeDeadLettered  Outcome = "dead_lettered"
	OutcomeDuplicate     Outcome = "duplicate"
	OutcomePanic         Outcome = "panic"
	OutcomeTimeout       Outcome = "timeout"
	OutcomeUndecodable   Outcome = "undecodable"
)

// Stat is one observation. Consumer is the subscription name on the consume
// side and the Source on the publish side. Type is the event type.
type Stat struct {
	Consumer string
	Type     string
	Outcome  Outcome
	Attempt  int
	Elapsed  time.Duration
	Err      error
}

// Observer receives Stats. Implementations must be cheap and non-blocking.
type Observer interface {
	Observe(ctx context.Context, s Stat)
}

// NopObserver discards everything.
type NopObserver struct{}

func (NopObserver) Observe(context.Context, Stat) {}

// OrNop returns o or a NopObserver.
func OrNop(o Observer) Observer {
	if o == nil {
		return NopObserver{}
	}
	return o
}
