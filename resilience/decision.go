package resilience

import (
	"context"
	"time"
)

// Decision is the low-cardinality outcome a policy reports for one evaluation.
// The set is closed on purpose: it is a metric label.
type Decision string

const (
	DecisionSuccess          Decision = "success"
	DecisionFailure          Decision = "failure"
	DecisionTimeout          Decision = "timeout"
	DecisionCanceled         Decision = "canceled"
	DecisionBulkheadFull     Decision = "bulkhead_full"
	DecisionBreakerOpen      Decision = "breaker_open"
	DecisionLimitExceeded    Decision = "limit_exceeded"
	DecisionRetry            Decision = "retry"
	DecisionRetryBudget      Decision = "retry_budget"
	DecisionRetriesExhausted Decision = "retries_exhausted"
	DecisionShed             Decision = "shed"
	DecisionPoolFull         Decision = "pool_full"
	DecisionHedgeStarted     Decision = "hedge_started"
	DecisionHedgeWon         Decision = "hedge_won"
	DecisionHedgeLost        Decision = "hedge_lost"
	DecisionCoalesced        Decision = "coalesced"
	DecisionPanic            Decision = "panic"
)

// Policy names used in Event.Policy. Also a closed set.
const (
	PolicyRetry    = "retry"
	PolicyBreaker  = "breaker"
	PolicyBulkhead = "bulkhead"
	PolicyLimit    = "ratelimit"
	PolicyShed     = "shed"
	PolicyWorkpool = "workpool"
	PolicyHedge    = "hedge"
	PolicyCoalesce = "coalesce"
	PolicyStack    = "stack"
)

// Event is one policy decision. Dependency is the logical name of what the
// policy protects (an upstream service, a pool); it is the only free-text label
// and must stay low-cardinality.
type Event struct {
	Dependency string
	Policy     string
	Decision   Decision
	// Attempt is 1-based where attempts are meaningful (retry, hedge), else 0.
	Attempt int
	// Elapsed is the duration of the attempt or call where measured, else 0.
	Elapsed time.Duration
	Err     error
}

// BreakerState is the circuit breaker state reported through the Observer.
type BreakerState int

const (
	BreakerClosed BreakerState = iota
	BreakerOpen
	BreakerHalfOpen
)

func (s BreakerState) String() string {
	switch s {
	case BreakerClosed:
		return "closed"
	case BreakerOpen:
		return "open"
	case BreakerHalfOpen:
		return "half_open"
	default:
		return "unknown"
	}
}

// StateChange is a circuit breaker transition.
type StateChange struct {
	Dependency string
	From, To   BreakerState
}

// Observer receives policy decisions and breaker transitions. Implementations
// must be cheap and non-blocking: they run on the request path.
type Observer interface {
	ObserveDecision(ctx context.Context, e Event)
	ObserveBreakerState(ctx context.Context, c StateChange)
}

// NopObserver discards everything. Policies use it when no Observer is given.
type NopObserver struct{}

func (NopObserver) ObserveDecision(context.Context, Event)           {}
func (NopObserver) ObserveBreakerState(context.Context, StateChange) {}

// Observers fans out to several observers in order.
func Observers(obs ...Observer) Observer {
	out := make(multiObserver, 0, len(obs))
	for _, o := range obs {
		if o != nil {
			out = append(out, o)
		}
	}
	if len(out) == 1 {
		return out[0]
	}
	return out
}

type multiObserver []Observer

func (m multiObserver) ObserveDecision(ctx context.Context, e Event) {
	for _, o := range m {
		o.ObserveDecision(ctx, e)
	}
}

func (m multiObserver) ObserveBreakerState(ctx context.Context, c StateChange) {
	for _, o := range m {
		o.ObserveBreakerState(ctx, c)
	}
}

// OrNop returns o, or NopObserver when o is nil, so policies never nil-check.
func OrNop(o Observer) Observer {
	if o == nil {
		return NopObserver{}
	}
	return o
}
