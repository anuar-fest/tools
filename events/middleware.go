package events

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"
)

// Middleware wraps a Handler.
type Middleware func(Handler) Handler

// Chain applies middlewares so the first one listed is outermost.
func Chain(h Handler, mws ...Middleware) Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}

// Recover turns a panic in the handler into an error (redelivered, so a
// deploy can fix it; MaxDeliver then dead-letters it). The stack is logged.
func Recover(logger *slog.Logger) Middleware {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return func(next Handler) Handler {
		return func(ctx context.Context, e Envelope) (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("events: handler panicked on %s %s: %v", e.Type, e.ID, r)
					logger.Error("handler panicked", "type", e.Type, "id", e.ID, "panic", r, "stack", string(debug.Stack()))
				}
			}()
			return next(ctx, e)
		}
	}
}

// Timeout bounds one handler run. A timed-out handler is redelivered.
func Timeout(d time.Duration) Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, e Envelope) error {
			ctx, cancel := context.WithTimeout(ctx, d)
			defer cancel()
			return next(ctx, e)
		}
	}
}

// Observe reports the outcome of every handler run to obs.
func Observe(obs Observer, consumer string) Middleware {
	obs = OrNop(obs)
	return func(next Handler) Handler {
		return func(ctx context.Context, e Envelope) error {
			start := time.Now()
			err := next(ctx, e)
			d, _ := DeliveryFrom(ctx)
			obs.Observe(ctx, Stat{
				Consumer: consumer,
				Type:     e.Type,
				Outcome:  outcomeOf(err),
				Attempt:  d.Attempt,
				Elapsed:  time.Since(start),
				Err:      err,
			})
			return err
		}
	}
}

func outcomeOf(err error) Outcome {
	switch {
	case err == nil:
		return OutcomeAcked
	case IsDrop(err):
		return OutcomeDropped
	case errors.Is(err, context.DeadlineExceeded):
		return OutcomeTimeout
	default:
		return OutcomeNacked
	}
}

// Logging logs failures at warn with the delivery attempt, and successes at
// debug. Payloads are never logged.
func Logging(logger *slog.Logger) Middleware {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return func(next Handler) Handler {
		return func(ctx context.Context, e Envelope) error {
			err := next(ctx, e)
			d, _ := DeliveryFrom(ctx)
			attrs := []any{"type", e.Type, "id", e.ID, "tenant", e.TenantID, "correlation", e.CorrelationID, "attempt", d.Attempt}
			if err != nil {
				logger.WarnContext(ctx, "event handler failed", append(attrs, "err", err, "drop", IsDrop(err))...)
			} else {
				logger.DebugContext(ctx, "event handled", attrs...)
			}
			return err
		}
	}
}
