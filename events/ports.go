package events

import (
	"context"
	"errors"
	"time"
)

// Publisher sends one envelope. Implementations return only after the
// broker acknowledged the write; a nil error means "durably accepted".
type Publisher interface {
	Publish(ctx context.Context, e Envelope) error
}

// PublisherFunc adapts a function to Publisher.
type PublisherFunc func(ctx context.Context, e Envelope) error

func (f PublisherFunc) Publish(ctx context.Context, e Envelope) error { return f(ctx, e) }

// Handler processes one delivery. Its return value is the acknowledgement:
//
//	nil                → ack
//	err                → redeliver later (nak with the consumer's backoff)
//	RetryAfter(err, d) → redeliver after d
//	Drop(err)          → never redeliver: dead-letter if configured, else terminate
//
// Handlers must be idempotent: redelivery is normal.
type Handler func(ctx context.Context, e Envelope) error

// Consumer is a running subscription.
type Consumer interface {
	// Stop stops pulling, lets in-flight handlers finish until ctx ends, then
	// returns. Unacknowledged messages are redelivered by the broker.
	Stop(ctx context.Context) error
}

// Delivery metadata a transport attaches to the handler context.
type Delivery struct {
	// Attempt is 1 on first delivery.
	Attempt int
	// Consumer is the subscription name.
	Consumer string
	// Stream or topic the message came from.
	Stream string
}

type deliveryKey struct{}

// WithDelivery attaches delivery metadata to ctx. Transports call it.
func WithDelivery(ctx context.Context, d Delivery) context.Context {
	return context.WithValue(ctx, deliveryKey{}, d)
}

// DeliveryFrom reads delivery metadata; ok is false outside a handler.
func DeliveryFrom(ctx context.Context) (Delivery, bool) {
	d, ok := ctx.Value(deliveryKey{}).(Delivery)
	return d, ok
}

type dropError struct{ err error }

func (e *dropError) Error() string { return e.err.Error() }
func (e *dropError) Unwrap() error { return e.err }

// Drop marks err as permanent: the message must not be redelivered. Use it
// for a payload that cannot be decoded or a business rule that will never
// pass. The transport dead-letters it when a dead-letter subject is set.
func Drop(err error) error {
	if err == nil {
		return nil
	}
	return &dropError{err: err}
}

// IsDrop reports whether err was marked with Drop.
func IsDrop(err error) bool {
	var d *dropError
	return errors.As(err, &d)
}

type retryAfterError struct {
	err   error
	after time.Duration
}

func (e *retryAfterError) Error() string { return e.err.Error() }
func (e *retryAfterError) Unwrap() error { return e.err }

// RetryAfter marks err as transient and asks for redelivery after d
// (a dependency said 429, a lock is held). Without it the consumer's own
// backoff applies.
func RetryAfter(err error, d time.Duration) error {
	if err == nil {
		return nil
	}
	return &retryAfterError{err: err, after: d}
}

// RetryDelay returns the delay requested with RetryAfter, if any.
func RetryDelay(err error) (time.Duration, bool) {
	var r *retryAfterError
	if errors.As(err, &r) {
		return r.after, true
	}
	return 0, false
}
