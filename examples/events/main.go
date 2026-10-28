// Command events shows the broker-free core: the CloudEvents-shaped
// envelope, the JSON codec, typed naming, the middleware chain (recover,
// timeout, inbox for idempotency, logging) and the ack semantics a handler
// expresses through its error: Drop, RetryAfter, or a plain failure.
//
//	go run ./events
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/anuar-fest/tools/events"
)

type orderPlaced struct {
	OrderID     string `json:"order_id"`
	AmountMinor int64  `json:"amount_minor"`
}

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	codec := events.JSON{}

	// The handler decides the ack: nil acks, Drop dead-letters without retry, RetryAfter delays the redelivery, anything else retries with backoff.
	var projector events.Handler = func(ctx context.Context, e events.Envelope) error {
		var op orderPlaced
		if err := events.Decode(codec, e, &op); err != nil {
			return err // a codec mismatch is already a Drop: redelivery will not change the bytes
		}
		d, _ := events.DeliveryFrom(ctx)
		logger.InfoContext(ctx, "projecting", "type", e.Type, "id", e.ID, "tenant", e.TenantID, "order", op.OrderID, "attempt", d.Attempt)
		switch {
		case op.AmountMinor < 0:
			return events.Drop(errors.New("negative amount")) // poison: park it, do not retry
		case op.AmountMinor > 1_000_000:
			return events.RetryAfter(errors.New("fraud check pending"), 30*time.Second)
		}
		return nil
	}
	// First listed is outermost. Inbox makes the handler idempotent per consumer; a MemoryInbox is enough in-process, events/pg for real.
	handler := events.Chain(projector,
		events.Recover(logger),
		events.Logging(logger),
		events.Inbox(events.NewMemoryInbox(), "projector", events.NopObserver{}),
		events.Timeout(2*time.Second),
	)

	// An in-process transport: what natsjs does over the wire, including the delivery metadata.
	var bus events.Publisher = events.PublisherFunc(func(ctx context.Context, e events.Envelope) error {
		return handler(events.WithDelivery(ctx, events.Delivery{Attempt: 1, Consumer: "projector", Stream: "memory"}), e)
	})

	placed, err := events.Encode(codec, "orders.order.placed.v1", "orders-api", orderPlaced{OrderID: "o-1", AmountMinor: 1200},
		events.WithTenant("acme"), events.WithSubject("order/o-1"))
	if err != nil {
		return err
	}
	if err = bus.Publish(ctx, placed); err != nil {
		return err
	}
	if err = bus.Publish(ctx, placed); err != nil { // same ID: the inbox skips it
		return err
	}
	// Derive keeps correlation and sets causation to the parent, so the whole flow is one trace in the logs.
	paid := events.Derive(placed, "orders.order.paid.v1", "payments", []byte(`{"order_id":"o-1","amount_minor":-5}`))
	err = bus.Publish(ctx, paid)
	logger.Info("poison handled", "dropped", events.IsDrop(err), "err", err)

	typ, err := events.ParseType(placed.Type) // "orders.order.placed.v1" → domain, aggregate, action, version
	if err != nil {
		return err
	}
	fmt.Printf("type: %+v\nheaders: %v\n", typ, events.ToHeaders(placed))
	return nil
}
