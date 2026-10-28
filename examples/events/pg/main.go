// Command pg shows the transactional outbox and the Postgres inbox: the
// business row and the event are written in one transaction, a relay
// publishes with SKIP LOCKED claims, and a consumer deduplicates through
// the inbox. It needs a Postgres database.
//
//	go run ./events/pg -db-dsn postgres://user:pass@localhost:5432/app
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/anuar-fest/tools/events"
	eventspg "github.com/anuar-fest/tools/events/pg"
)

func main() {
	dsn := flag.String("db-dsn", "", "Postgres DSN (required)")
	flag.Parse()
	if *dsn == "" {
		fmt.Fprintln(os.Stderr, "-db-dsn is required")
		os.Exit(2)
	}
	if err := run(context.Background(), *dsn); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, dsn string) error {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return err
	}
	defer pool.Close()
	if _, err = pool.Exec(ctx, eventspg.Schema); err != nil { // the outbox and inbox tables; idempotent
		return err
	}

	// 1. Outbox: the business write and the event in one transaction. Roll back and the event never existed.
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err = tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS example_orders (id text PRIMARY KEY, created_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return err
	}
	orderID := fmt.Sprintf("o-%d", time.Now().UnixNano())
	if _, err = tx.Exec(ctx, `INSERT INTO example_orders (id) VALUES ($1)`, orderID); err != nil {
		return err
	}
	e := events.New("orders.order.placed.v1", "orders-api", []byte(`{"order_id":"`+orderID+`"}`), events.WithTenant("acme"))
	if err = eventspg.Enqueue(ctx, tx, e); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}

	// 2. Relay: one per replica in production (app.Add(lifecycle.Component{Run: relay.Run})); one batch here.
	published := events.PublisherFunc(func(ctx context.Context, e events.Envelope) error {
		logger.InfoContext(ctx, "relayed to the broker", "type", e.Type, "id", e.ID)
		return nil
	})
	hostname, _ := os.Hostname()
	relay, err := eventspg.NewRelay(pool, published, eventspg.RelayConfig{InstanceID: hostname, Logger: logger})
	if err != nil {
		return err
	}
	n, err := relay.RelayBatch(ctx)
	if err != nil {
		return err
	}
	logger.Info("relay batch done", "published", n)

	// 3. Inbox: the consumer side; a redelivery of the same ID is skipped.
	inbox, err := eventspg.NewInbox(pool, time.Minute)
	if err != nil {
		return err
	}
	handler := events.Chain(func(ctx context.Context, e events.Envelope) error {
		logger.InfoContext(ctx, "projected", "id", e.ID)
		return nil
	}, events.Inbox(inbox, "projector", events.NopObserver{}))
	for range 2 {
		if err = handler(ctx, e); err != nil {
			return err
		}
	}
	pruned, err := eventspg.Prune(ctx, pool, 7*24*time.Hour) // housekeeping for a cron, not a request path
	if err != nil {
		return err
	}
	logger.Info("outbox pruned", "rows", pruned)
	return nil
}
