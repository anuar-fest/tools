// Command natsjs shows JetStream done right: a connection with credentials
// out of the URL, a stream with explicit limits, a dedup publisher, and a
// bounded durable consumer with heartbeats, backoff and a dead-letter
// subject. It needs a NATS server with JetStream enabled.
//
//	nats-server -js &
//	go run ./events/natsjs -nats-url nats://localhost:4222
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/anuar-fest/tools/events"
	"github.com/anuar-fest/tools/events/natsjs"
	"github.com/anuar-fest/tools/lifecycle"
)

const service = "natsjs-example"

func main() {
	url := flag.String("nats-url", "", "NATS URL (required)")
	flag.Parse()
	if *url == "" {
		fmt.Fprintln(os.Stderr, "-nats-url is required")
		os.Exit(2)
	}
	os.Exit(run(context.Background(), *url))
}

func run(ctx context.Context, url string) int {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	app, err := build(ctx, url, logger)
	if err != nil {
		logger.Error("boot failed", "err", err)
		return 1
	}
	return lifecycle.ExitCode(app.Run(ctx))
}

func build(ctx context.Context, url string, logger *slog.Logger) (*lifecycle.App, error) {
	nc, err := natsjs.Connect(ctx, natsjs.Config{URL: url, Name: service, Logger: logger})
	if err != nil {
		return nil, err
	}
	// The stream owner declares limits; zero limits are never what production wants.
	streams := []natsjs.StreamConfig{
		{Name: "ORDERS", Subjects: []string{"orders.>"}, MaxAge: 24 * time.Hour, MaxBytes: 64 << 20},
		natsjs.DeadLetterStream(),
	}
	for _, s := range streams {
		if err = nc.EnsureStream(ctx, s); err != nil {
			return nil, errors.Join(err, nc.Close(ctx))
		}
	}
	app := lifecycle.New(lifecycle.Config{Logger: logger})
	app.Add(lifecycle.Component{Name: "nats", Stop: nc.Close})

	consumer, err := nc.Consume(ctx, natsjs.ConsumerConfig{
		Stream: "ORDERS", Name: service + "-projector", Workers: 2, MaxDeliver: 3,
		Handler: func(ctx context.Context, e events.Envelope) error {
			d, _ := events.DeliveryFrom(ctx)
			logger.InfoContext(ctx, "event received", "type", e.Type, "id", e.ID, "attempt", d.Attempt)
			if strings.Contains(string(e.Data), "poison") {
				return errors.New("cannot process") // retried with backoff, then dead-lettered with x-dlq-* headers
			}
			return nil
		},
		Middlewares:       []events.Middleware{events.Inbox(events.NewMemoryInbox(), service+"-projector", events.NopObserver{})},
		DeadLetterSubject: natsjs.DeadLetterSubject(service + "-projector"),
		Logger:            logger,
	})
	if err != nil {
		return nil, errors.Join(err, nc.Close(ctx))
	}
	// Added after the connection: stopped before it, so in-flight handlers finish before the socket drains.
	app.Add(lifecycle.Component{Name: "consumer", Stop: consumer.Stop})

	pub := nc.NewPublisher(natsjs.PublisherConfig{Timeout: 5 * time.Second})
	app.Add(lifecycle.Component{Name: "publisher", Run: func(ctx context.Context) error {
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for i := 1; ; i++ {
			select {
			case <-ctx.Done():
				return nil
			case <-t.C:
				data := fmt.Sprintf(`{"order_id":"o-%d"}`, i)
				if i%5 == 0 {
					data = `{"order_id":"poison"}`
				}
				e := events.New("orders.order.placed.v1", service, []byte(data), events.WithTenant("acme"))
				if err := pub.Publish(ctx, e); err != nil { // returns after the server acked; Nats-Msg-Id dedups a retry
					logger.Warn("publish failed", "err", err)
				}
			}
		}
	}})
	return app, nil
}
