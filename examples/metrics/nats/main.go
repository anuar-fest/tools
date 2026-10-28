// Command nats shows metrics for raw nats.go: messages sent and consumed
// and handler durations per subject and consumer, plus the connection's
// counters (in/out messages and bytes, reconnects, connected), all on
// /metrics. It needs a NATS server.
//
//	go run ./metrics/nats -nats-url nats://localhost:4222
//	curl -s localhost:9090/metrics | grep messaging_
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/anuar-fest/tools/httpserver"
	"github.com/anuar-fest/tools/lifecycle"
	"github.com/anuar-fest/tools/metrics"
	metricsnats "github.com/anuar-fest/tools/metrics/nats"
)

func main() {
	adminAddr := flag.String("admin-addr", ":9090", "admin listen address for /metrics")
	url := flag.String("nats-url", "", "NATS URL (required)")
	flag.Parse()
	if *url == "" {
		fmt.Fprintln(os.Stderr, "-nats-url is required")
		os.Exit(2)
	}
	os.Exit(run(context.Background(), *adminAddr, *url))
}

func run(ctx context.Context, adminAddr, url string) int {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	app, err := build(ctx, adminAddr, url, logger)
	if err != nil {
		logger.Error("boot failed", "err", err)
		return 1
	}
	return lifecycle.ExitCode(app.Run(ctx))
}

func build(ctx context.Context, adminAddr, url string, logger *slog.Logger) (*lifecycle.App, error) {
	m, err := metrics.New(ctx, metrics.Config{ServiceName: "metricsnats-example", Logger: logger})
	if err != nil {
		return nil, err
	}
	nc, err := nats.Connect(url, nats.Name("metricsnats-example"))
	if err != nil {
		return nil, err
	}
	n, err := metricsnats.New(metricsnats.Config{
		Consumer: "greeter",
		Logger:   logger,
		// Subjects that embed ids would create a series per id; map them to a closed set.
		DestinationName: func(subject string) string { return strings.Join(strings.SplitN(subject, ".", 3)[:2], ".") },
	})
	if err != nil {
		nc.Close()
		return nil, err
	}
	stats, err := metricsnats.RegisterConnStats(nil, nc) // messaging.nats.messages{direction}, .bytes, .reconnects, .connected
	if err != nil {
		nc.Close()
		return nil, err
	}
	// Metrics-only subscriber: MsgHandler recovers a panic itself. With tracing too: tr.MsgHandler(n.Handler(handle)).
	sub, err := nc.QueueSubscribe("example.greet.*", "greeter", n.MsgHandler(func(_ context.Context, msg *nats.Msg) error { //nolint:contextcheck // the handler has no caller context: the message is the root
		if strings.HasSuffix(msg.Subject, ".bad") {
			return errors.New("cannot greet") // error.type on the consumed counter and the duration
		}
		return nil
	}))
	if err != nil {
		nc.Close()
		return nil, err
	}

	admin := http.NewServeMux()
	admin.Handle("GET /metrics", m.Handler())
	adminSrv, err := httpserver.New(httpserver.Config{Addr: adminAddr, Handler: admin, Logger: logger})
	if err != nil {
		return nil, err
	}
	if err := adminSrv.Listen(ctx); err != nil {
		return nil, err
	}
	app := lifecycle.New(lifecycle.Config{Logger: logger})
	app.Add(lifecycle.Component{Name: "metrics", Stop: m.Shutdown})
	app.Add(lifecycle.Component{Name: "admin-http", Run: adminSrv.Run})
	app.Add(lifecycle.Component{Name: "nats", Stop: func(ctx context.Context) error {
		err := errors.Join(sub.Drain(), stats.Unregister())
		return errors.Join(err, nc.Drain())
	}})
	app.Add(lifecycle.Component{Name: "publisher", Run: func(ctx context.Context) error {
		t := time.NewTicker(500 * time.Millisecond)
		defer t.Stop()
		for i := 0; ; i++ {
			select {
			case <-ctx.Done():
				return nil
			case <-t.C:
				subject := fmt.Sprintf("example.greet.%d", i) // the metric label is "example.greet" through DestinationName
				if i%4 == 0 {
					subject = "example.greet.bad"
				}
				if err := n.PublishMsg(ctx, nc, &nats.Msg{Subject: subject, Data: []byte("hello")}); err != nil && ctx.Err() == nil {
					logger.Warn("publish failed", "err", err)
				}
			}
		}
	}})
	return app, nil
}
