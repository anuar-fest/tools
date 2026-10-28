// Command alarms shows application alarms the Prometheus way: a condition
// the code recognised becomes alarms_total (events) or alarms_active
// (states) on /metrics, a log line and a span event, and the alerting
// rules that fire on them are generated from the declared definitions so
// Alertmanager does the routing. The hooks turn a readiness loss and an
// opened breaker into alarms without a line of code at the call site.
//
//	go run ./alarms -rules                       # print the Prometheus rule file and exit
//	go run ./alarms
//	curl -s localhost:9090/metrics | grep -E 'alarms_(total|active)'
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/anuar-fest/tools/alarms"
	"github.com/anuar-fest/tools/health"
	"github.com/anuar-fest/tools/httpserver"
	"github.com/anuar-fest/tools/lifecycle"
	"github.com/anuar-fest/tools/metrics"
	"github.com/anuar-fest/tools/resilience/breaker"
)

const service = "alarms-example"

// definitions are the alarms this service raises; the hooks' own are added
// by alarms.New. Declared once, validated at boot, rendered into rules.
var definitions = []alarms.Definition{
	{Type: "billing", Name: "quota_exceeded", Severity: alarms.Critical, Summary: "a tenant went over its hard quota"},
	{Type: "cache", Name: "unreachable", Kind: alarms.State, Severity: alarms.Warning, For: 2 * time.Minute, Summary: "the cache is unreachable"},
}

func main() {
	adminAddr := flag.String("admin-addr", ":9090", "admin listen address for /metrics and /healthz")
	rules := flag.Bool("rules", false, "print the Prometheus alerting rules for this service and exit")
	flag.Parse()
	if *rules {
		out, err := alarms.Rules(alarms.RulesConfig{Service: service, Definitions: append(alarms.BuiltinDefinitions(), definitions...), Labels: map[string]string{"team": "platform"}})
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		_, _ = os.Stdout.Write(out) // ship this file with the chart; Prometheus loads it, Alertmanager routes on severity and service
		return
	}
	os.Exit(run(context.Background(), *adminAddr))
}

func run(ctx context.Context, adminAddr string) int {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	app, err := build(ctx, adminAddr, logger)
	if err != nil {
		logger.Error("boot failed", "err", err)
		return 1
	}
	return lifecycle.ExitCode(app.Run(ctx))
}

func build(ctx context.Context, adminAddr string, logger *slog.Logger) (*lifecycle.App, error) {
	m, err := metrics.New(ctx, metrics.Config{ServiceName: service, Logger: logger})
	if err != nil {
		return nil, err
	}
	al, err := alarms.New(alarms.Config{Service: service, Logger: logger, Definitions: definitions}) // no goroutine, nothing to close
	if err != nil {
		return nil, err
	}

	// Hook 1: readiness flips set and clear the health/readiness_lost state.
	dbUp := true
	hc := health.New(health.Config{Logger: logger, OnReadinessChange: al.ReadinessChanged})
	hc.AddReadiness("database", func(context.Context) error {
		if !dbUp {
			return errors.New("connection refused")
		}
		return nil
	})
	// Hook 2: a breaker that opens raises resilience/breaker_opened and sets resilience/breaker_open until it closes.
	br, err := breaker.New(breaker.Config{Name: "payments", Trip: breaker.ConsecutiveFailures(2), Observer: al.ResilienceObserver(nil)})
	if err != nil {
		return nil, err
	}

	admin := http.NewServeMux()
	admin.Handle("GET /metrics", m.Handler())
	admin.Handle("GET /healthz/ready", hc.ReadyHandler())
	adminSrv, err := httpserver.New(httpserver.Config{Addr: adminAddr, Handler: admin, Logger: logger})
	if err != nil {
		return nil, err
	}
	if err = adminSrv.Listen(ctx); err != nil {
		return nil, err
	}
	app := lifecycle.New(lifecycle.Config{Logger: logger, OnShutdown: func(string) { hc.SetDraining(true) }})
	app.Add(lifecycle.Component{Name: "metrics", Stop: m.Shutdown})
	app.Add(lifecycle.Component{Name: "admin-http", Run: adminSrv.Run})
	app.Add(lifecycle.Component{Name: "scenario", Run: func(ctx context.Context) error {
		// An event: the code knows this is wrong before any threshold does. increase(alarms_total{...}[5m]) > 0 fires.
		al.Raise(ctx, alarms.Alarm{Type: "billing", Name: "quota_exceeded", Detail: "tenant acme over the hard limit", Attrs: []slog.Attr{slog.String("tenant", "acme")}})
		// A state: alarms_active == 1 until Clear; the rule fires after For.
		al.Set(ctx, alarms.Alarm{Type: "cache", Name: "unreachable", Detail: "redis refused the connection"})
		al.Set(ctx, alarms.Alarm{Type: "cache", Name: "unreachable", Detail: "still"}) // already set: no second log line
		for range 2 {
			_ = breaker.Do(ctx, br, func(context.Context) error { return errors.New("payments down") }) // second failure opens the breaker
		}
		dbUp = false
		hc.Ready(ctx) // the probe would do this; readiness_lost is set here
		select {
		case <-time.After(10 * time.Second):
			al.Clear(ctx, alarms.Alarm{Type: "cache", Name: "unreachable", Detail: "redis is back"})
			dbUp = true
			hc.Ready(ctx) // readiness_lost cleared
		case <-ctx.Done():
		}
		<-ctx.Done()
		return nil
	}})
	return app, nil
}
