// Command logx shows the one logger setup a service needs: a validated
// level, JSON or text output, key-based redaction, Secret values that never
// print, and context hooks that stamp per-request attributes on every line.
//
//	go run ./logx -log-level debug -log-format text
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/anuar-fest/tools/logx"
)

type tenantKey struct{}

// tenantAttrs is a ContextAttrs hook: anything a middleware put in the
// context reaches every *Context log call without being passed around.
func tenantAttrs(ctx context.Context) []slog.Attr {
	if t, ok := ctx.Value(tenantKey{}).(string); ok {
		return []slog.Attr{slog.String("tenant", t)}
	}
	return nil
}

type config struct {
	DSN    string
	APIKey logx.Secret // never prints: fmt, json and slog all see [REDACTED]
}

func main() {
	levelFlag := flag.String("log-level", "info", "debug | info | warn | error")
	format := flag.String("log-format", "json", "json | text")
	flag.Parse()

	level, err := logx.ParseLevel(*levelFlag) // a typo fails the boot instead of silently logging at Info
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	logger, err := logx.New(logx.Config{
		Level:        level,
		Format:       logx.Format(*format),
		Attrs:        []slog.Attr{slog.String("service", "logx-example"), slog.String("version", "dev")},
		RedactKeys:   []string{"card_number"}, // on top of DefaultRedactKeys (password, token, authorization, ...)
		ContextAttrs: []logx.ContextAttrs{tenantAttrs},
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	cfg := config{DSN: "postgres://app@db:5432/app", APIKey: logx.Secret(os.Getenv("EXAMPLE_API_KEY"))} // a real service loads it from config
	logger.Info("config loaded", "dsn", cfg.DSN, "api_key", cfg.APIKey)
	logger.Info("payment captured", "card_number", "4111111111111111", "amount_minor", 1200)
	logger.Warn("auth failed", "password", "hunter2", "authorization", "Bearer abc")
	logger.Debug("only visible with -log-level debug", "detail", 42)

	ctx := context.WithValue(context.Background(), tenantKey{}, "acme")
	logger.InfoContext(ctx, "request served") // carries tenant=acme from the hook
	logger.Info("request served")             // no context, no tenant

	fmt.Fprintln(os.Stderr, "revealed on purpose:", cfg.APIKey.Reveal())
}
