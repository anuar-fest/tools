// Command zerolog shows the migration bridge: a service that keeps its
// zerolog logger hands the libraries an slog.Handler over it, so both
// generations write to one sink with one redaction list and one set of
// context hooks.
//
//	go run ./logx/zerolog
package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/rs/zerolog"

	"github.com/anuar-fest/tools/logx"
	logxzerolog "github.com/anuar-fest/tools/logx/zerolog"
)

type requestKey struct{}

func requestAttrs(ctx context.Context) []slog.Attr {
	if id, ok := ctx.Value(requestKey{}).(string); ok {
		return []slog.Attr{slog.String("request_id", id)}
	}
	return nil
}

func main() {
	// The logger the service already has.
	zl := zerolog.New(os.Stdout).With().Timestamp().Str("service", "zerolog-example").Logger()
	zl.Info().Msg("still written by zerolog directly")

	// What the libraries get: same sink, same redaction, same hooks.
	h := logxzerolog.NewHandler(logxzerolog.Config{
		Logger:      zl,
		Level:       slog.LevelInfo,
		ReplaceAttr: logx.Redact(logx.DefaultRedactKeys...),
	})
	logger := slog.New(logx.ContextHandler(h, requestAttrs))

	logger.Info("written through slog", "password", "hunter2") // password=[REDACTED]
	ctx := context.WithValue(context.Background(), requestKey{}, "req-123")
	logger.WarnContext(ctx, "slow query", "elapsed_ms", 840) // request_id=req-123 from the hook
	logger.Debug("dropped: below the handler's level")
}
