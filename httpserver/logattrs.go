package httpserver

import (
	"context"
	"log/slog"
)

// LogAttrs returns the request id and the authenticated consumer carried
// by ctx, for logx.ContextHandler: every log line of a request then carries
// them without each call site adding them.
func LogAttrs(ctx context.Context) []slog.Attr {
	var attrs []slog.Attr
	if id := RequestIDFrom(ctx); id != "" {
		attrs = append(attrs, slog.String("request_id", id))
	}
	if c := ConsumerFrom(ctx); c != "" {
		attrs = append(attrs, slog.String("consumer", c))
	}
	return attrs
}
