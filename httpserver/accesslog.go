package httpserver

import (
	"log/slog"
	"net/http"
	"time"
)

// AccessLogConfig configures AccessLog.
type AccessLogConfig struct {
	// Logger receives one line per request. nil discards.
	Logger *slog.Logger
	// Route names the matched route for the line; defaults to PatternRoute.
	// It is called after the handler ran, when routers know the pattern.
	Route RouteFunc
	// Skip drops lines for requests such as probes and scrapes.
	Skip func(*http.Request) bool
	// FailuresOnly logs only status 400 and above.
	FailuresOnly bool
	// Message is the log message; default "request served".
	Message string
}

// AccessLog writes one line per request: method, route, path, status,
// bytes in and out, duration, client ip, user agent, protocol. 5xx and
// panics log at Error, everything else at Info. The request id comes from
// the context through LogAttrs; the line adds no field for it.
func AccessLog(cfg AccessLogConfig) func(http.Handler) http.Handler {
	logger := orDiscard(cfg.Logger)
	route := cfg.Route
	if route == nil {
		route = PatternRoute
	}
	msg := cfg.Message
	if msg == "" {
		msg = "request served"
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if cfg.Skip != nil && cfg.Skip(r) {
				next.ServeHTTP(w, r)
				return
			}
			rec := Record(w)
			r = withRouteHolder(r)
			ctx := r.Context()
			start := time.Now()
			next.ServeHTTP(rec, r)
			status := StatusOf(rec, r)
			rt := route(r)
			SetRoute(r, rt)
			if cfg.FailuresOnly && status < http.StatusBadRequest {
				return
			}
			level := slog.LevelInfo
			if status >= http.StatusInternalServerError || rec.Panicked() {
				level = slog.LevelError
			}
			logger.LogAttrs(ctx, level, msg, //nolint:contextcheck // ctx is the request context, re-read after the route holder was attached
				slog.String("method", r.Method),
				slog.String("route", rt),
				slog.String("path", r.URL.Path),
				slog.Int("status", status),
				slog.Int64("bytes_in", max(r.ContentLength, 0)),
				slog.Int64("bytes_out", rec.BytesWritten()),
				slog.Float64("duration_ms", float64(time.Since(start))/float64(time.Millisecond)),
				slog.String("client_ip", ClientIP(r)),
				slog.String("user_agent", r.UserAgent()),
				slog.String("proto", r.Proto),
			)
		})
	}
}
