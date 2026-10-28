package httpserver

import (
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
)

// Headers that describe a body the panicking handler never finished.
var bodyHeaders = []string{
	"Content-Length", "Content-Encoding", "Content-Range", "Content-Disposition",
	"Content-Type", "Accept-Ranges", "ETag", "Last-Modified",
}

const maxStackBytes = 16 << 10

// Recover turns a panic into a logged 500 and marks the response as
// panicked for the metrics and access log outside it. http.ErrAbortHandler
// is re-panicked as net/http expects. If the response had started nothing
// more is written: the client gets a truncated body, which is honest.
func Recover(logger *slog.Logger) func(http.Handler) http.Handler {
	logger = orDiscard(logger)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rec := Record(w)
			ctx := r.Context()
			defer func() {
				p := recover()
				if p == nil {
					return
				}
				if p == http.ErrAbortHandler { //nolint:errorlint // sentinel compared by identity, as net/http does
					panic(p)
				}
				markPanicked(rec)
				stack := debug.Stack()
				if len(stack) > maxStackBytes {
					stack = stack[:maxStackBytes]
				}
				logger.ErrorContext(ctx, "panic recovered",
					"panic", fmt.Sprint(p), "method", r.Method, "path", r.URL.Path, "stack", string(stack))
				if rec.Status() != 0 || rec.Hijacked() {
					return
				}
				h := rec.Header()
				for _, name := range bodyHeaders {
					h.Del(name)
				}
				WriteProblem(rec, r, NewProblem(http.StatusInternalServerError, "The server could not complete the request."))
			}()
			next.ServeHTTP(rec, r)
		})
	}
}

func orDiscard(l *slog.Logger) *slog.Logger {
	if l == nil {
		return slog.New(slog.DiscardHandler)
	}
	return l
}
