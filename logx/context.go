package logx

import (
	"context"
	"log/slog"
)

// ContextAttrs reads attributes from a context. Implementations return nil
// when the context carries nothing of theirs, and must be cheap: they run
// on every log call that passes a context.
//
// httpserver.LogAttrs (request id, consumer) and otelx.LogAttrs (trace id,
// span id) have this signature.
type ContextAttrs func(ctx context.Context) []slog.Attr

// ContextHandler wraps inner so that records logged through the *Context
// methods carry the attributes extract reads from the context. Records
// logged without a context are passed through unchanged.
//
// Attributes are added to the record, so after WithGroup they appear inside
// the open group; wire the handler at the bottom of the chain and avoid
// groups on loggers that log request-scoped lines if that matters.
func ContextHandler(inner slog.Handler, extract ...ContextAttrs) slog.Handler {
	fns := make([]ContextAttrs, 0, len(extract))
	for _, f := range extract {
		if f != nil {
			fns = append(fns, f)
		}
	}
	if len(fns) == 0 {
		return inner
	}
	return &contextHandler{inner: inner, extract: fns}
}

type contextHandler struct {
	inner   slog.Handler
	extract []ContextAttrs
}

func (h *contextHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.inner.Enabled(ctx, l)
}

func (h *contextHandler) Handle(ctx context.Context, r slog.Record) error {
	if ctx == nil {
		return h.inner.Handle(ctx, r)
	}
	var attrs []slog.Attr
	for _, f := range h.extract {
		attrs = append(attrs, f(ctx)...)
	}
	if len(attrs) > 0 {
		r = r.Clone()
		r.AddAttrs(attrs...)
	}
	return h.inner.Handle(ctx, r)
}

func (h *contextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &contextHandler{inner: h.inner.WithAttrs(attrs), extract: h.extract}
}

func (h *contextHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	return &contextHandler{inner: h.inner.WithGroup(name), extract: h.extract}
}
