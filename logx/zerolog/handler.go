// Package logxzerolog is an slog.Handler that writes through a
// zerolog.Logger, for services that keep zerolog while every library they
// use logs through *slog.Logger. One sink, one format, no second logger.
//
//	handler := logxzerolog.NewHandler(logxzerolog.Config{Logger: log.Logger})
//	logger := slog.New(logx.ContextHandler(handler, httpserver.LogAttrs, otelx.LogAttrs))
//
// slog levels map to zerolog levels; nothing maps to Fatal or Panic, so a
// library can never exit the process by logging.
package logxzerolog

import (
	"context"
	"log/slog"
	"runtime"
	"strconv"

	"github.com/rs/zerolog"
)

// Config configures NewHandler.
type Config struct {
	// Logger is the sink. A zero Logger discards.
	Logger zerolog.Logger
	// Level is an extra minimum; by default the zerolog logger's level and
	// the global zerolog level decide.
	Level slog.Leveler
	// AddSource writes file:line as "caller".
	AddSource bool
	// ReplaceAttr rewrites leaf attributes before they are written, e.g.
	// logx.Redact(logx.DefaultRedactKeys...). A zero Attr is dropped.
	ReplaceAttr func(groups []string, a slog.Attr) slog.Attr
}

// NewHandler builds the bridge.
func NewHandler(cfg Config) slog.Handler {
	return &handler{cfg: cfg}
}

type handler struct {
	cfg    Config
	groups []string
	// attrs are the attributes from WithAttrs, already nested under the
	// groups open at the time they were added.
	attrs []slog.Attr
}

// Enabled implements slog.Handler.
func (h *handler) Enabled(_ context.Context, level slog.Level) bool {
	if h.cfg.Level != nil && level < h.cfg.Level.Level() {
		return false
	}
	zl := toZerolog(level)
	return zl >= h.cfg.Logger.GetLevel() && zl >= zerolog.GlobalLevel()
}

// Handle implements slog.Handler.
func (h *handler) Handle(_ context.Context, r slog.Record) error {
	ev := h.cfg.Logger.WithLevel(toZerolog(r.Level))
	if ev == nil {
		return nil
	}
	if h.cfg.AddSource && r.PC != 0 {
		frames := runtime.CallersFrames([]uintptr{r.PC})
		if f, _ := frames.Next(); f.File != "" {
			ev.Str("caller", f.File+":"+strconv.Itoa(f.Line))
		}
	}
	all := make([]slog.Attr, 0, len(h.attrs)+r.NumAttrs())
	all = append(all, h.attrs...)
	if r.NumAttrs() > 0 {
		if len(h.groups) == 0 {
			r.Attrs(func(a slog.Attr) bool {
				all = append(all, a)
				return true
			})
		} else {
			attrs := make([]any, 0, r.NumAttrs())
			r.Attrs(func(a slog.Attr) bool {
				attrs = append(attrs, a)
				return true
			})
			all = append(all, nest(h.groups, attrs))
		}
	}
	for _, a := range merge(all) {
		h.write(ev, nil, a)
	}
	ev.Msg(r.Message)
	return nil
}

// merge folds groups that share a key at the same level into one, so
// WithGroup("http").With(...) and a later WithGroup("http") produce one
// "http" object: zerolog would write the key twice and JSON readers keep
// only the last. Inline (empty-key) groups are flattened first.
func merge(attrs []slog.Attr) []slog.Attr {
	flat := flatten(attrs, make([]slog.Attr, 0, len(attrs)))
	out := make([]slog.Attr, 0, len(flat))
	index := map[string]int{}
	for _, a := range flat {
		if a.Value.Kind() == slog.KindGroup {
			if i, ok := index[a.Key]; ok {
				prev := out[i].Value.Group()
				members := make([]slog.Attr, 0, len(prev)+len(a.Value.Group()))
				members = append(append(members, prev...), a.Value.Group()...)
				out[i] = slog.Attr{Key: a.Key, Value: slog.GroupValue(members...)}
				continue
			}
			index[a.Key] = len(out)
		}
		out = append(out, a)
	}
	for i, a := range out {
		if a.Value.Kind() == slog.KindGroup {
			out[i] = slog.Attr{Key: a.Key, Value: slog.GroupValue(merge(a.Value.Group())...)}
		}
	}
	return out
}

func flatten(attrs, out []slog.Attr) []slog.Attr {
	for _, a := range attrs {
		a.Value = a.Value.Resolve()
		if a.Value.Kind() == slog.KindGroup && a.Key == "" {
			out = flatten(a.Value.Group(), out)
			continue
		}
		out = append(out, a)
	}
	return out
}

// WithAttrs implements slog.Handler.
func (h *handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}
	next := h.clone()
	if len(h.groups) == 0 {
		next.attrs = append(next.attrs, attrs...)
		return next
	}
	as := make([]any, len(attrs))
	for i, a := range attrs {
		as[i] = a
	}
	next.attrs = append(next.attrs, nest(h.groups, as))
	return next
}

// WithGroup implements slog.Handler.
func (h *handler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	next := h.clone()
	next.groups = append(next.groups, name)
	return next
}

func (h *handler) clone() *handler {
	return &handler{
		cfg:    h.cfg,
		groups: append([]string(nil), h.groups...),
		attrs:  append([]slog.Attr(nil), h.attrs...),
	}
}

// nest wraps attrs in groups[0] > groups[1] > ... .
func nest(groups []string, attrs []any) slog.Attr {
	a := slog.Group(groups[len(groups)-1], attrs...)
	for i := len(groups) - 2; i >= 0; i-- {
		a = slog.Group(groups[i], a)
	}
	return a
}

func (h *handler) write(ev *zerolog.Event, groups []string, a slog.Attr) {
	a.Value = a.Value.Resolve()
	if a.Value.Kind() != slog.KindGroup {
		if h.cfg.ReplaceAttr != nil {
			a = h.cfg.ReplaceAttr(groups, a)
			a.Value = a.Value.Resolve()
		}
		if a.Equal(slog.Attr{}) {
			return
		}
	}
	switch a.Value.Kind() {
	case slog.KindString:
		ev.Str(a.Key, a.Value.String())
	case slog.KindInt64:
		ev.Int64(a.Key, a.Value.Int64())
	case slog.KindUint64:
		ev.Uint64(a.Key, a.Value.Uint64())
	case slog.KindFloat64:
		ev.Float64(a.Key, a.Value.Float64())
	case slog.KindBool:
		ev.Bool(a.Key, a.Value.Bool())
	case slog.KindDuration:
		ev.Dur(a.Key, a.Value.Duration())
	case slog.KindTime:
		ev.Time(a.Key, a.Value.Time())
	case slog.KindGroup:
		members := a.Value.Group()
		if len(members) == 0 {
			return
		}
		if a.Key == "" { // inline group, as slog's handlers do
			for _, m := range members {
				h.write(ev, groups, m)
			}
			return
		}
		d := zerolog.Dict()
		sub := append(append(make([]string, 0, len(groups)+1), groups...), a.Key)
		for _, m := range members {
			h.write(d, sub, m)
		}
		ev.Dict(a.Key, d)
	default:
		if err, ok := a.Value.Any().(error); ok {
			ev.AnErr(a.Key, err)
			return
		}
		ev.Interface(a.Key, a.Value.Any())
	}
}

// toZerolog maps slog levels. Levels above Error stay Error: Fatal and
// Panic would end the process, which a logging call must never do.
func toZerolog(l slog.Level) zerolog.Level {
	switch {
	case l < slog.LevelDebug:
		return zerolog.TraceLevel
	case l < slog.LevelInfo:
		return zerolog.DebugLevel
	case l < slog.LevelWarn:
		return zerolog.InfoLevel
	case l < slog.LevelError:
		return zerolog.WarnLevel
	default:
		return zerolog.ErrorLevel
	}
}
