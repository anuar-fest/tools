package logx

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
)

// Format selects the handler encoding.
type Format string

const (
	// FormatJSON is one JSON object per line (the default; what log
	// collectors parse).
	FormatJSON Format = "json"
	// FormatText is slog's key=value text, for developer machines.
	FormatText Format = "text"
)

// Config configures New. The zero value is a JSON logger at Info on stdout
// with the default redaction list.
type Config struct {
	// Level is the minimum level. Pass a *slog.LevelVar to change it at
	// runtime. nil means Info. See ParseLevel for strings from config.
	Level slog.Leveler
	// Format is FormatJSON or FormatText.
	Format Format
	// Output defaults to os.Stdout: collectors read the container's stdout.
	Output io.Writer
	// AddSource adds file:line of the log call.
	AddSource bool
	// RedactKeys extends DefaultRedactKeys. Matching is case-insensitive on
	// the attribute key, at any group depth.
	RedactKeys []string
	// DisableRedaction turns key-based redaction off. Secret values still
	// never print.
	DisableRedaction bool
	// ContextAttrs are hooks that read attributes from the context of
	// *Context log calls: request id, trace id, tenant. See ContextHandler.
	ContextAttrs []ContextAttrs
	// Attrs are added to every record: service, version, environment.
	Attrs []slog.Attr
	// ReplaceAttr runs after redaction, for renames such as msg → message.
	ReplaceAttr func(groups []string, a slog.Attr) slog.Attr
}

// New builds the logger. It fails on an unknown Format so a typo in config
// is caught at boot.
func New(cfg Config) (*slog.Logger, error) {
	out := cfg.Output
	if out == nil {
		out = os.Stdout
	}
	var level slog.Leveler = slog.LevelInfo
	if cfg.Level != nil {
		level = cfg.Level
	}
	opts := &slog.HandlerOptions{
		Level:       level,
		AddSource:   cfg.AddSource,
		ReplaceAttr: composeReplace(cfg),
	}
	var h slog.Handler
	switch cfg.Format {
	case "", FormatJSON:
		h = slog.NewJSONHandler(out, opts)
	case FormatText:
		h = slog.NewTextHandler(out, opts)
	default:
		return nil, fmt.Errorf("logx: unknown format %q (use %q or %q)", cfg.Format, FormatJSON, FormatText)
	}
	if len(cfg.ContextAttrs) > 0 {
		h = ContextHandler(h, cfg.ContextAttrs...)
	}
	if len(cfg.Attrs) > 0 {
		h = h.WithAttrs(cfg.Attrs)
	}
	return slog.New(h), nil
}

func composeReplace(cfg Config) func([]string, slog.Attr) slog.Attr {
	var redact func([]string, slog.Attr) slog.Attr
	if !cfg.DisableRedaction {
		redact = Redact(append(append([]string(nil), DefaultRedactKeys...), cfg.RedactKeys...)...)
	}
	switch {
	case redact == nil && cfg.ReplaceAttr == nil:
		return nil
	case redact == nil:
		return cfg.ReplaceAttr
	case cfg.ReplaceAttr == nil:
		return redact
	default:
		return func(groups []string, a slog.Attr) slog.Attr {
			return cfg.ReplaceAttr(groups, redact(groups, a))
		}
	}
}

// ParseLevel reads a level from configuration. It accepts debug, info, warn,
// warning, error and fatal (an alias of error), case-insensitively, plus
// slog's own forms such as "INFO+2". It fails on anything else: a service
// must not run at the wrong level because of a typo.
func ParseLevel(s string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "":
		return 0, errors.New("logx: empty log level")
	case "debug", "trace":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error", "fatal", "panic":
		return slog.LevelError, nil
	}
	var l slog.Level
	if err := l.UnmarshalText([]byte(strings.ToUpper(strings.TrimSpace(s)))); err != nil {
		return 0, fmt.Errorf("logx: unknown log level %q", s)
	}
	return l, nil
}
