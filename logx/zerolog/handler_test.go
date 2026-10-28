package logxzerolog_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	logxzerolog "github.com/anuar-fest/tools/logx/zerolog"
)

type secret string

func (secret) LogValue() slog.Value { return slog.StringValue("[REDACTED]") }

func newLogger(buf *bytes.Buffer, cfg logxzerolog.Config) *slog.Logger {
	cfg.Logger = zerolog.New(buf)
	return slog.New(logxzerolog.NewHandler(cfg))
}

func decode(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(buf.Bytes(), &m); err != nil {
		t.Fatalf("not json: %q: %v", buf.String(), err)
	}
	buf.Reset()
	return m
}

func TestHandler_LevelsKindsGroupsAndValuers(t *testing.T) {
	var buf bytes.Buffer
	log := newLogger(&buf, logxzerolog.Config{})
	log.Warn("hello", "s", "v", "n", 42, "f", 1.5, "b", true, "d", time.Second, "err", errors.New("boom"), "pw", secret("x"),
		slog.Group("req", "id", "r1", slog.Group("peer", "ip", "1.2.3.4")))
	m := decode(t, &buf)
	if m["level"] != "warn" || m["message"] != "hello" || m["s"] != "v" || m["n"] != float64(42) || m["f"] != 1.5 || m["b"] != true || m["err"] != "boom" || m["pw"] != "[REDACTED]" {
		t.Fatalf("%v", m)
	}
	req := m["req"].(map[string]any)
	if req["id"] != "r1" || req["peer"].(map[string]any)["ip"] != "1.2.3.4" {
		t.Fatalf("groups must nest as dicts: %v", req)
	}
	for in, want := range map[slog.Level]string{slog.LevelDebug: "debug", slog.LevelInfo: "info", slog.LevelError: "error", slog.LevelError + 4: "error", slog.LevelDebug - 4: "trace"} {
		log.Log(t.Context(), in, "l")
		if got := decode(t, &buf)["level"]; got != want {
			t.Errorf("level %v → %v, want %v", in, got, want)
		}
	}
}

func TestHandler_WithAttrsAndWithGroupCompose(t *testing.T) {
	var buf bytes.Buffer
	log := newLogger(&buf, logxzerolog.Config{})
	log.With("service", "svc").WithGroup("http").With("method", "GET").WithGroup("peer").Info("served", "ip", "::1")
	m := decode(t, &buf)
	if m["service"] != "svc" {
		t.Fatalf("root attr: %v", m)
	}
	h := m["http"].(map[string]any)
	if h["method"] != "GET" || h["peer"].(map[string]any)["ip"] != "::1" {
		t.Fatalf("nested groups: %v", m)
	}
}

func TestHandler_EnabledAndReplaceAttr(t *testing.T) {
	var buf bytes.Buffer
	cfg := logxzerolog.Config{Level: slog.LevelInfo, ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
		if a.Key == "password" {
			return slog.String(a.Key, "***")
		}
		if a.Key == "drop" {
			return slog.Attr{}
		}
		return a
	}}
	log := newLogger(&buf, cfg)
	log.Debug("hidden")
	if buf.Len() != 0 {
		t.Fatalf("debug below Level must be dropped: %s", buf.String())
	}
	log.Info("x", "password", "p", "drop", "d", slog.Group("g", "password", "q"))
	m := decode(t, &buf)
	if m["password"] != "***" || m["g"].(map[string]any)["password"] != "***" {
		t.Fatalf("ReplaceAttr at every depth: %v", m)
	}
	if _, ok := m["drop"]; ok {
		t.Fatal("zero attr must be dropped")
	}
	cfg.Logger = zerolog.New(&buf).Level(zerolog.ErrorLevel)
	log = slog.New(logxzerolog.NewHandler(cfg))
	if log.Enabled(t.Context(), slog.LevelWarn) || !log.Enabled(t.Context(), slog.LevelError) {
		t.Fatal("zerolog's own level must be honoured")
	}
}

func TestHandler_AddSource(t *testing.T) {
	var buf bytes.Buffer
	log := newLogger(&buf, logxzerolog.Config{AddSource: true})
	log.Info("x")
	if !strings.Contains(buf.String(), `"caller":"`) || !strings.Contains(buf.String(), "handler_test.go:") {
		t.Fatalf("%s", buf.String())
	}
}
