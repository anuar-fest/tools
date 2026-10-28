package logx_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/anuar-fest/tools/logx"
)

func decode(t *testing.T, line string) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("not json: %q: %v", line, err)
	}
	return m
}

func TestRedact_ByKeyAtAnyDepth(t *testing.T) {
	var buf bytes.Buffer
	log, err := logx.New(logx.Config{Output: &buf, RedactKeys: []string{"ssn"}})
	if err != nil {
		t.Fatal(err)
	}
	log.Info("login", "user", "alice", "Password", "hunter2", slog.Group("req", "api-key", "k1", "ssn", "123"), "token", 42)
	m := decode(t, buf.String())
	if m["user"] != "alice" {
		t.Fatalf("user must stay: %v", m)
	}
	if m["Password"] != logx.Redacted || m["token"] != logx.Redacted {
		t.Fatalf("top-level secrets must be redacted: %v", m)
	}
	req := m["req"].(map[string]any)
	if req["api-key"] != logx.Redacted || req["ssn"] != logx.Redacted {
		t.Fatalf("nested secrets must be redacted: %v", req)
	}
}

func TestRedact_Disabled(t *testing.T) {
	var buf bytes.Buffer
	log, _ := logx.New(logx.Config{Output: &buf, DisableRedaction: true})
	log.Info("x", "password", "p")
	if decode(t, buf.String())["password"] != "p" {
		t.Fatal("redaction must be off")
	}
}

func TestSecret_NeverPrints(t *testing.T) {
	s := logx.Secret("s3cr3t")
	for name, got := range map[string]string{
		"%v":  fmt.Sprintf("%v", s),
		"%s":  fmt.Sprintf("<%s>", s),
		"%q":  fmt.Sprintf("%q", s),
		"%+v": fmt.Sprintf("%+v", struct{ P logx.Secret }{s}),
		"%#v": fmt.Sprintf("%#v", s),
	} {
		if strings.Contains(got, "s3cr3t") {
			t.Errorf("%s leaked: %s", name, got)
		}
	}
	b, _ := json.Marshal(map[string]any{"p": s})
	if strings.Contains(string(b), "s3cr3t") {
		t.Errorf("json leaked: %s", b)
	}
	var buf bytes.Buffer
	log, _ := logx.New(logx.Config{Output: &buf, DisableRedaction: true})
	log.Info("x", "cfg", s, slog.Any("nested", struct{ S logx.Secret }{s}))
	if strings.Contains(buf.String(), "s3cr3t") {
		t.Errorf("slog leaked: %s", buf.String())
	}
	if s.Reveal() != "s3cr3t" {
		t.Fatal("Reveal must return the value")
	}
	var parsed logx.Secret
	if err := parsed.UnmarshalText([]byte("v")); err != nil || parsed.Reveal() != "v" {
		t.Fatal("UnmarshalText must set the value")
	}
}

type ctxKey struct{}

func TestContextHandler_AddsAttrsFromContext(t *testing.T) {
	var buf bytes.Buffer
	extract := func(ctx context.Context) []slog.Attr {
		if v, ok := ctx.Value(ctxKey{}).(string); ok {
			return []slog.Attr{slog.String("request_id", v)}
		}
		return nil
	}
	log, err := logx.New(logx.Config{Output: &buf, ContextAttrs: []logx.ContextAttrs{extract, nil}, Attrs: []slog.Attr{slog.String("service", "s")}})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.WithValue(context.Background(), ctxKey{}, "r-1")
	log.InfoContext(ctx, "with")
	log.Info("without")
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	with, without := decode(t, lines[0]), decode(t, lines[1])
	if with["request_id"] != "r-1" || with["service"] != "s" {
		t.Fatalf("context attrs missing: %v", with)
	}
	if _, ok := without["request_id"]; ok {
		t.Fatalf("no context, no attr: %v", without)
	}
	buf.Reset()
	log.WithGroup("g").With("k", "v").InfoContext(ctx, "grouped")
	g := decode(t, buf.String())["g"].(map[string]any)
	if g["k"] != "v" || g["request_id"] != "r-1" {
		t.Fatalf("WithGroup/WithAttrs must pass through: %v", g)
	}
}

func TestParseLevel(t *testing.T) {
	cases := []struct {
		in   string
		want slog.Level
	}{{"debug", slog.LevelDebug}, {"INFO", slog.LevelInfo}, {" warn ", slog.LevelWarn}, {"warning", slog.LevelWarn}, {"error", slog.LevelError}, {"fatal", slog.LevelError}, {"info+2", slog.LevelInfo + 2}}
	for _, c := range cases {
		in, want := c.in, c.want
		got, err := logx.ParseLevel(in)
		if err != nil || got != want {
			t.Errorf("ParseLevel(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	for _, bad := range []string{"", "verbose", "inf"} {
		if _, err := logx.ParseLevel(bad); err == nil {
			t.Errorf("ParseLevel(%q) must fail", bad)
		}
	}
}

func TestNew_LevelVarAndFormat(t *testing.T) {
	var buf bytes.Buffer
	var lv slog.LevelVar
	lv.Set(slog.LevelWarn)
	log, err := logx.New(logx.Config{Output: &buf, Level: &lv, Format: logx.FormatText})
	if err != nil {
		t.Fatal(err)
	}
	log.Info("dropped")
	lv.Set(slog.LevelDebug)
	log.Debug("kept", "password", "x")
	out := buf.String()
	if strings.Contains(out, "dropped") || !strings.Contains(out, "kept") || !strings.Contains(out, "password="+logx.Redacted) {
		t.Fatalf("level var / text format: %q", out)
	}
	if _, err := logx.New(logx.Config{Format: "yaml"}); err == nil {
		t.Fatal("unknown format must fail")
	}
}

func TestNew_ReplaceAttrRunsAfterRedaction(t *testing.T) {
	var buf bytes.Buffer
	log, _ := logx.New(logx.Config{Output: &buf, ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
		if a.Key == slog.MessageKey {
			a.Key = "message"
		}
		return a
	}})
	log.Info("hi", "password", "p")
	m := decode(t, buf.String())
	if m["message"] != "hi" || m["password"] != logx.Redacted {
		t.Fatalf("%v", m)
	}
}
