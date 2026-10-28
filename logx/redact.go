package logx

import (
	"log/slog"
	"strings"
)

// Redacted replaces every redacted value.
const Redacted = "[REDACTED]"

// DefaultRedactKeys are attribute keys whose values never reach a log line.
// Matching is case-insensitive and ignores "-" vs "_", so "api-key",
// "API_KEY" and "apiKey" all match "api_key".
var DefaultRedactKeys = []string{
	"password", "passwd", "pwd", "secret", "token", "access_token", "refresh_token",
	"id_token", "authorization", "api_key", "apikey", "x_api_key", "cookie",
	"set_cookie", "private_key", "client_secret", "credentials", "credential",
	"secret_access_key", "dsn", "connection_string",
}

// Secret is a string that never prints. fmt, encoding/json, encoding/text
// and slog all see Redacted; only Reveal returns the value. Use it for every
// credential in a config struct so a dumped config cannot leak.
type Secret string

// Reveal returns the underlying value. Call it at the point of use, never
// to build a log line.
func (s Secret) Reveal() string { return string(s) }

// IsZero reports whether the secret is empty.
func (s Secret) IsZero() bool { return s == "" }

func (s Secret) String() string   { return Redacted }
func (s Secret) GoString() string { return "logx.Secret(" + Redacted + ")" }

// LogValue implements slog.LogValuer.
func (Secret) LogValue() slog.Value { return slog.StringValue(Redacted) }

// MarshalJSON implements json.Marshaler.
func (Secret) MarshalJSON() ([]byte, error) { return []byte(`"` + Redacted + `"`), nil }

// MarshalText implements encoding.TextMarshaler.
func (Secret) MarshalText() ([]byte, error) { return []byte(Redacted), nil }

// UnmarshalText implements encoding.TextUnmarshaler so config loaders can
// fill a Secret from env or a file.
func (s *Secret) UnmarshalText(b []byte) error {
	*s = Secret(b)
	return nil
}

// Redact returns a slog.HandlerOptions.ReplaceAttr function that replaces
// the value of any attribute whose key matches one of keys, at any group
// depth. Group attributes are traversed by the handler, so only leaves are
// seen here.
func Redact(keys ...string) func(groups []string, a slog.Attr) slog.Attr {
	set := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		set[normalizeKey(k)] = struct{}{}
	}
	return func(_ []string, a slog.Attr) slog.Attr {
		if a.Value.Kind() == slog.KindGroup {
			return a
		}
		if _, hit := set[normalizeKey(a.Key)]; hit {
			return slog.String(a.Key, Redacted)
		}
		return a
	}
}

func normalizeKey(k string) string {
	return strings.ReplaceAll(strings.ToLower(k), "-", "_")
}
