package httpserver

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"log/slog"
	"maps"
	"net/http"
	"slices"
	"strings"
)

// APIKeyConfig configures APIKey.
type APIKeyConfig struct {
	// Header holding the key; default Authorization.
	Header string
	// Scheme prefix in the header, default "APIKEY" ("APIKEY <key>").
	// Empty means the header holds the bare key (X-Api-Key style).
	Scheme string
	// Keys maps a consumer name to its key. Rotate by adding the new key
	// under the same name with a suffix ("billing", "billing-next").
	Keys map[string]string
	// SharedKey is accepted with an empty consumer name. Prefer Keys: a
	// shared key cannot be revoked for one caller.
	SharedKey string
	// Skip bypasses authentication for a request (public routes, probes).
	Skip func(*http.Request) bool
	// Logger logs rejected requests at Warn without the presented key.
	Logger *slog.Logger
}

type consumerKey struct{}

type credential struct {
	digest   [sha256.Size]byte
	consumer string
}

// APIKey authenticates service callers. Keys are compared as SHA-256
// digests in constant time, and every candidate is checked so a wrong key
// costs the same wherever it diverged. The consumer name is put in the
// context (ConsumerFrom) and in log lines through LogAttrs.
func APIKey(cfg APIKeyConfig) (func(http.Handler) http.Handler, error) {
	header := cfg.Header
	if header == "" {
		header = "Authorization"
	}
	scheme := cfg.Scheme
	if cfg.Header == "" && cfg.Scheme == "" {
		scheme = "APIKEY"
	}
	creds := make([]credential, 0, len(cfg.Keys)+1)
	for _, name := range slices.Sorted(maps.Keys(cfg.Keys)) {
		key := cfg.Keys[name]
		if name == "" || key == "" {
			return nil, errors.New("httpserver: APIKey: empty consumer name or key")
		}
		creds = append(creds, credential{digest: sha256.Sum256([]byte(key)), consumer: name})
	}
	if cfg.SharedKey != "" {
		creds = append(creds, credential{digest: sha256.Sum256([]byte(cfg.SharedKey))})
	}
	if len(creds) == 0 {
		return nil, errors.New("httpserver: APIKey: no keys configured")
	}
	logger := orDiscard(cfg.Logger)
	challenge := scheme
	if challenge == "" {
		challenge = "ApiKey"
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if cfg.Skip != nil && cfg.Skip(r) {
				next.ServeHTTP(w, r)
				return
			}
			presented, ok := presentedKey(r.Header.Get(header), scheme)
			if ok {
				var consumer string
				consumer, ok = match(creds, presented)
				if ok {
					next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), consumerKey{}, consumer)))
					return
				}
			}
			logger.WarnContext(r.Context(), "api key rejected", "method", r.Method, "path", r.URL.Path, "client_ip", ClientIP(r))
			w.Header().Set("WWW-Authenticate", challenge)
			Fail(w, r, http.StatusUnauthorized, "A valid API key is required.")
		})
	}, nil
}

func presentedKey(value, scheme string) (string, bool) {
	if scheme == "" {
		return value, value != ""
	}
	if len(value) <= len(scheme)+1 || !strings.EqualFold(value[:len(scheme)], scheme) || value[len(scheme)] != ' ' {
		return "", false
	}
	key := strings.TrimSpace(value[len(scheme)+1:])
	return key, key != ""
}

func match(creds []credential, key string) (string, bool) {
	presented := sha256.Sum256([]byte(key))
	consumer, accepted := "", false
	for _, c := range creds {
		if subtle.ConstantTimeCompare(c.digest[:], presented[:]) == 1 {
			accepted = true
			if consumer == "" {
				consumer = c.consumer
			}
		}
	}
	return consumer, accepted
}

// ConsumerFrom returns the consumer name behind the API key, or "" for an
// unauthenticated request or the shared key.
func ConsumerFrom(ctx context.Context) string {
	c, _ := ctx.Value(consumerKey{}).(string)
	return c
}
