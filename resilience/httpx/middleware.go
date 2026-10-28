package httpx

import (
	"encoding/json"
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"

	"github.com/anuar-fest/tools/resilience/ratelimit"
	"github.com/anuar-fest/tools/resilience/shed"
)

// Problem writes an RFC 9457 application/problem+json response. retryAfter > 0
// adds a Retry-After header, jittered by up to 20% so clients that were
// refused together do not come back together.
func Problem(w http.ResponseWriter, status int, title, detail string, retryAfter time.Duration) {
	if retryAfter > 0 {
		secs := retryAfter.Seconds() * (1 + 0.2*rand.Float64())
		w.Header().Set("Retry-After", strconv.Itoa(max(1, int(secs+0.5))))
	}
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{ // the client is already gone if this fails
		"type":   "about:blank",
		"title":  title,
		"status": status,
		"detail": detail,
	})
}

// Shed admits requests through s, classified by classify (nil means every
// request is Normal), and answers 503 with Retry-After when shed. Put it
// first in the chain: shedding must cost less than authenticating.
func Shed(s *shed.Shedder, classify func(*http.Request) shed.Class, retryAfter time.Duration) func(http.Handler) http.Handler {
	if classify == nil {
		classify = func(*http.Request) shed.Class { return shed.Normal }
	}
	if retryAfter <= 0 {
		retryAfter = time.Second
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			release, ok := s.Admit(r.Context(), classify(r))
			if !ok {
				Problem(w, http.StatusServiceUnavailable, "Service overloaded", "The server is shedding load; retry after the indicated delay.", retryAfter)
				return
			}
			defer release()
			next.ServeHTTP(w, r)
		})
	}
}

// RateLimit limits requests per key (a tenant, an API key, a client IP as
// seen behind a trusted proxy) and answers 429 with Retry-After. An empty
// key is not limited.
func RateLimit(k *ratelimit.Keyed, keyOf func(*http.Request) string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := keyOf(r)
			if key != "" {
				if ok, retryAfter := k.Allow(key); !ok {
					Problem(w, http.StatusTooManyRequests, "Rate limit exceeded", "Too many requests for this key; retry after the indicated delay.", retryAfter)
					return
				}
			}
			next.ServeHTTP(w, r)
		})
	}
}
