package httpserver

import (
	"context"
	"crypto/rand"
	"net/http"
)

// HeaderRequestID is the default request id header.
const HeaderRequestID = "X-Request-Id"

// RequestIDConfig configures RequestID. The zero value trusts a valid
// inbound X-Request-Id and generates one otherwise.
type RequestIDConfig struct {
	// Header defaults to HeaderRequestID.
	Header string
	// IgnoreInbound always generates a new id. Set it on public ingress that
	// no gateway fronts; keep it off behind Traefik or Istio so one id spans
	// the gateway and every service.
	IgnoreInbound bool
}

type requestIDKey struct{}

// RequestID gives every request an id, in the context and in the response
// header. Inbound ids are kept only if they are 1 to 128 characters of
// [A-Za-z0-9._:/-]; anything else is replaced.
func RequestID(cfg RequestIDConfig) func(http.Handler) http.Handler {
	header := cfg.Header
	if header == "" {
		header = HeaderRequestID
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			id := ""
			if !cfg.IgnoreInbound {
				if in := r.Header.Get(header); validRequestID(in) {
					id = in
				}
			}
			if id == "" {
				id = rand.Text()
			}
			w.Header().Set(header, id)
			next.ServeHTTP(w, r.WithContext(WithRequestID(r.Context(), id)))
		})
	}
}

// RequestIDFrom returns the request id in ctx, or "".
func RequestIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey{}).(string)
	return id
}

// WithRequestID returns ctx carrying id. Consumers and workers use it to
// continue a correlation that arrived in an event.
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey{}, id)
}

func validRequestID(s string) bool {
	if len(s) == 0 || len(s) > 128 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '.' || c == '_' || c == ':' || c == '/' || c == '-':
		default:
			return false
		}
	}
	return true
}
