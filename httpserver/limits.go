package httpserver

import (
	"context"
	"errors"
	"net/http"
	"time"
)

// MaxBytes caps the request body at n bytes. A declared Content-Length over
// the cap is refused with 413 before any byte is read; a body that grows
// past it fails the handler's read with *http.MaxBytesError (see
// IsRequestTooLarge and WriteError). Put it before anything that reads
// the body.
func MaxBytes(n int64) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.ContentLength > n {
				w.Header().Set("Connection", "close")
				Fail(w, r, http.StatusRequestEntityTooLarge, "The request body is too large.")
				return
			}
			if r.Body != nil && r.Body != http.NoBody {
				r2 := new(http.Request)
				*r2 = *r
				r2.Body = http.MaxBytesReader(w, r.Body, n)
				r = r2
			}
			next.ServeHTTP(w, r)
		})
	}
}

// IsRequestTooLarge reports whether err came from a body over MaxBytes.
func IsRequestTooLarge(err error) bool {
	var mbe *http.MaxBytesError
	return errors.As(err, &mbe)
}

// Timeout sets a deadline on the request context with ErrRequestTimeout as
// cause. Unlike http.TimeoutHandler it does not buffer the response, so
// streaming keeps working; the handler must honour ctx and map the error
// (WriteError answers 504).
func Timeout(d time.Duration) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeoutCause(r.Context(), d, ErrRequestTimeout)
			defer cancel()
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// NoCache marks API responses uncacheable.
func NoCache() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			h := w.Header()
			h.Set("Cache-Control", "no-store")
			h.Set("Expires", "0")
			next.ServeHTTP(w, r)
		})
	}
}
