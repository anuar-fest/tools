package httpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
)

// Problem is an RFC 9457 error body. It is also an error, so a handler can
// return it and let WriteError render it.
//
//nolint:errname // RFC 9457 calls it a problem; it is the wire shape first and an error second.
type Problem struct {
	// Type is a URI for the error class; "about:blank" means the status says it all.
	Type string
	// Title is a short human summary; defaults to the status text.
	Title string
	// Status is the HTTP status.
	Status int
	// Detail explains this occurrence. Never put internal details here.
	Detail string
	// Instance identifies the occurrence; defaults to the request path.
	Instance string
	// Extensions are extra members, e.g. "errors" for field validation.
	Extensions map[string]any
}

// NewProblem builds a Problem for status with the standard title.
func NewProblem(status int, detail string) Problem {
	return Problem{Type: "about:blank", Title: http.StatusText(status), Status: status, Detail: detail}
}

// With returns a copy with one extension member set.
func (p Problem) With(key string, value any) Problem {
	ext := make(map[string]any, len(p.Extensions)+1)
	for k, v := range p.Extensions {
		ext[k] = v
	}
	ext[key] = value
	p.Extensions = ext
	return p
}

func (p Problem) Error() string {
	if p.Detail == "" {
		return strconv.Itoa(p.Status) + " " + p.Title
	}
	return fmt.Sprintf("%d %s: %s", p.Status, p.Title, p.Detail)
}

// MarshalJSON flattens Extensions next to the standard members.
func (p Problem) MarshalJSON() ([]byte, error) {
	m := make(map[string]any, 5+len(p.Extensions))
	for k, v := range p.Extensions {
		m[k] = v
	}
	m["type"] = p.Type
	m["title"] = p.Title
	m["status"] = p.Status
	if p.Detail != "" {
		m["detail"] = p.Detail
	}
	if p.Instance != "" {
		m["instance"] = p.Instance
	}
	return json.Marshal(m)
}

// WriteProblem writes p as application/problem+json with the request id
// as an extension member. Nothing is written if the response has started.
func WriteProblem(w http.ResponseWriter, r *http.Request, p Problem) {
	if rec, ok := w.(ResponseRecorder); ok && (rec.Status() != 0 || rec.Hijacked()) {
		return
	}
	if p.Type == "" {
		p.Type = "about:blank"
	}
	if p.Title == "" {
		p.Title = http.StatusText(p.Status)
	}
	if p.Status == 0 {
		p.Status = http.StatusInternalServerError
	}
	if r != nil {
		if p.Instance == "" && r.URL != nil {
			p.Instance = r.URL.Path
		}
		if id := RequestIDFrom(r.Context()); id != "" {
			if _, has := p.Extensions["request_id"]; !has {
				p = p.With("request_id", id)
			}
		}
	}
	body, err := json.Marshal(p)
	if err != nil {
		body = []byte(`{"type":"about:blank","title":"Internal Server Error","status":500}`)
		p.Status = http.StatusInternalServerError
	}
	h := w.Header()
	h.Set("Content-Type", "application/problem+json")
	h.Set("Cache-Control", "no-store")
	h.Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(p.Status)
	// The client is gone if this fails; there is nobody left to tell.
	_, _ = w.Write(body)
}

// Fail writes a Problem for status with detail.
func Fail(w http.ResponseWriter, r *http.Request, status int, detail string) {
	WriteProblem(w, r, NewProblem(status, detail))
}

// ErrRequestTimeout is the cause of a context deadline set by Timeout.
var ErrRequestTimeout = errors.New("httpserver: request timeout")

// WriteError maps err to a response: a Problem is written as is, a body
// over MaxBytes is 413, a request that hit Timeout is 504, a client that
// went away gets nothing, and anything else is a generic 500. The error's
// text never reaches the client.
func WriteError(w http.ResponseWriter, r *http.Request, err error) {
	var p Problem
	switch {
	case errors.As(err, &p):
		WriteProblem(w, r, p)
	case IsRequestTooLarge(err):
		Fail(w, r, http.StatusRequestEntityTooLarge, "The request body is too large.")
	case errors.Is(err, ErrRequestTimeout), errors.Is(context.Cause(r.Context()), ErrRequestTimeout):
		Fail(w, r, http.StatusGatewayTimeout, "The request took too long to complete.")
	case r.Context().Err() != nil:
		// Client is gone; a response would be written to nobody.
	default:
		Fail(w, r, http.StatusInternalServerError, "The server could not complete the request.")
	}
}
