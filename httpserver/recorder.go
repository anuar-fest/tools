package httpserver

import (
	"bufio"
	"io"
	"net"
	"net/http"
)

// ResponseRecorder is the http.ResponseWriter the middleware in this package
// share: one wrapper per request records the status and the bytes written,
// so access log, metrics, recover and tracing agree on what was sent.
type ResponseRecorder interface {
	http.ResponseWriter
	// Status is the status written, or 0 while nothing was written.
	Status() int
	// BytesWritten counts response body bytes so far.
	BytesWritten() int64
	// Hijacked reports whether the connection was taken over (WebSocket).
	Hijacked() bool
	// Panicked reports whether Recover caught a panic on this request.
	Panicked() bool
}

// Record returns w as a ResponseRecorder, reusing the one an outer
// middleware already installed so there is one view of the response.
func Record(w http.ResponseWriter) ResponseRecorder {
	if rec, ok := w.(*recorder); ok {
		return rec
	}
	return &recorder{ResponseWriter: w}
}

type recorder struct {
	http.ResponseWriter
	status      int
	bytes       int64
	wroteHeader bool
	hijacked    bool
	panicked    bool
}

func (r *recorder) Status() int         { return r.status }
func (r *recorder) BytesWritten() int64 { return r.bytes }
func (r *recorder) Hijacked() bool      { return r.hijacked }
func (r *recorder) Panicked() bool      { return r.panicked }

func (r *recorder) WriteHeader(code int) {
	// 1xx (Early Hints) are sent at once and a final status still follows.
	if code >= 100 && code < 200 && code != http.StatusSwitchingProtocols {
		r.ResponseWriter.WriteHeader(code)
		return
	}
	if !r.wroteHeader {
		r.status = code
		r.wroteHeader = true
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *recorder) Write(b []byte) (int, error) {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
	n, err := r.ResponseWriter.Write(b)
	r.bytes += int64(n)
	return n, err
}

// ReadFrom keeps net/http's sendfile path for large bodies.
func (r *recorder) ReadFrom(src io.Reader) (int64, error) {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
	var n int64
	var err error
	if rf, ok := r.ResponseWriter.(io.ReaderFrom); ok {
		n, err = rf.ReadFrom(src)
	} else {
		n, err = io.Copy(r.ResponseWriter, src)
	}
	r.bytes += n
	return n, err
}

// Flush implements http.Flusher through the Unwrap chain.
func (r *recorder) Flush() {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
	// Flusher has no error path; a writer that cannot flush is a no-op here.
	_ = http.NewResponseController(r.ResponseWriter).Flush()
}

// Hijack implements http.Hijacker so WebSocket upgrades work behind the
// middleware chain.
func (r *recorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	c, rw, err := http.NewResponseController(r.ResponseWriter).Hijack()
	if err == nil {
		r.hijacked = true
	}
	return c, rw, err
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (r *recorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

// StatusClientClosedRequest is nginx's 499: the client left before a
// response was written.
const StatusClientClosedRequest = 499

// StatusOf is the status the access log, metrics and tracing report. When
// the handler wrote nothing it is 101 after a hijack, 500 after a panic,
// 499 when the client went away, otherwise 200 (what net/http sends for an
// empty response).
func StatusOf(rec ResponseRecorder, r *http.Request) int {
	if s := rec.Status(); s != 0 {
		return s
	}
	switch {
	case rec.Hijacked():
		return http.StatusSwitchingProtocols
	case rec.Panicked():
		return http.StatusInternalServerError
	case r.Context().Err() != nil:
		return StatusClientClosedRequest
	default:
		return http.StatusOK
	}
}

func markPanicked(rec ResponseRecorder) {
	if rr, ok := rec.(*recorder); ok {
		rr.panicked = true
	}
}
