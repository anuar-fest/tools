package metrics

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
)

// ErrorType classifies err into a low-cardinality error.type value: the
// context sentinels, "timeout" for network timeouts, io.EOF, otherwise the
// Go type name without the pointer star. Hooks with a richer vocabulary
// (SQLSTATE, gRPC codes) use theirs first and fall back to this.
func ErrorType(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.Canceled):
		return "context.Canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "context.DeadlineExceeded"
	case errors.Is(err, io.EOF):
		return "io.EOF"
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return "timeout"
	}
	return strings.TrimLeft(fmt.Sprintf("%T", err), "*")
}
