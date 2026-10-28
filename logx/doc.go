// Package logx builds the *slog.Logger a service hands to every library in
// this repository: JSON by default, the level parsed and validated at boot
// (an unknown level is an error, not a silent "info"), secrets redacted by
// key, and request/trace attributes pulled from the context by hooks the
// service registers (see ContextHandler).
//
// It depends on the standard library only. The zerolog bridge for services
// that keep their logger lives in the nested module logx/zerolog.
package logx
