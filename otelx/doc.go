// Package otelx installs the OpenTelemetry tracing SDK once per process:
// the resource (service name, version, environment), a tracer provider
// that exports over OTLP/HTTP with parent-based sampling, the W3C
// propagators, and one Shutdown that flushes the last spans.
//
// Everything else in this repository instruments through the OpenTelemetry
// API and never touches the SDK: otelx/server/* and otelx/client/* trace
// the servers you own and the systems you call; resilience/otel and
// events/otel implement this repository's own observer ports. LogAttrs
// stamps trace ids on log lines through logx.ContextHandler.
//
// Metrics are the metrics module's job (metrics.New builds the meter
// provider; metrics/<system> holds one hook per integration). The two are
// wired independently; pass Telemetry.Resource to metrics.Config.Resource
// so both signals carry the same identity.
//
// The OpenTelemetry line is pinned to the newest that still builds with
// Go 1.25 (v1.45.x); bump it with the consumers' toolchain.
package otelx
