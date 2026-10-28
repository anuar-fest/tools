// Package metrics installs the OpenTelemetry metrics SDK once per process
// and is the toolbox every metrics hook in this repository shares: the
// meter provider with a Prometheus pull exporter (Go and process collectors
// included) or an OTLP push exporter, the resource (service name, version,
// environment), the standard bucket sets, a cardinality guard expressed as
// a metric View, a builder for business metrics with closed label sets, and
// the low-cardinality error classifier.
//
// Hooks live one per integration under metrics/<system> (http, grpc, pgx,
// sql, redis, nats, runtime). Each records through the OpenTelemetry API
// on the shared buckets, so one dashboard fits every integration. Traces
// are the otelx module's job; the two are wired independently and share
// the resource through Config.Resource.
//
// The OpenTelemetry line is pinned to the newest that still builds with
// Go 1.25 (v1.45.x, Prometheus exporter v0.67.x); bump it with the
// consumers' toolchain.
package metrics
