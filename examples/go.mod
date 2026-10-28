module github.com/anuar-fest/tools/examples

go 1.25.0

require (
	github.com/anuar-fest/tools/alarms v0.0.0
	github.com/anuar-fest/tools/events v0.0.0
	github.com/anuar-fest/tools/events/natsjs v0.0.0
	github.com/anuar-fest/tools/events/otel v0.0.0
	github.com/anuar-fest/tools/events/pg v0.0.0
	github.com/anuar-fest/tools/health v0.0.0
	github.com/anuar-fest/tools/httpserver v0.0.0
	github.com/anuar-fest/tools/lifecycle v0.0.0
	github.com/anuar-fest/tools/logx v0.0.0
	github.com/anuar-fest/tools/logx/zerolog v0.0.0
	github.com/anuar-fest/tools/metrics v0.0.0
	github.com/anuar-fest/tools/metrics/grpc v0.0.0-00010101000000-000000000000
	github.com/anuar-fest/tools/metrics/http v0.0.0-00010101000000-000000000000
	github.com/anuar-fest/tools/metrics/nats v0.0.0-00010101000000-000000000000
	github.com/anuar-fest/tools/metrics/pgx v0.0.0
	github.com/anuar-fest/tools/metrics/redis v0.0.0
	github.com/anuar-fest/tools/otelx v0.0.0
	github.com/anuar-fest/tools/otelx/client/grpc v0.0.0-00010101000000-000000000000
	github.com/anuar-fest/tools/otelx/client/nats v0.0.0-00010101000000-000000000000
	github.com/anuar-fest/tools/otelx/client/pgx v0.0.0-00010101000000-000000000000
	github.com/anuar-fest/tools/otelx/client/redis v0.0.0-00010101000000-000000000000
	github.com/anuar-fest/tools/otelx/server/grpc v0.0.0-00010101000000-000000000000
	github.com/anuar-fest/tools/otelx/server/http v0.0.0-00010101000000-000000000000
	github.com/anuar-fest/tools/resilience v0.0.0
	github.com/anuar-fest/tools/resilience/otel v0.0.0
	github.com/jackc/pgx/v5 v5.11.0
	github.com/nats-io/nats.go v1.51.0
	github.com/redis/go-redis/v9 v9.14.0
	github.com/rs/zerolog v1.35.1
	go.opentelemetry.io/otel v1.45.0
	go.opentelemetry.io/otel/exporters/stdout/stdouttrace v1.45.0
	go.opentelemetry.io/otel/sdk v1.45.0
	go.opentelemetry.io/otel/sdk/metric v1.45.0
	go.opentelemetry.io/otel/trace v1.45.0
	google.golang.org/grpc v1.83.0
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cenkalti/backoff/v5 v5.0.3 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.29.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/klauspost/compress v1.19.2 // indirect
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/nats-io/nkeys v0.4.16 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/prometheus/client_golang v1.24.1 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.70.1 // indirect
	github.com/prometheus/otlptranslator v1.0.0 // indirect
	github.com/prometheus/procfs v0.21.1 // indirect
	github.com/stretchr/testify v1.12.1 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp v1.45.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.45.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.45.0 // indirect
	go.opentelemetry.io/otel/exporters/prometheus v0.67.0 // indirect
	go.opentelemetry.io/otel/metric v1.45.0 // indirect
	go.opentelemetry.io/proto/otlp v1.11.0 // indirect
	golang.org/x/crypto v0.55.0 // indirect
	golang.org/x/net v0.57.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260803160001-6ac0973c030d // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260803160001-6ac0973c030d // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

// Siblings in this repository; a real service imports tagged versions.
replace (
	github.com/anuar-fest/tools/alarms => ../alarms
	github.com/anuar-fest/tools/events => ../events
	github.com/anuar-fest/tools/events/natsjs => ../events/natsjs
	github.com/anuar-fest/tools/events/otel => ../events/otel
	github.com/anuar-fest/tools/events/pg => ../events/pg
	github.com/anuar-fest/tools/health => ../health
	github.com/anuar-fest/tools/httpserver => ../httpserver
	github.com/anuar-fest/tools/lifecycle => ../lifecycle
	github.com/anuar-fest/tools/logx => ../logx
	github.com/anuar-fest/tools/logx/zerolog => ../logx/zerolog
	github.com/anuar-fest/tools/metrics => ../metrics
	github.com/anuar-fest/tools/metrics/grpc => ../metrics/grpc
	github.com/anuar-fest/tools/metrics/http => ../metrics/http
	github.com/anuar-fest/tools/metrics/nats => ../metrics/nats
	github.com/anuar-fest/tools/metrics/pgx => ../metrics/pgx
	github.com/anuar-fest/tools/metrics/redis => ../metrics/redis
	github.com/anuar-fest/tools/otelx => ../otelx
	github.com/anuar-fest/tools/otelx/client/grpc => ../otelx/client/grpc
	github.com/anuar-fest/tools/otelx/client/nats => ../otelx/client/nats
	github.com/anuar-fest/tools/otelx/client/pgx => ../otelx/client/pgx
	github.com/anuar-fest/tools/otelx/client/redis => ../otelx/client/redis
	github.com/anuar-fest/tools/otelx/server/grpc => ../otelx/server/grpc
	github.com/anuar-fest/tools/otelx/server/http => ../otelx/server/http
	github.com/anuar-fest/tools/resilience => ../resilience
	github.com/anuar-fest/tools/resilience/otel => ../resilience/otel
)
