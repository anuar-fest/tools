module github.com/anuar-fest/tools/alarms

go 1.25.0

require (
	github.com/anuar-fest/tools/events v0.0.0
	github.com/anuar-fest/tools/health v0.0.0
	github.com/anuar-fest/tools/resilience v0.0.0
	go.opentelemetry.io/otel v1.45.0
	go.opentelemetry.io/otel/metric v1.45.0
	go.opentelemetry.io/otel/sdk v1.45.0
	go.opentelemetry.io/otel/sdk/metric v1.45.0
	go.opentelemetry.io/otel/trace v1.45.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	golang.org/x/sys v0.47.0 // indirect
)

// Core modules live in this repo; consumers get the tagged versions.
replace (
	github.com/anuar-fest/tools/events => ../events
	github.com/anuar-fest/tools/health => ../health
	github.com/anuar-fest/tools/resilience => ../resilience
)
