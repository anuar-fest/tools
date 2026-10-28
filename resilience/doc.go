// Package resilience holds the vocabulary shared by every policy in this module:
// the low-cardinality Decision an operation ends with, the Observer port that
// adapters (OpenTelemetry, logs) implement, the sentinel errors policies return,
// and the Clock port tests substitute.
//
// The policies themselves live in sub-packages (retry, breaker, bulkhead,
// deadline, ratelimit, shed, workpool, hedge, coalesce) and compose in a fixed
// order inside package stack:
//
//	deadline → bulkhead → limit → retry( breaker( attempt-timeout( op ) ) )
//
// This package imports only the standard library on purpose: it is what every
// adapter module depends on, so it must never pull an SDK with it.
package resilience
