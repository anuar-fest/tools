// Package lifecycle runs a service's long-lived components and stops them
// in order when a signal arrives, the parent context ends, or one of them
// fails. Every component has an owner, a cancellation path and a join:
//
//	signal → OnShutdown (mark unready) → PreStopDelay → for each component,
//	last added first: Stop(ctx) → cancel Run's context → wait for Run to return
//
// under one budget sized to the pod's termination grace period. A
// component that does not stop in time is reported, not waited for
// forever; a second signal gives up waiting at once. The exit code tells
// the supervisor whether the shutdown was clean.
//
// Add components in dependency order: telemetry and the database first (they
// stop last), consumers and relays next, the HTTP server last (it stops
// first, so no request arrives after its dependencies are gone).
package lifecycle
