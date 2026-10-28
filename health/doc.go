// Package health separates liveness ("restart me") from readiness ("send me
// traffic"). Liveness checks look only inward (a deadlocked worker, an
// exhausted pool); readiness checks the dependencies a request needs (the
// database, the broker) and goes false on purpose while the process drains,
// so the load balancer stops sending work before the listener closes.
//
// Checks run concurrently under one timeout, results are cached briefly so
// several probers cost one evaluation, and a check that panics fails
// instead of killing the process. A Check is func(ctx) error, so
// db.PingContext, pool.Ping and health.Bool(nc.Healthy, ...) fit directly.
package health
