# lifecycle — ordered start, ordered stop, one budget, honest exit code

## Problem
Each service has its own `signal.Notify` that shuts the HTTP server down and
exits 0 while consumers are still mid-message, the outbox relay has a claimed
row, and the last spans never leave the batch processor. A failed startup
logs and lingers unreachable instead of exiting.

## When to use / when not to
- Every service `main`: add components in dependency order, call `Run`, exit with `ExitCode`.
- Not for request-scoped goroutines (use `workpool`/`errgroup`) or for Fx users (`fxmod`, later).

## API in 10 lines
```go
app := lifecycle.New(lifecycle.Config{Logger: logger, ShutdownTimeout: 25 * time.Second,
    PreStopDelay: 2 * time.Second, OnShutdown: func(string) { hc.SetDraining(true) }})
app.Add(lifecycle.Component{Name: "telemetry", Stop: tel.Shutdown})     // added first: stops last
app.Add(lifecycle.Component{Name: "postgres", Stop: func(context.Context) error { return db.Close() }})
app.Add(lifecycle.Component{Name: "relay", Run: relay.Run})             // canceled, then joined
app.Add(lifecycle.Component{Name: "consumer", Stop: consumer.Stop})
app.Add(lifecycle.Component{Name: "http", Run: srv.Run})                // added last: stops first
os.Exit(lifecycle.ExitCode(app.Run(ctx)))
```

## Defaults
| Parameter | Default | Why |
|-----------|---------|-----|
| `ShutdownTimeout` | 25 s | under a 30 s `terminationGracePeriodSeconds` with a `preStop` sleep |
| `PreStopDelay` | 0 | set 3–5 s when there is no `preStop` hook, so endpoints update before the listener closes |
| `Signals` | SIGINT, SIGTERM | what Kubernetes and a terminal send |
| second signal | abandon the budget | an operator pressing Ctrl-C twice means now |

## Composition order
```
signal / ctx / Shutdown() / failed Run
  → OnShutdown (mark unready) → PreStopDelay
  → for each component, last added first: Stop(ctx) → cancel Run ctx → wait for Run
  → return errors.Join(cause, stop errors)
```
A `Run` that returns nil on its own is a one-shot (migration, warm-up) and does
not end the app; a `Run` that returns an error ends everything.

## Failure modes
| Situation | Behaviour | Result |
|-----------|-----------|--------|
| component `Run` fails | shutdown starts; cause is first in the error | exit 1 |
| component panics (Run or Stop) | recovered, reported, others still stop | exit 1 |
| `Stop` or `Run` exceeds its budget | `ErrStopTimeout` for that component, next one proceeds | exit 1 |
| clean signal shutdown | all stopped within budget | exit 0 |

## Observability
Info per component start/stop with duration, Warn on a second signal, Error
per failed stop. Nothing else: the components own their metrics.

## Refuse list
- No `os.Exit` inside the library; `main` owns the exit.
- No restart/supervision loops: that is the orchestrator's job.
- No dependency graph beyond add order: explicit order is readable and testable.

## Proof
`lifecycle_test.go` under `-race` and `goleak`: reverse order with joined
runs, failure cascade, budget overrun reported while the rest still stops,
parent context cancel, panic containment, one-shot components, a real
SIGUSR1, late `Add` and double `Run` refused.
