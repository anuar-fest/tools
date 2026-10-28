# 08 — Side-effects isolation

## Intent

Push I/O, time, randomness, and process environment to the **edges**. Core logic receives values or narrow ports so behavior stays testable and isolatable.

## Author mode

- Perform disk, network, and DB work only in adapters or explicit infrastructure services.
- Pass `context.Context` for cancellation through effectful calls.
- Inject `Clock`, `IDGen`, or pass `now time.Time` into pure rules that depend on time.
- Read env/config at startup; pass typed config inward — avoid `os.Getenv` mid-request in domain.
- Keep logging/metrics at edges or via narrow observer ports; do not make business rules depend on exporter setup.

## Audit mode

Checklist:

- [ ] Can the decision path be tested with fixed `now` and in-memory fakes?
- [ ] Are there hidden `time.Now()`, `rand`, or `os.Getenv` calls inside core policy?
- [ ] Do tests flake because of real clocks or sleep?
- [ ] Is filesystem access wrapped behind a port when the domain only needs “document content”?

Red flags:

- Domain package calling HTTP clients directly.
- `sleep` inside business logic for “retry” instead of an explicit policy + adapter.
- Global client singletons initialized in `init()` and used everywhere.

## Go adaptations

```go
// Good
func (s Service) Place(ctx context.Context, now time.Time, cmd PlaceCmd) error

// Or
type Clock interface{ Now() time.Time }

// Bad
func Place() { if time.Now().Weekday() == time.Sunday { ... }; http.Get(...) }
```

Isolation means **control of effects**, not necessarily separate processes.

## Non-goals

- Not every `fmt` or `strings` call needs a port.
- Stdlib `time.Time` values in domain are encouraged; live clock access is what to isolate.
