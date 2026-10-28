# 09 — Testing: fakes and integration

## Intent

Prefer **real dependencies** for infrastructure you own (Postgres, Redis, brokers) and **hand-written fakes** for ports. Reserve mocks for true third-party edges.

## Author mode

- Unit-test domain and use cases with fake ports (in-memory maps, scripted stubs).
- Integration-test adapters and SQL against disposable Docker containers (e.g. testcontainers-go); migrate schema in test setup; destroy after.
- Mock only external SaaS, payment providers, email gateways, and similar.
- Never mock PostgreSQL, Redis, Kafka, RabbitMQ, or NATS when the code under test is persistence/messaging itself.
- Name tests after behavior; use table-driven cases; run race detector on concurrent code.
- Keep tests self-contained — no manually started shared databases required.

## Audit mode

Checklist:

- [ ] Do persistence tests hit a real engine, or a mock that reinvents SQL?
- [ ] Are fakes trivial and local, or a second framework?
- [ ] Do tests share mutable global DB state across packages without isolation?
- [ ] Is coverage concentrated on getters while adapters are untested?

Red flags:

- Mockery of `QueryRow` with brittle call sequences.
- Tests that require a developer’s local Postgres with hand-loaded fixtures.
- “Unit tests” that only assert mock call counts and never outcomes.

## Go adaptations

```go
// Good: fake port
type memOrders struct{ byID map[string]Order }

// Good: integration
// testcontainers Postgres + migrate + repo test

// Bad: mock *sql.DB expectations as the only store test
```

Prefer **fakes over mocks**: implement the consumer-owned port in the test package.

## Non-goals

- Not a ban on `net/http/httptest` or light stubs.
- Not a requirement for 100% coverage metrics over meaningful scenarios.
