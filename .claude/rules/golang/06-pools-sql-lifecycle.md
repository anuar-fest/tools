# 06 — Pools and SQL lifecycle

## Intent

DB/HTTP connection pools are **blast-radius devices**. Rows, statements, and transactions must be closed/released; pools sized and isolated so one dependency cannot starve the process.

## Author mode

- Always `defer rows.Close()` (and check `rows.Err()`); close rows before next query on same conn/tx when required.
- Transactions: short critical section; no HTTP calls inside tx; pass ctx; rollback on error paths.
- Size `MaxOpen` / `MaxIdle` / conn lifetime from real concurrency — not cargo-cult defaults.
- Prefer **partitioned pools/clients** per dependency or priority class over one giant shared pool when failure isolation matters.
- Under cancel: do not leave acquires hanging without ctx; document what happens mid-query on cancel.
- Never hold pool connections while waiting on another slow dependency.

## Audit mode

Checklist:

- [ ] Rows/Stmt/Tx closed on all paths?
- [ ] Shared pool across unrelated egress?
- [ ] Tx duration / lock time observable?
- [ ] Pool metrics (wait, in-use) watched under load?

Red flags: `Query` without Close; long tx spanning external HTTP; single pool for payments + analytics + admin.

## Go specifics / anti-patterns

```go
rows, err := db.QueryContext(ctx, q, args...)
if err != nil { return err }
defer rows.Close()
for rows.Next() { ... }
return rows.Err()
```

## Non-goals

- Not ORM advocacy — adapters may use pgx/database/sql; rules are lifecycle and isolation.
- Cross-link architecture: domain must not import driver types.
