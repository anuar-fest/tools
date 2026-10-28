# ADR 0012 — alarms are Prometheus alerts: a metric plus a generated rule, no notifier in the library

**Status:** accepted · **Date:** 2026-10-28 · supersedes the alarms part of [0010](0010-otelx-layout-server-client-metrics-alarms.md)

## Context
`otelx/alarms` counted every raised condition, logged it, put an event on
the span, and also notified Slack, Telegram, a webhook or Alertmanager
from a bounded in-process queue with its own dedup window. That put a
second alerting pipeline inside every pod: routing, grouping and
silencing were re-implemented badly, a page depended on the pod reaching
Slack, and the Prometheus rules that would have made the counter useful
were still written by hand per service, or not at all. The package also
lived under `otelx`, which after ADR 0011 is the tracing SDK; alarms are
not a tracing concern.

## Decision
`alarms/` is a top-level module and alarms are Prometheus alerts:

- A condition is declared once as a `Definition` (type, name, severity,
  kind, `for`/window, summary). `New` validates the set at boot; the
  hooks' own definitions are built in.
- Two kinds. An **event** is counted: `Raise` adds to `alarms_total`. A
  **state** holds until cleared: `Set` records `alarms_active = 1` on the
  inactive → active transition (and counts it), `Clear` records 0. Both
  log once per transition and add an event to the active span.
- `Rules` renders the Prometheus alerting rule file for the definitions:
  `increase(alarms_total{alarm_type,alarm_name}[window]) > 0` for events,
  `alarms_active{…} == 1` with `for:` for states, labels `severity`,
  `alarm_type`, `alarm_name`, `service` plus the service's own. The file
  is deterministic and ships with the chart.
- The library notifies nobody. Alertmanager routes on `severity` and
  `service`, groups, deduplicates and silences.
- The hooks stay: breaker open (event per opening, state while any is
  open), dead letter, handler panic, undecodable, readiness lost (state).

## Consequences
- `Config.Notifier`, `Dedup`, `QueueSize`, `NotifyTimeout`, the notifier
  constructors and `Close` are gone; `Alarms` owns no goroutine.
- `alarms_total` and its labels are unchanged; `alarms_active` is new;
  `alarms.suppressed`, `alarms.dropped` and `alarms.notify_failures` are
  gone with the queue.
- A service that still wants chat notifications configures an
  Alertmanager receiver; nothing in the pod changes.
- Paging now depends on the scrape path (Prometheus up, the pod scraped),
  which is the same dependency every other alert already has.

## Alternatives rejected
- **Keep the in-process notifiers next to the rules.** Two pipelines with
  different dedup semantics for one alarm; the rule file is the one that
  gets reviewed. Rejected.
- **Only a counter, rules by hand (the earlier library).** Every service
  wrote them differently or forgot; generation from the declared
  definitions is the whole point. Rejected.
- **A YAML dependency for the rule file.** The shape is fixed and small;
  `strconv.Quote` escapes are a subset of YAML's. Rejected.
