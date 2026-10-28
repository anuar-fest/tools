# alarms — application alarms as Prometheus alerts

## Problem
The code often knows something is wrong before any threshold does: a
breaker opened, a message was dead-lettered, a tenant went over quota.
Today that knowledge dies in a log line nobody greps. The earlier
`alarm` library counted it so a Prometheus rule could page, but every
service wrote the rule by hand, differently, or not at all, and some
services bolted on their own Slack notifier, so paging depended on the
pod being able to reach Slack.

## When to use / when not to
- Use `Raise` for an occurrence (an event): something happened once and is counted.
- Use `Set`/`Clear` for a condition (a state): it holds until the code says it stopped.
- Do not use it for rates and latencies: those are ordinary metrics with threshold rules.
- Do not notify from the process: Alertmanager routes, groups, deduplicates and silences; this package ships the rules it needs.

## API in 10 lines
```go
al, _ := alarms.New(alarms.Config{Service: "customers", Logger: logger, Definitions: []alarms.Definition{
    {Type: "billing", Name: "quota_exceeded", Severity: alarms.Critical, Summary: "a tenant went over its hard quota"},
    {Type: "cache", Name: "unreachable", Kind: alarms.State, For: 2 * time.Minute},
}})
al.Raise(ctx, alarms.Alarm{Type: "billing", Name: "quota_exceeded", Detail: "tenant over hard limit", Attrs: []slog.Attr{slog.String("tenant", id)}})
al.Set(ctx, alarms.Alarm{Type: "cache", Name: "unreachable", Detail: "redis refused"}); al.Clear(ctx, alarms.Alarm{Type: "cache", Name: "unreachable"})
resObs = al.ResilienceObserver(resObs)            // breaker opened → event; any breaker open → state
evObs  = al.EventsObserver(evObs)                 // dead letter, handler panic, undecodable → events
hc := health.New(health.Config{OnReadinessChange: al.ReadinessChanged}) // readiness lost → state
rules, _ := al.Rules(alarms.RulesConfig{Labels: map[string]string{"team": "payments"}}) // → prometheus/rules/customers.yml
```

## What one alarm produces
| Signal | Shape |
|--------|-------|
| counter | `alarms{alarm.type, alarm.name, alarm.severity}` → `alarms_total`; every Raise and every Set transition |
| gauge | `alarms.active{…}` → `alarms_active`: 1 while a state is set, 0 once cleared |
| log line | level by severity (`critical` → ERROR, `warning` → WARN, `info` → INFO), `alarm=true`, type, name, severity, service, err, your attrs; trace id through `logx` hooks; a Clear logs once at INFO |
| span event | `alarm` with type, name, severity, detail on the active span |
| alerting rule | `increase(alarms_total{alarm_type,alarm_name}[5m]) > 0` for events, `alarms_active{…} == 1` with `for:` for states; labels `severity`, `alarm_type`, `alarm_name`, `service` plus yours |

Type and Name are labels: declared constants matching
`^[a-z][a-z0-9_]{0,63}$`. An undeclared pair still counts and logs, an
invalid one becomes `_invalid`, and pairs beyond `MaxNames` become
`_other`; none of those has a rule.

## Built-in hooks
| Hook | Alarm | Kind | Severity |
|------|-------|------|----------|
| `ResilienceObserver` | `resilience/breaker_opened` | event | warning |
| `ResilienceObserver` | `resilience/breaker_open` (any breaker open) | state, `for: 2m` | warning |
| `EventsObserver` | `events/dead_letter`, `events/handler_panic` | event | critical |
| `EventsObserver` | `events/undecodable` | event | warning |
| `ReadinessChanged` | `health/readiness_lost` | state, `for: 1m` | critical |

`BuiltinDefinitions` returns them for a rules file rendered without an
`Alarms` value.

## Defaults
| Parameter | Default | Why |
|-----------|---------|-----|
| `Severity` | warning | a missing severity is not critical |
| `Kind` | event | most alarms are occurrences |
| `RulesConfig.Window` | 5 m | one scrape interval of slack on top of Alertmanager's `group_wait` |
| `Definition.For` (state) | 0 | a state the code set is already true; raise it for flapping checks |
| `MaxNames` | 200 | a bug in a name cannot explode the series |

## Shipping the rules
```go
// cmd/rules/main.go, or a -rules flag on the service binary
out, _ := alarms.Rules(alarms.RulesConfig{Service: "customers", Definitions: append(alarms.BuiltinDefinitions(), defs...)})
os.WriteFile("deploy/prometheus/customers.rules.yml", out, 0o644)
```
The file is deterministic (sorted by type and name) so it diffs cleanly in
the chart. The Prometheus exporter puts resource attributes in
`target_info`, not on `alarms_total`; when several services share one
Prometheus, pass `Selector: "job=\"customers\""` so a rule matches only its
own series. Alertmanager routes on `severity` and `service`.

## Refuse list
- No notifiers, no queue, no dedup window: Alertmanager does that better and does not depend on the pod reaching Slack.
- No dynamic values in Type or Name; put them in Detail or Attrs.
- No synchronous anything: `Raise`, `Set` and `Clear` never block and never fail.
- No YAML library: the rule file is a fixed shape written with the stdlib.

## Proof
`alarms_test.go`: counter, log line and span event for one alarm;
severity from the definition and the warning fallback; state transitions
(set once, idempotent, cleared once, `alarms_active` 1 → 0); definition
validation and the name cap; hooks for resilience (two breakers, state
cleared when the last closes), events and health produce exactly the
expected series. `rules_test.go`: event and state expressions, `for`,
labels, selector, sorted and stable output, validation table, alert
names.
