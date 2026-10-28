# logx — the service logger: slog, validated level, redaction, context hooks

## Problem
Six copies of `logger.go` each parse a level string, ignore it outside dev
(the bug that shipped debug logging to production), print config structs with
passwords in them, and have no way to put the request id or trace id on a
line without every call site adding it. Libraries that log need a logger
without inheriting the service's framework.

## When to use / when not to
- Use `New` once in `main`; pass the `*slog.Logger` to every module in this repo.
- Use `Secret` for every credential field in a config struct.
- Use `ContextHandler` hooks (`httpserver.LogAttrs`, `otelx.LogAttrs`) so request and trace ids ride along with `InfoContext`.
- Keep zerolog if the service already has dashboards on its format: `logx/zerolog` bridges slog into it.
- Do not log payloads or headers; redaction is a net under the trapeze, not the trapeze.

## API in 10 lines
```go
level, err := logx.ParseLevel(cfg.LogLevel)          // "verbose" is an error, not "info"
logger, err := logx.New(logx.Config{
    Level: level, Format: logx.FormatJSON,
    Attrs: []slog.Attr{slog.String("service", "customers"), slog.String("version", version)},
    ContextAttrs: []logx.ContextAttrs{httpserver.LogAttrs, otelx.LogAttrs},
})
logger.InfoContext(ctx, "customer created", "customer_id", id)   // + request_id, trace_id
type DB struct{ Password logx.Secret }                           // prints "[REDACTED]" everywhere
```

## Defaults
| Parameter | Default | Why |
|-----------|---------|-----|
| `Level` | Info | Debug in production doubles log volume and leaks internals |
| `Format` | JSON | collectors parse it; text is for a terminal |
| `Output` | stdout | the container's stdout is what cadvisor/Loki read |
| redaction | `DefaultRedactKeys` | password, token, authorization, api_key, cookie, dsn … at any depth |
| `AddSource` | off | file:line costs a stack walk per line |

## Composition order
`ContextHandler` wraps the encoder; `Attrs` wrap that. Groups opened later put
context attributes inside the group (documented limitation; see `ContextHandler`).

## Failure modes
| Situation | Behaviour |
|-----------|-----------|
| unknown level string | `ParseLevel` returns an error; the service refuses to start |
| unknown format | `New` returns an error |
| secret logged by value | `Secret` renders `[REDACTED]` through fmt, json, text, slog |
| secret logged under a known key as plain string | redacted by key (`Redact`) |
| secret under an unknown key as plain string | **not** redacted; use `Secret` |

## Refuse list
- No async/buffered writer with drop counters: a slow stdout is a node problem, not a library one.
- No sampling: do it in the collector, where you can change it without a deploy.
- No log-to-file rotation: the runtime owns stdout.
- No `Fatal`: return errors; `main` decides the exit code (see `lifecycle`).

## Proof
`logx_test.go`: redaction at depth and with key variants, `Secret` through
five formatting paths, context attributes present only with a context,
`LevelVar` changes at runtime, invalid level and format rejected.
