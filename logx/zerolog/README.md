# logx/zerolog — slog.Handler over a zerolog.Logger

## Problem
The services log with zerolog; every module in this repo logs through
`*slog.Logger`. Two loggers means two formats in one stream and two places
to set the level.

## When to use / when not to
- Use during the migration: keep `zerolog.Logger` as the sink, hand libraries
  `slog.New(logxzerolog.NewHandler(...))`.
- Once the service itself logs through slog, drop this module and use `logx.New`.

## API in 10 lines
```go
h := logxzerolog.NewHandler(logxzerolog.Config{
    Logger:      log.Logger,                                    // the existing zerolog logger
    ReplaceAttr: logx.Redact(logx.DefaultRedactKeys...),        // same redaction as logx
})
logger := slog.New(logx.ContextHandler(h, httpserver.LogAttrs, otelx.LogAttrs))
```

## Defaults
| Parameter | Default | Why |
|-----------|---------|-----|
| `Level` | the zerolog logger's and global level | one place decides |
| `AddSource` | off | stack walk per line |
| level mapping | trace/debug/info/warn/error; nothing above error | `Fatal` and `Panic` end the process; a library must never do that by logging |

## Failure modes
| Situation | Behaviour |
|-----------|-----------|
| zero `Logger` | lines are discarded |
| two groups with the same key | merged into one object; zerolog alone would write the key twice and JSON readers keep the last |
| `LogValuer` values (`logx.Secret`) | resolved before writing |

## Refuse list
- No zap bridge: `go.uber.org/zap/exp/zapslog` already exists upstream.
- No level translation to zerolog's `Fatal`/`Panic`.

## Proof
`handler_test.go`: kinds, nested groups as dicts, `WithAttrs`/`WithGroup`
composition with merged keys, `ReplaceAttr` at depth, level gating from
both sides, `caller` with `AddSource`.
