# 08 — Files and idempotent I/O

## Intent

File side effects (settlement, exports, uploads) must be **path-safe**, **atomic where replace matters**, and clean on failure. Partial files must not become “success.”

## Author mode

- Resolve and constrain paths (no `../` escape from export root).
- Write to temp in the same directory → `Sync` if durability required → `Rename` for atomic replace.
- Clean temp on error; do not leave `.tmp` as the published artifact.
- Idempotent exports: stable names/keys or explicit versioning so retries do not corrupt.
- Close files on all paths; check close errors when durability matters.

## Audit mode

Checklist:

- [ ] User-controlled path segments sanitized?
- [ ] Direct write to final path under crash risk?
- [ ] Retry creates duplicates or truncates good files?
- [ ] Permissions/umask intentional?

Red flags: `os.WriteFile(userPath)`; rename across filesystems assumed atomic; ignoring close errors after money export.

## Go specifics / anti-patterns

```go
// Good pattern
f, err := os.CreateTemp(dir, "export-*.tmp")
// write, f.Sync(), f.Close(), os.Rename(tmp, final)
```

## Non-goals

- Not a full object-storage guide — S3 adapters still need idempotent keys and checksum policy.
- Not mandating fsync on every log line — declare durability class per artifact.
