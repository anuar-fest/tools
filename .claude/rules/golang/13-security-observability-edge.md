# 13 — Security and observability at the edge

## Intent

Edges must not leak **secrets/PII**, must **audit dangerous mutations**, and must propagate **trace/correlation** across goroutines. Admin and webhook surfaces are hostile.

## Author mode

- Never log raw Authorization, API keys, PANs, or full PII bodies — redact or hash.
- Audit **before** or transactionally with sensitive admin actions; do not audit-only-after success if failure modes matter for forensics (document ordering).
- Webhooks: verify signatures; bound body size; drain bodies; SSRF-safe egress (block link-local/metadata IPs).
- Open redirects and host header trust: allowlist.
- Errors to clients: stable codes; no stack traces or internal hostnames.
- Put trace/request IDs in context and logs; preserve across worker handoff.

## Audit mode

Checklist:

- [ ] Secrets in structured logs?
- [ ] Admin mutate without authz + audit?
- [ ] Webhook handler SSRF or replay gaps?
- [ ] Recover/logging middleware swallows audit?

Red flags: `log.Info("admin", "authorization", hdr)`; `http.Get(userURL)`; redirect to `url` query unchecked.

## Go specifics / anti-patterns

```go
// Bad — anti-pattern
log.Info("admin request", "authorization", r.Header.Get("Authorization"), "body", string(body))
```

## Non-goals

- Not a full threat model — focus on common Go service footguns at the edge.
- Cross-link architecture side-effect isolation for where logging ports live.
