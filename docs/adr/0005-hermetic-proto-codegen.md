# ADR 0005 — Hermetic protobuf generation with buf and `go tool`

**Status:** accepted · **Date:** 2026-10-16

## Context
The events repo generates Go from `.proto` with a Makefile that downloads a
`protoc` zip without a checksum, `go install`s `protoc-gen-go` at a version
that differs from the README, and relies on `find` ordering and the local
toolchain. CI regenerates and fails on any diff. The diff appears whenever a
developer's tool version, Go version (`setup-go: stable`), editor
format-on-save or `find` order differs from CI. The failure is on the
developer, who has no way to reproduce CI's environment.

## Decision
Generation is **hermetic and declared in the repository**:
- `buf` is the only generator entry point, pinned with `go get -tool
  github.com/bufbuild/buf/cmd/buf@vX` so `go tool buf` uses the version in
  `go.mod` everywhere (Go 1.24+).
- Plugins are **remote and pinned** in `buf.gen.yaml`
  (`buf.build/protocolbuffers/go:v1.36.6`). Output depends only on the
  plugin version and the `.proto` files, never on anything installed locally.
- The Go toolchain is pinned by `go.mod` and CI uses `go-version-file`,
  never `stable`.
- Generated files are marked `linguist-generated` and `-diff` in
  `.gitattributes` and excluded from editor formatting.
- `buf lint` and `buf breaking --against '.git#branch=main'` run in CI, so
  a field renumbering fails the PR, not production.
- The CI drift check stays, now meaningful: `go tool buf generate && git
  diff --exit-code` fails only when someone forgot to regenerate.

## Consequences
- Local and CI output are byte-identical by construction.
- `protoc` is no longer installed anywhere.
- Moving the plugin version is a one-line change reviewed with its diff.

## Alternatives rejected
- **Not committing generated code** (generate in consumers). Each consumer
  needs buf and the protos; Go module resolution needs committed code
  anyway. Rejected.
- **Buf Schema Registry generated SDKs.** Cleanest, but needs a paid
  private registry. Revisit when the budget exists.
