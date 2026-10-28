# Hermetic protobuf generation

This is the replacement for the `protoc`-download Makefile in
`shaffra_events`. Copy [`templates/proto/`](../../templates/proto/) into a
contracts repository; everything is pinned in files that are reviewed.

## Why the old setup drifts

| Cause | Where | Effect |
|-------|-------|--------|
| Generator version defined in two places | Makefile says `protoc-gen-go v1.36.6`, README says `v1.35.2` and suggests the system `protoc` | v1.35→v1.36 rewrites every file (`protogen:"open.v1"`, raw descriptors as strings, field order). One developer regenerates the world. |
| `protoc` downloaded with `curl` without a checksum | Makefile | A different patch build changes the header comment on every file. |
| Plugin resolved via `PATH` until recently | Makefile | `$GOPATH/bin/protoc-gen-go` won over the pinned one. |
| CI uses `setup-go: stable` | `.github/workflows/ci.yml` | A new Go release adds `vet` checks and changes `gofmt`; tests fail with no code change. |
| Generated files hand- and editor-edited | git history: import regrouping, comment edits in `.pb.go` | Format-on-save undone by regeneration → diff. |
| Outputs flattened by basename, `find` order | `go_package = "go/messages;messages"` for 135 files | Two protos with one basename overwrite each other; APFS and ext4 order differently. |
| Only changed/added files are checked | `git status --porcelain go/` | A renamed `.proto` leaves a stale `.pb.go` that passes CI. |

## What the template does instead

```
go.mod            go 1.25; tool github.com/bufbuild/buf/cmd/buf   ← pinned by `go get -tool`
buf.yaml          module config, lint rules, breaking rules
buf.gen.yaml      remote plugins pinned: buf.build/protocolbuffers/go:v1.36.6
proto/<domain>/v1/*.proto   one package per domain and major version; go_package per package
gen/go/...        committed output, linguist-generated, excluded from formatting
Makefile          generate | lint | breaking | check (= generate + git diff --exit-code)
.github/workflows/ci.yml   go-version-file, make check, buf breaking against main
.gitattributes    *.pb.go linguist-generated -diff
```

- **`go tool buf`** runs the exact buf version in `go.mod` on every machine
  and in CI. No install step.
- **Remote plugins** mean the generator runs on buf's servers at the pinned
  version; nothing local influences the output. (Air-gapped? Pin the local
  plugin with `go get -tool google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.6`
  and switch `buf.gen.yaml` to `local: go tool protoc-gen-go`.)
- **`buf generate` cleans the output directory first** (`clean: true`), so
  stale files from renamed protos disappear.
- **`buf breaking`** fails the PR on wire-incompatible changes (renumbered
  fields, changed types, removed fields without `reserved`).
- **`buf lint`** enforces package naming, `go_package`, field naming, and
  that enums have a zero value.

## Day-to-day

```sh
make generate   # go tool buf generate
make lint       # go tool buf lint
make breaking   # go tool buf breaking --against '.git#branch=main'
make check      # what CI runs: generate, then fail on any diff
```

Bumping the generator: change the version in `buf.gen.yaml`, run `make
generate`, review the diff, merge. One PR, one cause, one diff.

## Migration from `shaffra_events`

1. Move `proto/*.proto` into `proto/<domain>/v1/` by stream (`accounts`,
   `customers`, …) and set `option go_package =
   "github.com/Shaffra/shaffra_events/gen/go/<domain>/v1;<domain>v1"`.
   Keep the message names; consumers change import paths once.
2. Add `reserved` for every field ever removed; `buf breaking` enforces it
   from then on.
3. Replace the Makefile and workflow with the template's.
4. Delete `.bin/`, the protoc download, and the README's "install protoc"
   section.
5. Keep `subjects.go` out of the generated directory; it is hand-written.
