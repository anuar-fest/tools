# Proto contracts repository template

Hermetic protobuf → Go generation. See `docs/codegen/proto.md` in the tools
repo for the reasoning. Copy this directory to the root of a contracts repo,
set the module path in `go.mod` and `go_package` options, then:

```sh
go get -tool github.com/bufbuild/buf/cmd/buf@v1.54.0   # pins buf in go.mod
make check
```
