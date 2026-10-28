# Documentation index

| Path | What it is |
|------|------------|
| [roadmap.md](roadmap.md) | Phased roadmap for this repo: what gets built, in which order, and why. |
| [pattern-template.md](pattern-template.md) | The README format every package follows. |
| [adr/](adr/) | Architecture decision records for the repo-wide choices (0010: the otelx layout, 0011: the metrics module family, 0012: alarms as Prometheus alerts). |
| [codegen/proto.md](codegen/proto.md) | Hermetic protobuf generation (buf + `go tool`) and why the old Makefile drifts. |
| [shaffra/architecture-review.md](shaffra/architecture-review.md) | Critique of the Shaffra service repos: duplication, structure, tooling. |
| [shaffra/load-and-resiliency-findings.md](shaffra/load-and-resiliency-findings.md) | Ranked findings: what breaks under load, security holes, known problems. |
| [shaffra/roadmap.md](shaffra/roadmap.md) | How to migrate the Shaffra services onto these libraries, service by service. |
| [../examples/](../examples/) | One runnable `main` per package: the smallest correct wiring of each module, compiled in CI. |

Package-level docs live next to the code (`resilience/README.md`, `resilience/retry/README.md`, ...).
