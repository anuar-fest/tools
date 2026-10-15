# Tools

A monorepo for reusable platform packages. It brings together focused tools
for application infrastructure, service integrations, and observability, such
as a resilient executor, metrics with hooks, database integrations, and
OpenTelemetry support.

## Package organization

Keep each independently useful package in its own directory under `packages/`.
Group packages by capability, and keep service-specific integrations separate
from their core abstractions. For example:

```text
packages/
  resilient-executor/
  metrics/
  integrations/
    postgres/
  opentelemetry/
```

This is an illustrative layout; add directories as packages are introduced.
Each package should document its purpose, public API, configuration, and
supported integrations, and keep its implementation and tests together.

## Package boundaries

- Keep packages focused and usable independently where practical.
- Make dependencies between packages explicit; avoid relying on repository-wide
  implementation details.
- Keep optional integrations in separate packages so users only need to adopt
  the capabilities they use.
- Define stable extension points (such as hooks) in the package that owns the
  capability, and implement service or OpenTelemetry adapters at the boundary.
- Add package-specific setup, usage, and testing instructions with each
  package.
