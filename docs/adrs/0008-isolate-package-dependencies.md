# 0008: Isolate package dependencies

Date: 2026-10-07

## Status

Accepted.

## Context

The [monorepo](0007-use-a-monorepo.md) contains components with different
dependency requirements. A shared environment can let a package import a library
it never declared or bind independent upgrades to a single dependency resolution.

[0003](0003-isolate-dagster-code-locations.md) establishes separate Dagster runtime
environments and introduces package isolation. This record gives package-level
dependency ownership its own decision, applying beyond Dagster code locations.

## Decision

Manage packages as independent projects with explicit dependencies, public APIs,
and clean build and test environments. For Python, use independent uv projects,
each with its own dependency declarations and lockfile. Use a `src/` layout and
test installed distributions as well as development code.

A package may depend on another repository package through a declared dependency
and its public API. Do not expose sibling source trees through repository-wide
import paths or import another package's internals. Check permitted import
relationships and detect dependencies missing from declarations.

Build and test each package using only its declared runtime and development
dependencies. A deployable image installs the explicit dependency closure it
needs. Share contracts through small packages when actual reuse warrants them.

Apply the same independently installable and testable boundary to TypeScript
packages when introduced. Shared tooling configuration must not supply implicit
runtime dependencies.

## Consequences

- Packages can be validated and upgraded independently, and dependency mistakes
  become visible before deployment.
- A shared repository environment cannot act as a convenient fallback for
  missing dependencies.
- Multiple lockfiles and clean environments add maintenance and build work.
- A runtime that composes declared packages must still resolve their combined
  dependency closure. Separate runtime boundaries handle incompatible closures.
- Python cannot enforce these boundaries by directory layout alone; installed
  package tests and import checks are required.
