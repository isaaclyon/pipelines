# 0007: Use a monorepo

Date: 2026-10-07

## Status

Accepted.

## Context

Ingestion, orchestration, transformations, and semantic definitions form one
analytics platform. A source or model change can require coordinated updates to
multiple components, their fixtures, and their documentation.

Separate repositories would make those contracts harder to review together and
require release coordination before the platform has established its boundaries.
A single repository must still preserve independent package dependencies.

## Decision

Keep the platform's maintained components, shared engineering standards, and
architecture records in one repository. Organize executable components as
packages with explicit ownership and public interfaces.

Review related changes to ingestion contracts, dbt models, and Cube definitions
together. Keep each package independently buildable and testable according to
[0008](0008-isolate-package-dependencies.md), and preserve the separate runtime
boundaries defined in [0003](0003-isolate-dagster-code-locations.md).

Provide repository-level container commands that coordinate package checks while
retaining the ability to run each package's checks independently. Changes to a
public interface must also exercise its affected consumers.

## Consequences

- A single change can include the code, fixtures, and documentation needed to
  update a platform contract coherently.
- Shared context and standards are discoverable alongside the implementation.
- Repository membership does not grant dependency access. Import and build checks
  must enforce package boundaries.
- Cross-package changes may require several package checks and runtime-image
  builds. The repository must make those dependencies visible.
- Splitting a component into another repository remains possible but would need
  an explicit versioning and publication contract.
