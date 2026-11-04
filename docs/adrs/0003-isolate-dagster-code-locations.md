# 0003: Isolate Dagster code locations

Date: 2026-10-07

## Status

Accepted.

## Context

The monorepo needs strict package isolation. Ingestion connectors and dbt
adapters have different dependency sets and upgrade cycles. Installing every
package into one environment can hide undeclared imports and force unrelated
components to accept the same dependency versions.

Dagster supports separate code locations: services that load asset definitions
in their own Python environments. This gives the platform an execution boundary
that matches its package-isolation requirement.

## Decision

Use separate Dagster code locations for ingestion and transformations, with an
independent dependency environment and image for each. Keep the control-plane
image limited to Dagster services and their configuration.

Manage Python packages as independent uv projects with their own lockfiles and
test environments. A deployable location may install explicitly declared
packages and their dependency closure. Access another package through its
declared public API; filesystem paths and repository-wide import paths must not
make undeclared dependencies available.

Build and test packages independently before composing a runtime image. Share
small packages only when there is an actual shared contract. Split additional
code locations when a source needs a separate dependency or deployment boundary.

Declare cross-location asset dependencies using stable asset keys. Exchange data
through warehouse relations or explicit files, and coordinate downstream runs
with Dagster asset automation or sensors.

## Consequences

- Connector and adapter upgrades can be tested and deployed independently.
- Separate environments expose dependency mistakes earlier and limit the
  dependency conflicts a runtime must resolve.
- Asset definitions across code locations cannot materialize in a single
  Dagster run. An ingestion-to-mart flow spans coordinated runs.
- Cross-location dependencies describe lineage; they do not pass Python objects
  between processes. Data contracts and publication boundaries must be explicit.
- Multiple environments, locks, and images create maintenance work. Package and
  code-location boundaries should follow concrete ownership or dependency needs.

## References

- [Dagster cross-location asset dependencies](https://docs.dagster.io/guides/build/assets/defining-assets-with-asset-dependencies)
- [uv workspace isolation limitations](https://docs.astral.sh/uv/concepts/workspaces/)

## Addendum: 2026-10-07

[0008](0008-isolate-package-dependencies.md) records package dependency ownership
as a separate decision applicable to every maintained package. This record
continues to define the Dagster code-location and runtime boundaries. The
package-isolation requirements introduced here are unchanged.
