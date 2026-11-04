# 0005: Promote immutable release images

Date: 2026-10-07

## Status

Accepted. Extended by [0036](0036-upgrade-clients-one-at-a-time-on-pinned-toolkit-versions.md),
which pins one toolkit version per client.

## Context

Containers only improve reproducibility if deployment uses the artifacts that
passed validation. Rebuilding on each host, using mutable image tags, or mounting
production source code can change an environment independently of its tested
release.

Development machines and the Hetzner VM may also use different CPU architectures.
Local success must not stand in for testing the production image variant.

## Decision

Build release images in CI from a recorded source revision, pinned base images,
and locked dependencies. Publish them to a container registry and identify
deployments by immutable digest. Record the component digests as one release
manifest, including compatible Dagster control-plane and code-location versions.

Test release images for the VM's target CPU architecture before promotion. Local
development may use native-architecture images built from the same definitions.
When publishing multiple architecture variants, test each supported variant.

Promote the same release images through remote development and production.
Supply environment-specific credentials and configuration at runtime. Keep
separate build stages for developer tooling and tests where useful, and exercise
the final release artifacts in integration checks.

Include code and declared runtime artifacts in release images. Generate
target-specific dbt artifacts in the appropriate isolated target rather than
reusing DuckDB-compiled SQL in Snowflake. Production must not install dependencies
at startup or mount mutable application source.

Retain the previous release manifest for rollback. Deploy code-location servers
and their run-image configuration together so newly launched runs use the
intended release. Account for active runs before retiring old images.

## Consequences

- Validation and deployment refer to an identifiable set of artifacts.
- A registry and release manifest become part of the deployment workflow.
- Dependency changes require a new build. Local source-mounted development
  requires a release-image check before promotion.
- Native libraries and database drivers need architecture-specific validation.
- Rolling back images restores code and dependencies. Warehouse migrations,
  schema changes, and published data require their own recovery procedure.
