# 0023: Maintain Dagster storage in an operator window

Date: 2026-10-14

## Status

Deprecated on 2026-11-03. This repository now ships only the local and CI
targets, and its remote release workflow was removed. The record stays as history.
Previously accepted by the platform owner.
Partially supersedes [ADR 0020](0020-keep-consecutive-releases-compatible.md)
for incompatible Dagster storage upgrades and
[ADR 0021](0021-run-data-changes-as-orchestrated-pipeline-work.md)
for maintenance of the control plane's own storage. Their ordinary release and
warehouse-change requirements remain in force.

## Context

Dagster sometimes changes its PostgreSQL schema. Old images may not read the
migrated schema, so an image rollback cannot recover the instance. A migration
run orchestrated by that same instance would depend on the storage it is changing.
Integrating database restoration into every deployment would expand the uniform
code-only workflow in [ADR 0019](0019-deploy-releases-through-one-source-agnostic-workflow.md).

The owner accepts maintenance downtime. Releases can be skipped and remote
environments can lag, so a warning on the migration commit alone is insufficient.
Recovery must preserve effective configuration and mounted files as well as the
database and matching images.

## Decision

Perform incompatible Dagster storage upgrades as a separate operator procedure
under the existing environment deployment lock. Deploy enforcement before the
upgrade. Commit a version assessment and inherited deployment hold; clear the
hold by review only after every existing remote environment is verified. A
future environment starts at a verified storage baseline.

Close submissions and automation, drain queued and active work within a declared
deadline, and abort without cancelling runs if draining fails. Stop remaining
writers before capturing a secured, coherent database/release/image/configuration
recovery set. Migrate with the candidate runtime and verify while dispatch stays
disabled. A committed flag supplements operator exclusion of old scripts, raw
Docker access, external submitters, and concurrent deployments.

Before reopening work, record the migrated release as current and retire the
incompatible predecessor from automatic rollback. Preserve its recovery set
separately. Until another compatible release succeeds, a failed redeploy requires
operator recovery or a forward fix and must preserve the stack rather than
invoke first-deploy teardown.

Database restoration with matching images and configuration is permitted within
the closed window. Starting dispatch or reopening submissions ends that routine
restore option. Thereafter use a forward fix or explicitly planned incident
recovery that accounts for warehouse effects and lost orchestration history.

## Consequences

- Ordinary deployment remains source-agnostic, changes code only, and performs
  no storage migration, drain, cancellation, or database restoration.
- A maintenance release can temporarily lack a compatible rollback predecessor;
  the PR and operator record make that exception explicit.
- Maintenance requires downtime and operator control of every writer and deploy
  path. Offline checks validate release declarations, not live environment claims.
- The operator procedure and isolated real-schema rehearsal provide recovery evidence. They do not constitute ongoing
  backup infrastructure or authorize remote upgrades.
