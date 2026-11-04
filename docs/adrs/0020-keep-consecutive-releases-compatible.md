# 0020: Keep consecutive releases compatible

Date: 2026-10-09

## Status

Deprecated on 2026-11-03. This repository now ships only the local and CI
targets, and its remote release workflow was removed. The record stays as history.
Previously accepted.
Partially superseded 2026-10-14 by [ADR 0023](0023-maintain-dagster-storage-in-an-operator-window.md)
for incompatible Dagster storage maintenance and its temporary lack of a
compatible rollback predecessor. Ordinary releases retain this contract.
Amended 2026-10-09 after review of the deploy workflow spec: an environment
may skip superseded releases, so each step merges only after the previous step
is deployed everywhere.

## Context

[ADR 0005](0005-promote-immutable-release-images.md) keeps the previous release
manifest for rollback, and [ADR 0019](0019-deploy-releases-through-one-source-agnostic-workflow.md)
rolls back by redeploying it. Redeploying restores code and dependencies only.
Warehouse tables, ingestion checkpoints, batch records, Dagster run history, and
published marts keep whatever the newer release wrote.

A previous platform's September 2026 deployment retrospective
records the failure this causes. A release published provenance that the earlier
release rejected, so rolling back the Agent failed and the service was down for
37 minutes.

The same mismatch occurs without a rollback. Ingestion runs continue during a
deployment (ADR 0019), so for a short period the old and new releases both read
and write the same datasets and state. Data changes such as backfills run after
the release that needs them
([ADR 0021](0021-run-data-changes-as-orchestrated-pipeline-work.md)), so new
code also runs against old-shaped data until they finish.

## Decision

Each release must work with everything its predecessor writes, and its
predecessor must keep working with everything the new release writes. Rolling
back one release is therefore always safe. Rolling back further is unsupported;
fix forward instead.

Make incompatible changes in three steps, each in its own release:

1. **Expand.** Add the new column, table, field, model version, or accepted value
   next to the old one. Writers may write both.
2. **Migrate.** Move readers to the new shape and run any backfill.
3. **Contract.** Remove the old shape once nothing reads it.

An environment may skip a release that a newer one superseded before its
deployment started, so the release running before a deployment is not always the
one merged just before it. Merge each step only after the previous step's release
is running in every remote environment and its data change
([ADR 0021](0021-run-data-changes-as-orchestrated-pipeline-work.md)) has finished
there. Every deployed release then has a compatible predecessor, whichever
releases were skipped.

This applies to every artifact that outlives a container:

- **Raw tables.** Add columns as nullable. Keep existing columns' names, types,
  and nullability; a column may only become nullable. A rename, type change, or
  key change is a new column or a new dataset version.
- **Ingestion state.** Checkpoints, progress files, and batch records tolerate
  missing optional fields and ignore unknown ones. A release that adds a value to
  a persisted enumeration ships the reader first and starts writing the value in
  a later release. A field that the previous release drops when it rewrites the
  file must be safe to lose.
- **Orchestration identities.** Keep asset keys, job names, and schedule names
  stable, because Dagster stores run history and schedule state against them.
  Rename by adding the new definition and retiring the old one later.
- **Published marts and metrics.** Breaking changes to a published dbt model use
  a new model version with an enforced contract, extending the published-relation
  contracts in [ADR 0013](0013-transform-data-with-dbt-core.md). The old version
  stays until its readers move. Cube measures and dimensions
  ([ADR 0014](0014-govern-metrics-with-cube-core.md)) are marked deprecated in one
  release and removed in a later one.

When a change cannot be made compatible, its pull request says so and states the
forward fix. Production promotion of that release needs explicit acknowledgement
that rollback is unavailable.

## Consequences

- Rollback is a routine deployment, and the release before the current one is
  always a known-good target.
- Removing anything takes at least two releases, so unused columns, fields, and
  model versions linger until the contract step.
- Readers must handle nulls in newly added columns until backfills finish.
- Persisted formats need compatibility tests against data written by other
  releases, which raises the cost of changing them.
- A single rollback step means a bad release found two releases later is fixed
  forward.

### Fitness checks

- `packages/ingestion/tests/test_shopify_schema.py` compares the declared
  raw Shopify schema with a committed snapshot. It fails on a removed column, a
  changed type, a column that became required, or a new required column, and it
  requires deliberate snapshot updates for every schema change.
- Published marts gain dbt contract checks when the first published model lands.
