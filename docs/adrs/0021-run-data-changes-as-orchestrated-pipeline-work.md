# 0021: Run data changes as orchestrated pipeline work

Date: 2026-10-09

## Status

Accepted.
Partially superseded 2026-10-14 by [ADR 0023](0023-maintain-dagster-storage-in-an-operator-window.md)
for maintenance of Dagster's own storage, which runs in a separate operator
window. [ADR 0028](0028-develop-locally-before-shared-snowflake-release.md)
also provides a bounded exception for single-user local experiments. Shared
warehouse data changes retain this contract.

## Context

Code changes often need data changes. A new raw column is empty for history until
it is backfilled. A changed filter or parsing rule leaves earlier rows in the old
form. A changed incremental dbt model only applies its new logic to new rows. A
breaking change to a published mart forces every reader to change.

[ADR 0005](0005-promote-immutable-release-images.md) and the engineering
standards say data migrations are recorded separately from image deployment,
because an image rollback does not reverse warehouse changes. Neither says where
data changes run.

A previous platform ran warehouse migrations inside its release path. Its
September 2026 deployment retrospective records 17 migrations in 9
days with 8 failed attempts, migrations blocked by per-source route guards, and a
separate migration exception written for three sources. A failed migration
stopped the release for everyone.

This platform already has the tools that apply data changes during normal work.
dlt evolves raw schemas under each resource's schema contract
([ADR 0017](0017-ingest-data-with-dlt.md)). dbt rebuilds models
([ADR 0013](0013-transform-data-with-dbt-core.md)). Dagster runs, retries,
records, and limits the concurrency of both
([ADR 0012](0012-orchestrate-assets-with-dagster.md)).

## Decision

A deployment changes code only
([ADR 0019](0019-deploy-releases-through-one-source-agnostic-workflow.md)). Every
data change runs afterward as an ordinary Dagster run in the target environment,
with the release image, credentials, logs, retries, and concurrency limits that
scheduled work uses. Dagster run history is the record of what ran; there is no
separate migration journal.

### How each kind of change is applied

| Change | Applied by |
| --- | --- |
| New raw column or table | dlt on the next load, as a nullable column. History stays null until a re-extraction fills it, if needed |
| Raw filter, query, or parsing change that affects existing rows | The source's re-extraction operation over the affected windows, merging by key |
| Raw rename, type change, or key change | A new dataset or table version loaded alongside the old one, backfilled, then adopted by readers ([ADR 0020](0020-keep-consecutive-releases-compatible.md)) |
| dbt view or table logic | The next normal build. dbt on Snowflake replaces tables with `create or replace`, so readers keep the old table until the new one exists |
| dbt incremental logic that affects history | An explicit full-refresh or windowed rebuild run |
| Breaking change to a published mart | A new dbt model version alongside the old one; readers move; the old version is removed later |
| Warehouse objects outside pipelines: databases, roles, grants, warehouses | A separate reviewed infrastructure change, applied before the release that needs it and additive first |

### Rules

- The pull request that needs a data change states it: the operation, the target
  windows or models, and the expected run time and warehouse cost. After the
  release reaches an environment, a person or automation launches the operation
  there, first in development and then in production.
- Each release works before and after its data change finishes. For example, a
  model reading a new raw column handles nulls until the backfill completes.
- Data changes are safe to rerun and resume. They write by key or replace whole
  relations; they never truncate a published table and reload it in place.
- Each source provides a re-extraction operation that can rewrite rows whose
  source version is unchanged when the extraction logic has changed. Without it,
  a parsing change can only reach rows the source updates later.
- Data change runs share the concurrency limits of scheduled runs and serialize
  with runs that write the same dataset or checkpoint.
- Hand-run SQL against environment data is for incident response only and is
  recorded on the incident's issue.

## Consequences

- Deployments stay fast and identical whether or not a release needs a data
  change, and a failed data change does not block other releases.
- Data changes get the same retries, logs, and history as scheduled runs, and
  development rehearses them before production.
- Someone must launch each declared data change. A missed backfill leaves history
  incomplete without failing the deployment. Automating this from Dagster code
  versions is a possible follow-up.
- Raw breaking changes need a parallel dataset for a while, which costs storage
  and load time.
- Full refreshes and re-extractions appear as separate warehouse and source API
  costs, so they must be planned against freshness capacity.
- The raw Shopify loader preserves current rows when a backfill observes the same
  `updatedAt` with different content, which protects against stale replays. It
  therefore needs a re-extraction mode, keyed to a newer query version, before
  a parsing change can rewrite history.
