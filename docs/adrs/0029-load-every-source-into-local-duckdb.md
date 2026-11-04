# 0029: Load every source into local DuckDB

Date: 2026-11-01

## Status

Proposed on 2026-11-01 and accepted by the owner on 2026-11-02. Extends
[0028](0028-develop-locally-before-shared-snowflake-release.md), which made local
DuckDB the development target but did not authorize new source fetches.

## Context

Under [0015](0015-support-duckdb-and-snowflake.md) and
[0025](0025-use-duckdb-for-deterministic-tests.md), local runs used fixtures in
place of live sources. Sources were built to that rule:

- Shopify orders and Loop Returns can write DuckDB under the `local` target, but
  the local stack has no credential mount.
- GoodDay hard-codes a Snowflake destination and rejects `local` and `ci`.
- Item Master has offline capture code and no live entry point in any target.
- Each loader writes its own raw DuckDB file in the ingestion-state volume. The
  local dbt profile reads only `warehouse.duckdb` in the working volume, and DuckDB
  does not resolve a bare raw schema name in an attached file, so local dbt cannot
  read live raw data.

The local stack therefore cannot show real data or check that a source still
works without a shared Snowflake run. That defeats the local iteration 0028 chose.

## Decision

Every maintained source can load live into local DuckDB, and the local dbt build
reads those loads.

- **One loader, two destinations.** Each source selects DuckDB under `local` and
  Snowflake under `development` and `production`, using the same extraction,
  parsing and raw contract. No local-only extraction code.
- **Separate raw files.** Each source keeps its own raw DuckDB file and load state
  under the worktree's ingestion state. The local dbt profile attaches existing raw
  files read-only; each dbt source names its attached file only on the local target.
  A missing raw file fails the dependent build clearly. It is never replaced with an
  empty file.
- **Bounded by default.** Time-windowed sources read a recent window of a few
  days, set per run. Snapshot sources (GoodDay inventory, Item Master) capture
  current state once per run under their existing size, byte and time limits.
- **Run through local Dagster.** Live local loads are the same Dagster jobs, launched
  manually on the local stack. Nothing schedules them by default.
- **Developer credentials.** The developer renders local credentials from 1Password
  with their own identity under [0018](0018-manage-environment-secrets-with-1password.md).
  Use read-only source scopes. Credentials are mounted read-only and never enter
  images, Git or logs.
- **Checks stay offline.** CI and package checks keep deterministic fixtures and
  never reach a live source. The `ci` target does not accept live credentials.
- **Private data stays private.** Raw files may hold restricted fields. They stay
  owner-only, ignored and per worktree. Cube still receives only the analytical
  export defined by 0028.

The first pass covers Shopify orders, GoodDay inventory and Item Master. Loop
Returns and Shopify customers follow the same rules when selected.

Accepting this record authorizes the owner's own bounded local live reads of those
sources. It does not authorize Snowflake loads, schedules, publication, production
writes or other users' access.

## Consequences

A developer can load real recent data, build the marts and browse or serve them on
one machine, and see quickly when a source or contract breaks. Local success is not
Snowflake evidence; 0028's shared-release checks still apply.

Each source gains a DuckDB destination test and a local credential mount. The dbt
sources gain a local-only file setting. Live local runs cost source API quota and,
for GoodDay, billed BigQuery bytes. The developer's laptop holds private customer
data and must be treated accordingly.
