# 0031: Serve each client from MotherDuck or Snowflake

Date: 2026-11-04

## Status

Accepted by the owner on 2026-11-04.
Supersedes [0010](0010-use-snowflake-as-the-warehouse.md) and the target table in
[0006](0006-isolate-development-and-production-environments.md). Replaces the
Snowflake serving target in [0028](0028-develop-locally-before-shared-snowflake-release.md);
0028's local DuckDB development rules remain.

## Context

Small D2C brands differ in budget and existing tools. Some already pay for
Snowflake. Most do not need it, and its minimum costs are high for their data
volume. MotherDuck is a hosted DuckDB service with usage-based pricing and the
same SQL dialect as local DuckDB.

A DuckDB file on the client VM would be free, but DuckDB allows one writer at a
time. Under 0028 Cube must stop during each refresh, so dashboards would go
dark on every scheduled run. That is acceptable for one developer, not for a
brand's team.

dlt, dbt (through `dbt-duckdb`) and Cube's DuckDB driver all connect to
MotherDuck with a token.

## Decision

Each client serves from exactly one warehouse, chosen in its client config:

| Warehouse | When to use it |
| --- | --- |
| MotherDuck | Default for new clients |
| Snowflake | The client already runs Snowflake or needs its features |

Use local DuckDB for development in both cases, under 0028 and
[0029](0029-load-every-source-into-local-duckdb.md). A DuckDB file is not a
supported serving warehouse on a client VM.

Targets per client:

| Target | Backend | Purpose |
| --- | --- | --- |
| Local | DuckDB files and fixtures | Development and offline checks |
| Development | A separate database in the client's warehouse | Checks against the real warehouse before upgrades |
| Production | The client's production database | Scheduled loads, models, Cube and Metabase |

Keep 0006's isolation rules: separate credentials, writable targets, ingestion
state and Dagster storage per target, explicit target selection, and no fallback
to production.

dlt, dbt and Cube select the warehouse from one configuration value. Loaders do
not hard-code a destination. Keep one set of models and Cube definitions; isolate
Snowflake differences in adapter-dispatched macros, and test models on both
adapters. Because MotherDuck shares DuckDB's dialect, Snowflake is the backend
that needs dedicated compatibility tests.

## Consequences

- Clients without Snowflake pay only MotherDuck usage, and dashboards stay up
  while models refresh.
- Two serving backends must stay supported. Every model change needs DuckDB and
  Snowflake test coverage.
- Local success never proves Snowflake or MotherDuck behavior, grants or cost.
  Development-target checks are required before a client upgrade.
- Vendor Snowflake shares named in 0010 become a per-client source concern, not
  a toolkit assumption.
- Loaders that hard-code Snowflake, such as GoodDay, must move to the shared
  destination setting.
