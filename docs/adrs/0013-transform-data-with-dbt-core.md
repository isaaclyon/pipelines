# 0013: Transform data with dbt Core

Date: 2026-10-07

## Status

Accepted.

## Context

The platform needs versioned SQL transformations, visible model dependencies,
and tests for the business relations it publishes. Those transformations must
run in both DuckDB and Snowflake and integrate with Dagster execution.

Embedding warehouse SQL in orchestration functions would tie model changes to
pipeline code and obscure dependencies and transformation contracts.

## Decision

Use dbt Core to own warehouse transformations from staging through marts.
Declare source relations and model dependencies through `source()` and `ref()`.
Keep model definitions, macros, documentation, and tests with the transformation
package in the monorepo.

Run dbt through the transformation code location, with Dagster exposing model
assets and recording execution results. Preserve the independent package and
runtime environments specified in [0008](0008-isolate-package-dependencies.md)
and [0003](0003-isolate-dagster-code-locations.md).

Use the DuckDB and Snowflake adapters with explicit target configuration.
Maintain portable models and dispatched macros under
[0015](0015-support-duckdb-and-snowflake.md). Test transformation behavior with
small fixtures and assert published relations' contracts with data tests.
Use incremental materializations where the data's update pattern and volume
warrant them, with tests for repeat runs and updates.

## Consequences

- Transformation dependencies, SQL, documentation, and tests can be reviewed
  together and surfaced in Dagster.
- Models can be exercised independently of a complete ingestion run using
  controlled source fixtures.
- dbt Core, adapters, and the Dagster integration need compatible pinned versions.
- Incremental models require explicit keys and update behavior; passing a
  full-refresh test alone does not establish incremental correctness.
- dbt governs relational transformation logic. Exposed shared metric definitions
  belong to [Cube Core](0014-govern-metrics-with-cube-core.md).
