# 0015: Support DuckDB and Snowflake

Date: 2026-10-07

## Status

Superseded by [0025](0025-use-duckdb-for-deterministic-tests.md).

## Context

DuckDB provides fast, offline model iteration, while Snowflake is the remote
warehouse. The local environment must exercise real transformation and semantic
behavior rather than a separate implementation that can drift from production.

SQL dialects and database semantics differ. Successful compilation, a syntax
allowlist, or a passing DuckDB test alone cannot prove Snowflake compatibility.

## Decision

Support the maintained pipeline flow, dbt models, and Cube semantic definitions
on both backends. Keep one set of transformation and semantic definitions and
explicit backend configuration. Give API, file, BigQuery, and vendor-share
inputs deterministic local fixture equivalents with the same downstream shape.

Use portable SQL and dbt's cross-database macros first. Isolate unavoidable
database differences in adapter-dispatched macros with implementations and
execution tests for both adapters. Check that model SQL does not introduce
database-specific syntax outside those boundaries.

Execute the same representative fixtures in DuckDB and development Snowflake.
Compare stable results with deliberate normalization of database representations
and assert expected types. Test full refreshes, incremental repeat runs and
updates, money precision, timestamps, nulls, and nested data where applicable.
Execute representative Cube queries, including aggregate and join behavior.

Use the isolated targets in
[0006](0006-isolate-development-and-production-environments.md). Coordinate local
DuckDB writers and provide a stable published read boundary for Cube; verify
the precise publication mechanism during scaffolding.

## Consequences

- Local iteration exercises the maintained business logic without vendor or
  warehouse credentials, and remote tests verify actual Snowflake behavior.
- Both adapters and their macros must remain supported when dependencies or
  models change.
- Fixture maintenance, cross-database execution, and semantic-query checks add
  CI work and Snowflake compute cost.
- Result normalization must preserve business differences, such as money
  precision and time-zone meaning, rather than hide them.
- DuckDB does not reproduce Snowflake grants, infrastructure, performance, or
  every operational feature. Those need remote checks where relevant.
