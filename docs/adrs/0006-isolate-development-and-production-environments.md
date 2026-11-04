# 0006: Isolate development and production environments

Date: 2026-10-07

## Status

Accepted. Local serving equivalence is superseded by [0025](0025-use-duckdb-for-deterministic-tests.md).

## Context

Developers need to reproduce pipeline behavior locally and iterate without
interfering with production. DuckDB enables fast offline work, while a remote
Snowflake development database is necessary to verify actual warehouse behavior.

Sharing credentials, ingestion cursors, writable schemas, or service state
between environments would make development runs affect production and make
concurrent validation unreliable.

## Decision

Support the following explicit targets using the shared container definitions:

| Target | Data backend | Purpose |
| --- | --- | --- |
| Local | DuckDB and deterministic fixtures | Offline ingestion, transformation, and semantic iteration |
| Remote development | Dedicated Snowflake development database | Integration and warehouse compatibility checks |
| Production | Production Snowflake database | Scheduled production pipelines and semantic queries |

Allow an explicitly selected local Snowflake-development target for debugging.
Local and ordinary CI execution default to fixtures and must not require live
vendor credentials. Provide fixture data for API, file, BigQuery, and Snowflake
share inputs while preserving their downstream contracts.

Separate environment credentials, writable data targets, ingestion checkpoints,
Dagster metadata, Cube configuration and caches, networks, and persistent
volumes. Within remote development, give each developer or CI run an isolated
writable namespace. Use target-specific roles that cannot write to production
from a development session. Select a target explicitly; configuration failure
must not fall back to production.

Reproduce the service topology and execution lifecycle locally. Preserve common
model and semantic definitions across DuckDB and Snowflake through portable SQL
and adapter-specific macros. Execute compatibility tests in development
Snowflake as well as DuckDB, covering incremental behavior and representative
Cube queries.

Coordinate local DuckDB writers and give Cube a stable published view of marts
that does not contend with the working database. The exact snapshot format and
reader-refresh mechanism require verification during scaffolding.

## Consequences

- Developers can exercise the complete flow locally and investigate warehouse
  behavior in an isolated Snowflake target.
- Fixtures and publication boundaries need maintenance as source contracts evolve.
- DuckDB reproduces application behavior, not Snowflake infrastructure, grants,
  performance, or every SQL semantic. Tests in Snowflake remain necessary.
- Independent state consumes storage and requires cleanup. Snowflake development
  and integration checks incur warehouse costs.
- Isolation depends on enforced privileges and explicit resource names, in
  addition to configuration conventions.
