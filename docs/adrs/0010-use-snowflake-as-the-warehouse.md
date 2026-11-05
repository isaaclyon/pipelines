# 0010: Use Snowflake as the warehouse

Date: 2026-10-07

## Status

Superseded on 2026-11-04 by [0031](0031-serve-each-client-from-motherduck-or-snowflake.md).
Previously accepted.

## Context

The platform must combine commerce, marketing, fulfillment, and customer data.
Decile and Heap deliver data through Snowflake shares, while other inputs arrive
through APIs, files, or existing BigQuery tables.

Snowflake is the intended remote warehouse. Hosting orchestration on a VM does
not remove the need for durable analytical storage and warehouse execution.

## Decision

Use Snowflake as the warehouse for remote development and production. Store
ingested and modeled relations there and execute remote dbt transformations
against it. Connect Cube to the published marts.

Treat vendor Snowflake shares as external source relations. Define source-level
retention and history requirements when deciding whether to copy shared data.
Replicate required GoodDay BigQuery tables into the warehouse as source data;
preserve their upstream meaning and provenance.

Keep development and production writable targets and credentials separate,
following [0006](0006-isolate-development-and-production-environments.md).
Use DuckDB for offline iteration under the compatibility contract in
[0015](0015-support-duckdb-and-snowflake.md).

## Consequences

- Snowflake shares can be consumed in the same warehouse as the modeled data.
- Warehouse storage and compute are operated independently of the Hetzner VM's
  pipeline and semantic services.
- Warehouse execution incurs service costs and requires role, credential, and
  resource configuration.
- Vendor shares remain subject to their provider's availability and history
  contract; source retention must be considered explicitly.
- Snowflake-specific infrastructure is part of the remote deployment, while
  portable model logic requires ongoing cross-database testing.
