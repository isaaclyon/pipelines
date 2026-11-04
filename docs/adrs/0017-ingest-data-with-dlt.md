# 0017: Ingest data with dlt

Date: 2026-10-07

## Status

Accepted. Amended on 2026-10-14 by
[0024](0024-load-shopify-orders-with-dlt-incremental-merge.md): a custom source
still uses dlt incremental state, merge and schema handling instead of
building its own state, ledger or revision framework around the loader.

## Context

The platform ingests commerce, marketing, support, and finance data from APIs,
databases, and vendor exports. Connector coverage varies, and some sources need
custom extraction logic. We need a loading framework that fits the accepted
[Python](0009-use-python-for-data-pipelines.md) and
[Dagster](0012-orchestrate-assets-with-dagster.md) architecture and supports
[Snowflake and offline DuckDB development](0015-support-duckdb-and-snowflake.md).

Managed connectors reduce extraction work for supported systems but introduce
per-source costs and less control over unsupported endpoints. Self-hosting a
connector platform adds a separate runtime and operating burden. Handwritten
extraction and warehouse loading for every source would duplicate schema,
checkpoint, and loading behavior.

## Decision

Use the open-source dlt (data load tool) library as the default framework for
maintained ingestion pipelines. Run ingestion through Dagster, adding the
`dagster-dlt` integration when a pipeline uses it, and preserve the independent
package dependencies and runtime boundaries established in
[0008](0008-isolate-package-dependencies.md) and
[0003](0003-isolate-dagster-code-locations.md).

Choose the simplest source implementation that meets the source contract:

- Reuse a verified connector after checking its current API support, endpoint
  coverage, and maintenance.
- Use dlt's built-in database or filesystem sources for suitable inputs.
- Use declarative REST API configuration for standard HTTP APIs.
- Use custom Python sources and resources for GraphQL, bulk operations, or
  extraction behavior the existing sources cannot express reliably.

Load source-shaped data and ingestion metadata into the raw layer defined in
[0011](0011-organize-data-in-medallion-layers.md). Keep warehouse transformations
in [dbt Core](0013-transform-data-with-dbt-core.md). Native vendor shares and
managed connectors remain source-specific options when they materially improve
reliability or maintenance cost; record their ownership and downstream source
contract when adopted.

Give each source explicit keys, write behavior, cursor semantics where relevant,
schema handling, and retention and replay guarantees. Use merge for mutable
entities that require keyed updates, append for immutable records with a defined
replay strategy, and replace for intentional complete snapshots. Incremental
extraction does not by itself determine the write disposition.

Keep destination configuration, pipeline state, and writable namespaces isolated
by environment under
[0006](0006-isolate-development-and-production-environments.md). Exercise source
contracts with deterministic fixtures in DuckDB and verify destination behavior
in development Snowflake. Persist recoverable state outside run-container layers
and test retry and recovery behavior.

Use the [local dlt skill](../../.agents/skills/using-dlt/SKILL.md) for implementation
guidance. dltHub's paid runtime and AI harness are separate products and are not
required by this choice.

## Consequences

- Verified, declarative, and custom sources share one Python loading framework
  and can participate in the same orchestration and downstream dbt graph.
- dlt provides normalization, schema handling, incremental state, and destination
  loading, reducing duplicated connector infrastructure.
- We own source-specific completeness, rate limits, deletion behavior, history,
  and reconciliation. A successful load does not establish complete source data.
- Connector and destination upgrades need pinned dependencies and focused
  regression checks, including pagination, replay, updates, and state recovery.
- Merge retains current values unless history is explicitly designed. Raw-layer
  placement alone does not guarantee historical versions or indefinite replay.
- DuckDB tests provide fast feedback; they cannot establish Snowflake grants,
  loading behavior, or operational performance without remote checks.
