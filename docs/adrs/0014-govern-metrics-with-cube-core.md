# 0014: Govern metrics with Cube Core

Date: 2026-10-07

## Status

Accepted. Local serving equivalence is superseded by [0025](0025-use-duckdb-for-deterministic-tests.md).
Extended by [0034](0034-serve-dashboards-with-metabase-through-cube.md) for Metabase dashboards.

## Context

Applications and analytical consumers need consistent measures, dimensions, and
join behavior over the marts. Reimplementing those definitions in each consumer
would allow nominally identical metrics to use different filters or aggregation
rules.

The platform needs a self-hosted semantic layer that can expose a shared query
interface over Snowflake and the local development backend.

## Decision

Use Cube Core as the authority for exposed governed metrics over dbt marts.
Version its measures, dimensions, joins, and consumer-facing semantic views in
the monorepo. Define each governed metric once and document its meaning, units,
time dimension, and relevant exclusions.

dbt owns the underlying row-level data and business transformations. Cube owns
the exposed aggregation and query semantics built on those relations. Keep the
boundary explicit when a calculation spans both: marts supply canonical values
and flags, and Cube defines how consumers query and aggregate them.

Applications and other consumers requesting governed metrics use Cube's query
interfaces. Direct warehouse exploration must identify exploratory calculations
so they do not silently become competing metric definitions.

Self-host Cube with the topology in
[0002](0002-operate-services-with-docker-compose.md). Exercise representative
semantic queries against both DuckDB and Snowflake according to
[0015](0015-support-duckdb-and-snowflake.md). Verify intended consumer integrations
against the self-hosted feature set as they are introduced.

## Consequences

- Consumers share metric definitions and semantic join behavior, with changes
  reviewed alongside the marts they depend on.
- Cube's query, caching, and pre-aggregation services become part of deployment
  and local integration testing.
- SQL flexibility alone cannot guarantee metric consistency; consumers must
  honor the governed interface and documented definitions.
- Semantic joins, filters, time zones, and aggregation behavior require tests,
  including checks for double counting and changed metric results.
- Self-hosted Core and commercial Cube have different feature sets. A specific
  consumer integration may require additional implementation work.

## Addendum: 2026-10-14

Until the first pre-aggregation exists, deployments run one Cube API service
with Cube's in-memory cache and queue (`CUBEJS_CACHE_AND_QUEUE_DRIVER=memory`).
The refresh worker and Cube Store were removed because they served no cubes.
Add both back, with a digest-pinned Cube Store image in the release manifest,
in the change that introduces the first pre-aggregation or a second API
instance.

## Addendum: 2026-10-22

dbt YAML owns the meaning of published mart columns: each column's description,
type and `pii` flag. The transformations package records them in its reviewed
mart-contract snapshot, `packages/transformations/dbt/customer-mart-contracts.json`,
which its checks keep equal to the customers-enabled dbt manifest. The metrics
image copies that snapshot, and `packages/metrics/mart-columns.cjs` builds Cube
dimensions that read a mart column under its own name from it. A column the
snapshot lacks, an unmapped type or a restricted mart stops compilation.

Cube keeps everything about exposure and aggregation: the column allowlist,
computed and renamed dimensions, measures, joins, views, filters, aggregation
rules and the customers switch. Improve a column's wording in dbt, not in Cube.

We did not adopt `cube_dbt`, Cube's dbt package. Its models are YAML with Jinja
and cannot use `publication.cjs`, so the schema, data source, refresh key and
customers switch would need a second implementation in Python. It also copies
all dbt column `meta` into Cube's `/meta` output. A checked-in snapshot keeps
the Cube image offline and matched to the same commit's marts; Cube does not
fetch a manifest at runtime.
