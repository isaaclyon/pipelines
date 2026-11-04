# 0011: Organize data in medallion layers

Date: 2026-10-07

## Status

Accepted.

## Context

The source systems have different identifiers, schemas, and update behavior.
Consumers need consistent business data, while developers need to trace results
back to their inputs and replay transformations when definitions change.

Mixing source cleanup, reusable business logic, and consumer calculations in
the same models would make those responsibilities difficult to test and change.

## Decision

Organize relations into raw, staging, intermediate, and marts with explicit
ownership and contracts:

- **Raw:** source-shaped data and ingestion metadata required to understand and
  replay loads. External vendor relations enter the graph as declared sources.
- **Staging:** source-level parsing, naming, types, and keys. Preserve provenance
  and expose a consistent typed shape for each source entity.
- **Intermediate:** reusable joins and business transformations shared by
  downstream models. Add these models where reuse or complexity warrants them.
- **Marts:** published business-facing facts, dimensions, and other consumer
  relations with documented row grain, keys, and meaning.

Keep upstream dependencies visible through dbt sources and references. Put
warehouse transformations in [dbt Core](0013-transform-data-with-dbt-core.md),
and define exposed governed metrics over marts in
[Cube Core](0014-govern-metrics-with-cube-core.md).

## Consequences

- Source quirks are handled before consumer models, and lineage makes business
  outputs traceable to their inputs.
- Preserved raw data supports rebuilding models, within each source's retention
  and history guarantees.
- The layer names require meaningful boundaries; duplicating a relation merely
  to fill a layer adds work without improving its contract.
- Consumer-facing models need explicit grain and tests to prevent ambiguous
  joins and double counting.
- Materialization and incremental strategies still need to match each model's
  volume and update behavior.
