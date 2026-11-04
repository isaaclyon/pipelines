# 0022: Publish marts through forward-only transformation layers

Date: 2026-10-11

## Status

Accepted. Extends [0011](0011-organize-data-in-medallion-layers.md).

## Context

The first maintained models need predictable dependencies and a stable surface
for consumers. Allowing consumers to depend on cleanup internals would make
source changes expensive. Publishing only aggregates would force analysts back
to raw data for useful detail and reconciliation.

The layer responsibilities already exist in ADR 0011. This record defines their
dependency direction and consumer boundary before proving them with Shopify.

## Decision

Use raw → staging → intermediate → marts as the logical transformation direction.
An arrow means that the relation on the right consumes the relation on the left.

- Staging reads declared raw sources and performs source-level normalization.
- Intermediate reads staging and other intermediate models for joins, reshaping,
  and business transformations.
- Marts read staging, intermediate, and other marts. A simple transformation may
  go directly from staging to marts; do not create pass-through intermediate
  models merely to fill a layer.
- Same-layer dependencies must remain acyclic and respect the layer's purpose.
  Staging dependencies stay within source normalization. Shared logic needed by
  an upstream layer belongs upstream; staging and intermediate cannot read marts.
- Intermediate and marts obtain source data through staging. Declare dependencies
  with `source()` and `ref()`; hard-coded relation names must not bypass the graph.

Publish marts as the supported consumer interface. Preserve useful detail at
documented grains, including source identifiers, relevant attributes, canonical
values, and provenance needed to explain results. Detail marts can represent
orders, order lines, or transactions; marts do not imply aggregation. Publish
fields deliberately with documented meaning and applicable access restrictions.
Consumers should not need raw access for ordinary supported analysis.

Developer and diagnostic access to internal layers remains possible through
appropriate privileges. Such access does not make those relations stable consumer
contracts. Identify missing consumer data and extend the appropriate mart.

[Cube](0014-govern-metrics-with-cube-core.md) supplies governed measures,
dimensions, joins, and query semantics over marts. Reporting applications can
consume that interface. Direct SQL exploration over marts remains supported;
exploratory calculations do not become competing governed metric definitions.

Treat logical layer, physical schema, and consumer permissions as separate
concerns. Schema naming must preserve environment and developer/CI isolation.
Folders and dbt model access settings alone do not enforce warehouse permissions.
Exact schema/model naming, exceptions for auxiliary resources such as seeds,
and enforcement mechanisms remain implementation design work.

## Consequences

- Internal cleanup can evolve behind stable, detailed consumer relations.
- Useful detail increases documentation, testing, and compatibility obligations;
  mart design must account for analytical exploration as well as reporting.
- Forward dependencies keep business logic understandable but sometimes require
  moving shared logic upstream instead of reusing a downstream result.
- Dependency checks, schema isolation tests, consumer grants, and a Shopify proof
  must make these boundaries executable. Acceptance of this ADR does not imply
  that those mechanisms exist.
- [Engineering standards](../../engineering-standards.md#transformation-contracts-and-tests)
  define the testing baseline.
