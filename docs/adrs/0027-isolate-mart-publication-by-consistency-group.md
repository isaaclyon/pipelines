# 0027: Isolate mart publication by consistency group

Date: 2026-10-29

## Status

Accepted.
Extends [ADR 0022](0022-publish-marts-through-forward-only-transformation-layers.md).
Uses the Snowflake-only publication boundary in
[ADR 0025](0025-use-duckdb-for-deterministic-tests.md).

## Context

The initial publisher builds and validates every enabled mart, then replaces
one consumer schema. This preserves a consistent version but
makes unrelated business questions depend on one another. Enabling inventory
can block fresh orders, and each added mart increases every publication's work.
Each dbt command must still fit the bounded execution policy.

Orders and their availability evidence must agree. Current customer profiles
and inventory do not require the same publication generation as orders. Merely
selecting fewer models while replacing the same physical destination would
remove unrelated data or keep shared recovery and locking.

## Decision

Publish by the smallest declared consistency group, with orders and availability
together, customer profiles and their child details together, and inventory in
its own group. Assign every maintained mart explicitly. A publication selects
a complete group, not an arbitrary subset of its models.

Build the selected group's ancestors and required checks before publication.
Do not select unrelated descendant tests through shared ancestors. Reject
cross-group mart dependencies or checks that require another group's models;
move reusable logic upstream or combine outputs that truly require one version.
Shared project parsing and static policy validation remain project-wide.

Each group owns a separate physical destination, publication marker,
compatibility history, lock, and recovery. Reuse the existing atomic Snowflake schema
replacement mechanism rather than adding partial schema updates. Different
groups may refresh independently; shared writable raw inputs still serialize.
Keep current command time bounds and consumer access restrictions.

Readers route to each group's destination and refresh from its own marker.
They must not assume equal generations across groups. Existing combined
publications migrate by prepublishing separate destinations, moving consumers,
and retaining the old destination through the compatibility window. Never
silently remove old relations or change live data during deployment.

## Consequences

- Inventory and customer correctness failures do not prevent order publication.
  Build work and recovery are bounded by the selected group, not every mart.
- A shared ancestor may rebuild in more than one group. This duplicates some
  computation in exchange for independent refresh and failure handling.
- More destinations and refresh decisions require explicit ownership. No
  customer or inventory cadence is inferred from the order schedule.
- Cross-group joins cannot imply a common validated version. Cube keeps public
  order/customer views but does not offer the hidden legacy join in independent
  mode. A future cross-group business question needs an explicit consistency
  requirement rather than silently recreating platform-wide publication.
- Migration requires coordinated configuration changes and reader checks.
  Local fixtures establish build isolation; live Snowflake acceptance
  and activation remain explicit environment-specific work.
