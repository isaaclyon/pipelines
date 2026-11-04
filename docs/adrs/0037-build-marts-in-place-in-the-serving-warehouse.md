# 0037: Build marts in place in the serving warehouse

Date: 2026-11-04

## Status

Accepted by the owner on 2026-11-04.
Supersedes [0027](0027-isolate-mart-publication-by-consistency-group.md).

## Context

0027 published marts by building each consistency group separately and then
swapping a whole Snowflake schema. This kept every reader on one validated
version, at the cost of a custom publisher, separate destinations, markers,
locks and recovery for each group.

Those groups were defined for one brand's Shopify marts, which leave the toolkit
under [0030](0030-ship-a-toolkit-with-one-deployment-per-client.md). MotherDuck
cannot rely on Snowflake's schema swap, and small brands can accept that a
dashboard briefly mixes old and new tables during a refresh.

## Decision

dbt builds marts directly in the client's serving schema. Each model replaces
its own table in one statement, as dbt does by default. Tests run in the same
`dbt build`; a failing test stops its downstream models.

Remove the custom publication mechanism: publication groups, markers, schema
swaps and their Dagster assets. Cube and Metabase read the serving schema
directly.

Schedule dbt runs outside the hours when the client's team reads dashboards.

## Consequences

- Less code to maintain, and the same build behavior on MotherDuck and Snowflake.
- During a run, a reader can see some marts refreshed and others not. A failed
  test leaves its downstream marts at the previous version while upstream marts
  are new.
- A future question that needs strict consistency across marts needs a new
  decision.
- The glossary term "publication group" and related engineering standards must
  be removed with the code.
