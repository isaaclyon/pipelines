# 0012: Orchestrate assets with Dagster

Date: 2026-10-07

## Status

Accepted.

## Context

Ingestion must complete before its downstream transformations can run, and
operators need to inspect failures, retry work, and manage schedules and
backfills. Local development must exercise that coordination before deployment.

Independent scripts and schedules would leave dependencies and run state spread
across several tools. The platform needs one place to express and observe its
ingestion and transformation graph.

## Decision

Use open-source Dagster as the orchestrator from the initial scaffold. Model
loaded and transformed data as assets with stable keys and declared upstream
dependencies. Integrate dbt's model graph and execution results into Dagster.

Dagster owns pipeline scheduling, run coordination, retries, and backfill
execution. Keep extraction logic in ingestion packages, SQL transformations in
dbt, and semantic definitions in Cube. Cube's internal cache and pre-aggregation
refresh lifecycle remains part of its serving responsibilities.

Use the containerized service and run architecture in
[0002](0002-operate-services-with-docker-compose.md) and
[0004](0004-launch-dagster-runs-in-isolated-containers.md). Coordinate dependencies
across the separate code locations specified in
[0003](0003-isolate-dagster-code-locations.md) through asset automation or sensors.
Exercise the complete flow locally with deterministic fixtures.

## Consequences

- Dependencies, run results, schedules, and backfills have a common operational
  interface and can be tested before production deployment.
- Pipeline code remains separate from the orchestration mechanism.
- Dagster adds long-running services, metadata persistence, and version
  compatibility work to the platform.
- Correct asset dependencies and materialization events are required for
  downstream coordination; separate code locations execute in separate runs.
- Orchestration success does not establish source completeness or business
  correctness. Reconciliation and model tests remain required.
