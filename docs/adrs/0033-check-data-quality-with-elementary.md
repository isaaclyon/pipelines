# 0033: Check data quality with Elementary

Date: 2026-11-04

## Status

Accepted by the owner on 2026-11-04.

## Context

A freelancer operating several client VMs needs to learn about stale sources,
failed tests and unexpected volume changes before the client does. dbt tests
report failures for a single run, but they do not keep history, detect
anomalies or send alerts.

Elementary's open-source edition is a dbt package plus a command-line tool,
`edr`. The package records dbt results and runs anomaly tests in the warehouse.
`edr` builds a static HTML report and sends alerts to Slack or Teams. Its
published install options include DuckDB and Snowflake, and Elementary lists
MotherDuck as an integration (checked 2026-11-04). Elementary Cloud is a paid
product and is out of scope.

## Decision

Use Elementary's open-source dbt package and `edr` CLI for data quality
monitoring on every client.

- Install the package in the toolkit's dbt project. Its tables live in a
  dedicated schema in the client's warehouse.
- Every enabled source has freshness and volume checks. Staging models carry
  schema-change checks. Generic marts add anomaly checks on their key measures.
- After each scheduled dbt run, Dagster runs `edr` to send alerts and rebuild
  the report.
- Serve the report from the client VM behind Caddy with authentication.
- Alert channels are client configuration.

## Consequences

- Each client gets test history, anomaly detection and alerts without a paid
  service.
- Elementary runs extra queries and stores results in the warehouse, which
  adds a small compute and storage cost.
- The package and `edr` must work on DuckDB, MotherDuck and Snowflake. This is
  not yet verified on this stack; the first implementation must prove it on
  each backend.
- Upgrading Elementary becomes part of toolkit releases.
