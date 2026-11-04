# 0035: Send data to operational tools with dlt

Date: 2026-11-04

## Status

Accepted by the owner on 2026-11-04.

## Context

D2C brands get value from sending modeled data back to the tools their teams
use: customer segments and lifetime value to Klaviyo, conversions to Meta, and
tags to Shopify. This is called reverse ETL.

Hosted reverse ETL tools charge per destination or row. The main self-hosted
option, Multiwoven, reads Snowflake but not DuckDB or MotherDuck, and its
community activity has declined since its acquisition (checked 2026-11-04).
Adding a separate service would also add another scheduler and state store.

dlt is already the ingestion tool ([0017](0017-ingest-data-with-dlt.md)). Its
custom destinations let a Python function receive batches of rows and send them
to any API.

## Decision

Build reverse ETL as dlt pipelines with custom destinations, run as Dagster
assets downstream of the marts they read.

- Each sync reads one mart or Cube query with a documented grain and sends it
  to one destination.
- Syncs are idempotent: sending the same rows twice leaves the destination in
  the same state.
- Each sync is switched on per client and runs only in production unless a
  development destination account is configured.
- Syncs parse destination responses at the trust boundary, under
  [0016](0016-parse-data-at-trust-boundaries.md), and record failures without
  stopping other syncs.
- Start with Klaviyo profile properties and segments, then Meta conversions.

## Consequences

- No new service or vendor; reverse ETL uses the same scheduling, state,
  retries and logs as ingestion.
- Each destination is code the toolkit maintains, including rate limits,
  batching and API changes.
- Syncs write to systems that clients use to contact customers. Mistakes reach
  real people, so each sync needs offline tests against recorded responses and
  a dry-run mode.
