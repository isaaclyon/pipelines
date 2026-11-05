# 0034: Serve dashboards with Metabase through Cube

Date: 2026-11-04

## Status

Accepted by the owner on 2026-11-04.
Extends [0014](0014-govern-metrics-with-cube-core.md).

## Context

Brand teams need dashboards, not only an API. The toolkit must stay free apart
from compute, so the dashboard tool must be open source and self-hosted.
Metabase's open-source edition is widely used and easy for non-analysts.

0014 makes Cube the single definition of governed metrics. A dashboard tool that
queries the warehouse directly would let each chart define its own metric. Cube
exposes a SQL API that speaks the Postgres wire protocol, and Metabase can
connect to it as a Postgres database.

## Decision

Run Metabase's open-source edition on each client VM.

- Metabase connects to Cube's SQL API for governed metrics. It does not receive
  warehouse credentials by default.
- Metabase stores its application data in its own database on the client's
  Postgres server, separate from Dagster's database.
- Serve Metabase behind Caddy with HTTPS. Client users sign in to Metabase;
  Cube's API stays private to the VM network.
- The toolkit ships starter dashboards for generic marts as they are built.

## Consequences

- Clients get dashboards that use the same metric definitions as every other
  consumer.
- Charts are limited to what Cube exposes. A missing measure or dimension is
  added in Cube, not in Metabase SQL.
- Metabase's support for Cube's SQL API has limits that must be tested with
  real dashboards before release.
- Metabase is another service to upgrade, back up and secure on each VM.
