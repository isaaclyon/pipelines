# 0030: Ship a toolkit with one deployment per client

Date: 2026-11-04

## Status

Accepted by the owner on 2026-11-04.

## Context

The platform began as one brand's analytics stack. Its sources, marts, specs
and several decisions assume that brand's data and a shared Snowflake team
setup. The goal is now a reusable analytics platform for small direct-to-consumer
(D2C) brands that costs nothing beyond compute and warehouse usage.

One freelancer sets up and operates the platform for several brands. Each brand
has its own credentials, sources, schedules, warehouse account and private
customer data. Some brands need models that no other brand needs.

A forked template per brand is simple to start but makes every improvement a
manual merge into each fork. A shared multi-tenant deployment would mix client
data, credentials and failures on one host.

## Decision

This repository is a **toolkit**: connectors, dbt models, Cube definitions,
Dagster code locations, container images and deployment files that any client
can use. It contains no client-specific code, credentials, schedules or data.

Each client has:

- **A client repository** that pins one toolkit version and holds that client's
  configuration (enabled sources, warehouse, schedules, reporting time zone),
  any client-specific dbt models and Cube definitions, and a list of the
  environment variables its deployment needs.
- **Its own VM** running the toolkit's Docker Compose deployment, with its own
  Postgres, Dagster, Cube, Metabase and reports. Clients never share a host,
  database, warehouse account or credential.

Toolkit models are generic. The toolkit ships raw loads and one staging model
per raw table. Marts in the toolkit answer questions most D2C brands share and
must not encode one brand's rules. A rule that belongs to one brand lives in
that brand's client repository.

Connectors stay in the toolkit and are switched on per client. Growing a library
of maintained D2C connectors is part of the toolkit's value.

## Consequences

- Improvements reach every client through a version bump rather than a merge.
- The toolkit needs a stable extension interface: a client config file, a way
  to add client dbt models and Cube definitions on top of the toolkit's, and
  documented upgrade steps. Changing that interface affects every client.
- Brand-specific Shopify marts, their intermediate models, their specs and
  their Cube definitions leave the toolkit. Staging models remain.
- One VM per client multiplies small fixed costs and operating work, but a
  failure, leak or upgrade affects only one client.
- Exact client repository layout and the dbt extension mechanism are
  implementation work; see [ROADMAP.md](../../ROADMAP.md).
