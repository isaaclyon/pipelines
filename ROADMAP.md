# Roadmap

Updated 2026-11-04.

## Goal

A strong, reusable analytics platform for small direct-to-consumer (D2C)
brands. It runs in Docker on one VM per brand and uses open-source tools, so a
brand pays only for compute and warehouse usage.

One freelancer sets up and operates the platform for several brands. This
repository is the **toolkit**; each brand has a small **client repository** that
pins a toolkit version and holds its own configuration and custom models
([ADR 0030](docs/adrs/0030-ship-a-toolkit-with-one-deployment-per-client.md)).

| Concern | Tool | Decision |
| --- | --- | --- |
| Serving warehouse | MotherDuck (default) or Snowflake, per client | [0031](docs/adrs/0031-serve-each-client-from-motherduck-or-snowflake.md) |
| Local development | DuckDB | [0028](docs/adrs/0028-develop-locally-before-shared-snowflake-release.md), [0029](docs/adrs/0029-load-every-source-into-local-duckdb.md) |
| Ingestion | dlt | [0017](docs/adrs/0017-ingest-data-with-dlt.md) |
| Orchestration | Dagster | [0012](docs/adrs/0012-orchestrate-assets-with-dagster.md) |
| Transformation | dbt Core | [0013](docs/adrs/0013-transform-data-with-dbt-core.md), [0037](docs/adrs/0037-build-marts-in-place-in-the-serving-warehouse.md) |
| Metrics | Cube Core | [0014](docs/adrs/0014-govern-metrics-with-cube-core.md) |
| Dashboards | Metabase (open source) | [0034](docs/adrs/0034-serve-dashboards-with-metabase-through-cube.md) |
| Data quality | Elementary (open source) | [0033](docs/adrs/0033-check-data-quality-with-elementary.md) |
| Reverse ETL | dlt custom destinations | [0035](docs/adrs/0035-send-data-to-operational-tools-with-dlt.md) |
| Secrets | Environment files; no secret manager in the repo | [0032](docs/adrs/0032-read-secrets-from-environment-files.md) |
| Releases | Pinned toolkit versions, one client upgrade at a time | [0036](docs/adrs/0036-upgrade-clients-one-at-a-time-on-pinned-toolkit-versions.md) |

The first client is **Lola Blankets**. Lola serves from MotherDuck.

## Where the repository stands

- Containers, Dagster code locations, dlt loaders and package checks work locally.
- Connectors exist for Shopify orders and customers, Loop Returns, Impact
  Radius, GoodDay inventory and Google Sheets. All stay in the toolkit.
- dbt marts, intermediate models, Cube models and several specs encode one
  brand's Shopify rules. They do not belong in the toolkit.
- A custom Snowflake publication mechanism and 1Password templates remain.
- There is no VM deployment, MotherDuck support, Metabase, Elementary or
  reverse ETL.

## Phases

Each phase ends with a check that a person can run. Work one phase at a time
unless a later item is independent and small.

### Phase 1: Strip back to the core

Remove brand-specific and superseded code so the toolkit starts from raw loads
and staging models.

- Remove all dbt marts and intermediate models under
  `packages/transformations/dbt/models/marts/` and `models/intermediate/`, their
  mart-contract snapshots, Cube exposures, and the Cube models in
  `packages/metrics/model/`.
- Retire the specs that define those marts: Shopify order marts, order payment
  eligibility and new/repeat customer mix.
- Review each staging model: keep renames, types, keys and provenance; move any
  business rule out.
- Remove the publication mechanism (`publication.py`, `development_publication.py`
  and related checks) under ADR 0037, and the "publication group" glossary term and
  standards.
- Remove `deploy/secrets/` and every 1Password instruction under ADR 0032. Each
  service validates its required environment variables at startup.
- Leave Cube running with an empty or placeholder model so its smoke check still passes.

**Done when** `./scripts/platform local check` passes with only raw and staging
models, and no file mentions 1Password.

### Phase 2: Split the toolkit from the client

- Define the client config file: enabled sources, warehouse, schedules,
  reporting time zone and alert channels.
- Decide and build how a client repository adds dbt models and Cube definitions
  on top of the toolkit's.
- Document every environment variable and mounted file each service needs.
- Publish versioned toolkit images under ADR 0036.
- Create a sample client repository with fixtures.

**Done when** the sample client builds against a pinned toolkit version and
passes its checks offline.

### Phase 3: Serve from MotherDuck or Snowflake

MotherDuck comes first, because Lola uses it.

- One warehouse setting drives dlt, dbt and Cube. Remove hard-coded destinations,
  starting with GoodDay.
- Add MotherDuck support to the dbt profile, loaders and Cube driver.
- Keep dbt tests running on both DuckDB and Snowflake adapters.

**Done when** the sample client loads, builds and answers a Cube query on
MotherDuck and on Snowflake development targets.

### Phase 4: Run Lola Blankets on a VM

- Compose deployment for one VM: Caddy with HTTPS, Postgres (separate databases
  for Dagster and Metabase), Dagster, Cube and Metabase.
- Nightly backups of Postgres, with a tested restore.
- Answer Lola's first business question, "How much money did we make?", the way
  Shopify's own sales reports do: gross sales, discounts, returns, net sales,
  shipping, taxes, total sales, order count and average order value, by day and
  month. Build the generic orders marts and Cube measures this needs, and track
  the question as an outcome issue under the [delivery policy](docs/delivery-policy.md).
  The acceptance owner is the operator, acting as Lola's freelance analytics
  lead. The reference is the sales overview in Lola's Shopify Analytics dashboard.
- Connect Metabase to Cube's SQL API and build Lola's first dashboard.
- Update the "Current stage" section of `AGENTS.md`, because Lola becomes the
  first real deployment.

**Done when** a fresh VM goes from nothing to a scheduled Shopify run with one
documented command, and Lola's acceptance owner accepts the first dashboard.

### Phase 5: Data quality with Elementary

- Add the Elementary dbt package and prove it on DuckDB, MotherDuck and Snowflake.
- Freshness and volume checks on every enabled source; schema-change checks on
  staging; anomaly checks on generic mart measures.
- Dagster runs `edr` after each scheduled build to send Slack alerts and rebuild
  the report, served behind Caddy.

**Done when** a deliberately stale source on Lola's development target raises a
Slack alert and shows in the report.

### Phase 6: Grow the source and mart library

- Sources, in rough priority: Klaviyo, Meta Ads, Google Ads, GA4, Recharge or
  Stripe, Amazon Seller, TikTok Ads. Use dlt verified sources where they fit.
- Generic marts: orders, customers, cohorts and lifetime value, blended ad cost
  per new customer, contribution margin.
- Starter Metabase dashboards for each generic mart.

**Done when** each new source and mart has offline fixtures, staging, docs and
tests on both adapters, and is switched on for at least one client.

### Phase 7: Reverse ETL

- dlt custom destinations run as Dagster assets, with dry-run mode and recorded
  API responses for offline tests.
- First syncs: Klaviyo profile properties and segments, then Meta conversions.

**Done when** a Klaviyo sync for Lola runs on schedule, repeats without
duplicates, and its dry run matches its real run.

## Open questions

- Which VM host and size to standardize on for clients.
