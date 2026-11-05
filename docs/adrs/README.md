# Architecture decision records

ADRs record architectural choices and their rationale. Accepted records define
the direction of the platform; they do not indicate that implementation is
complete. [Engineering standards](../../engineering-standards.md) define the
rules for implementing those choices.

## Records

| Number | Decision | Status |
| --- | --- | --- |
| 0001 | [Run platform components in containers](0001-run-platform-components-in-containers.md) | Accepted |
| 0002 | [Operate services with Docker Compose](0002-operate-services-with-docker-compose.md) | Accepted |
| 0003 | [Isolate Dagster code locations](0003-isolate-dagster-code-locations.md) | Accepted |
| 0004 | [Launch Dagster runs in isolated containers](0004-launch-dagster-runs-in-isolated-containers.md) | Accepted |
| 0005 | [Promote immutable release images](0005-promote-immutable-release-images.md) | Accepted; extended by 0036 |
| 0006 | [Isolate development and production environments](0006-isolate-development-and-production-environments.md) | Local serving equivalence superseded by 0025; targets superseded by 0031 |
| 0007 | [Use a monorepo](0007-use-a-monorepo.md) | Accepted |
| 0008 | [Isolate package dependencies](0008-isolate-package-dependencies.md) | Accepted |
| 0009 | [Use Python for data pipelines](0009-use-python-for-data-pipelines.md) | Accepted |
| 0010 | [Use Snowflake as the warehouse](0010-use-snowflake-as-the-warehouse.md) | Superseded by 0031 |
| 0011 | [Organize data in medallion layers](0011-organize-data-in-medallion-layers.md) | Accepted |
| 0012 | [Orchestrate assets with Dagster](0012-orchestrate-assets-with-dagster.md) | Accepted |
| 0013 | [Transform data with dbt Core](0013-transform-data-with-dbt-core.md) | Accepted |
| 0014 | [Govern metrics with Cube Core](0014-govern-metrics-with-cube-core.md) | Local serving equivalence superseded by 0025; extended by 0034 |
| 0015 | [Support DuckDB and Snowflake](0015-support-duckdb-and-snowflake.md) | Superseded by 0025 |
| 0016 | [Parse data at trust boundaries](0016-parse-data-at-trust-boundaries.md) | Accepted |
| 0017 | [Ingest data with dlt](0017-ingest-data-with-dlt.md) | Accepted |
| 0018 | [Manage environment secrets with 1Password](0018-manage-environment-secrets-with-1password.md) | Superseded by 0032 |
| 0019 | [Deploy releases through one source-agnostic workflow](0019-deploy-releases-through-one-source-agnostic-workflow.md) | Deprecated |
| 0020 | [Keep consecutive releases compatible](0020-keep-consecutive-releases-compatible.md) | Deprecated |
| 0021 | [Run data changes as orchestrated pipeline work](0021-run-data-changes-as-orchestrated-pipeline-work.md) | Partially superseded by 0023 for storage maintenance and 0028 for local experiments |
| 0022 | [Publish marts through forward-only transformation layers](0022-publish-marts-through-forward-only-transformation-layers.md) | Accepted |
| 0023 | [Maintain Dagster storage in an operator window](0023-maintain-dagster-storage-in-an-operator-window.md) | Deprecated |
| 0024 | [Load Shopify orders with dlt incremental merge](0024-load-shopify-orders-with-dlt-incremental-merge.md) | Accepted |
| 0025 | [Use DuckDB for deterministic tests](0025-use-duckdb-for-deterministic-tests.md) | Serving restriction superseded by 0028 |
| 0026 | [Scale dbt checks with the change](0026-scale-dbt-checks-with-the-change.md) | Accepted |
| 0027 | [Isolate mart publication by consistency group](0027-isolate-mart-publication-by-consistency-group.md) | Superseded by 0037 |
| 0028 | [Develop locally before shared Snowflake release](0028-develop-locally-before-shared-snowflake-release.md) | Accepted; serving target replaced by 0031 |
| 0029 | [Load every source into local DuckDB](0029-load-every-source-into-local-duckdb.md) | Accepted; credentials rule superseded by 0032 |
| 0030 | [Ship a toolkit with one deployment per client](0030-ship-a-toolkit-with-one-deployment-per-client.md) | Accepted |
| 0031 | [Serve each client from MotherDuck or Snowflake](0031-serve-each-client-from-motherduck-or-snowflake.md) | Accepted |
| 0032 | [Read secrets from environment files](0032-read-secrets-from-environment-files.md) | Accepted |
| 0033 | [Check data quality with Elementary](0033-check-data-quality-with-elementary.md) | Accepted |
| 0034 | [Serve dashboards with Metabase through Cube](0034-serve-dashboards-with-metabase-through-cube.md) | Accepted |
| 0035 | [Send data to operational tools with dlt](0035-send-data-to-operational-tools-with-dlt.md) | Accepted |
| 0036 | [Upgrade clients one at a time on pinned toolkit versions](0036-upgrade-clients-one-at-a-time-on-pinned-toolkit-versions.md) | Accepted |
| 0037 | [Build marts in place in the serving warehouse](0037-build-marts-in-place-in-the-serving-warehouse.md) | Accepted |

## Convention

- Allocate the next four-digit sequence number when adding a record. Numbers
  represent creation chronology; never reuse or renumber them.
- Name files `NNNN-present-tense-imperative-title.md`.
- Use the Nygard sections: Status, Context, Decision, and Consequences. Include
  the date and keep one independently changeable decision per record.
- Use `Proposed`, `Accepted`, `Rejected`, `Deprecated`, or `Superseded by`
  followed by a link to the new record as the status.
- Preserve an accepted record's original rationale. To change its decision,
  create a new record, link to the old one, and update the old record's status
  and this index. Use a dated addendum for clarifications.
- Use the [repository ADR skill](../../.agents/skills/architecture-decision-record-skill/SKILL.md)
  when writing records.
