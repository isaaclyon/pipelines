# Transformations

Owns the dbt project, its models and tests, and the transformation Dagster code
location. This independent Python project owns its `src/`, `tests/`, dependency
declarations and uv lockfile. The dbt project ships inside the wheel and the
runtime image.

```sh
./scripts/platform local check transformations
./scripts/platform local dg transformations list defs
./scripts/platform local dbt parse --quiet
./scripts/platform local dbt build
```

Run commands from the repository root. The package check runs strict typing,
Ruff, pytest, dependency and import-boundary probes, Dagster definition checks,
`dbt parse`, the [model checks](#model-checks) and SQLFluff.

## What the project contains

The project holds raw sources and staging models only. Staging models are views,
one per source entity: renamed, cast and lightly normalized raw data. They hold
no business rules.

Intermediate models and marts are added in
[Roadmap](../../ROADMAP.md) Phase 4, as each business question needs them. dbt
builds marts in place in the serving schema; tests run in the same `dbt build`,
and a failing test stops the models downstream of it
([ADR 0037](../../docs/adrs/0037-build-marts-in-place-in-the-serving-warehouse.md)).
Nothing in Dagster builds dbt models yet.

| Path | Contents |
| --- | --- |
| `dbt/models/staging/<source>/` | Source YAML (`_<source>__sources.yml`), model YAML and one `stg_<source>__<entity>` view per entity |
| `dbt/tests/` | Singular data tests on staging models |
| `dbt/tests/generic/` | `unique_combination` and `composite_relationship` generic tests |
| `dbt/macros/` | Schema naming, UTC and reporting-date conversion, raw-file attachment, Shopify line columns and output type validation |
| `src/analytics_transformations/` | Model checks and the Dagster definitions |
| `lineage/` | Separately locked environment for the dbt-colibri lineage page |

### Staging models by source

| Source | Models | Enabled by |
| --- | --- | --- |
| Shopify orders | `stg_shopify_orders__*`: orders, lines, tags, discount codes, agreements and sales, taxes, discounts, shipping lines, transactions, refunds and their children, ingestion batches | Always |
| Shopify customers | `stg_shopify_customers__*`: customers, addresses, tags, metafields, tax exemptions, company contacts, field and profile access evidence | `SHOPIFY_CUSTOMERS_ENABLED=true` |
| Loop Returns | `stg_loop_returns__headers`, `stg_loop_returns__snapshots` | `LOOP_RETURNS_ENABLED=true`; otherwise both models are empty |
| GoodDay inventory | `stg_goodday__inventory_captures`, `stg_goodday__inventory_items` | `GOODDAY_INVENTORY_ENABLED=true` |

Staging keeps source identities, exact decimal money with its currency, UTC
instants and evidence columns such as extraction revision and load identity.
`stg_shopify_orders__orders.order_date` is the calendar date of `ordered_at` in
the reporting time zone. When Shopify has no processing time, it falls back to
the creation time; the warning-level `shopify_orders_order_date_fallback` test
counts those orders.

Shopify staging casts quantities, positions and indexes to their declared
`bigint` types, so DuckDB and Snowflake resolve the same type. Line columns added
by extraction revision 4 can be absent before the first expanded load; the
`shopify_line_fulfillment_columns` macro then emits typed nulls. Loop amounts keep
their own currency label. GoodDay captures join only successful dlt loads.

## Settings

Each value is read from the environment. Verify a value in `dbt/profiles.yml`,
`dbt/dbt_project.yml`, the source YAML or `src/` before relying on it.

| Setting | Read by | Meaning |
| --- | --- | --- |
| `DBT_NAMESPACE` | Schema macro, required | Prefix for every model schema; see [naming](#layer-dependency-check). `scripts/platform local dbt` defaults it to `local` |
| `DBT_TARGET` | Profile (default `local`) | `local` (DuckDB), `snowflake-development` or `production` |
| `DUCKDB_PATH` | Profile | Working DuckDB file (default `/var/lib/analytics/working/warehouse.duckdb`) |
| `DBT_RAW_ROOT` | Raw-file macros and source YAML | Folder of per-source raw DuckDB files; see [raw files](#raw-files-on-the-local-stack-adr-0029) |
| `ANALYTICS_REPORTING_TIME_ZONE` | `reporting_time_zone()` in `macros/time.sql` and `order_date` | IANA zone for reporting dates (default `America/Los_Angeles`); Cube reads the same value |
| `SHOPIFY_CUSTOMERS_ENABLED` | Project, sources and Dagster checks | Enables customer sources, models and freshness checks (default `false`) |
| `GOODDAY_INVENTORY_ENABLED` | Project and sources | Enables GoodDay sources and models (default `false`) |
| `LOOP_RETURNS_ENABLED` | Loop staging SQL | Fills Loop staging models (default `false`) |
| `SHOPIFY_ORDERS_DATASET`, `SHOPIFY_CUSTOMERS_DATASET`, `LOOP_RETURNS_DATASET`, `GOODDAY_DATASET` | Source YAML | Raw schema for each source (defaults `SHOPIFY_ORDERS_RAW`, `SHOPIFY_CUSTOMERS_RAW`, `LOOP_RETURN_HEADERS_RAW`, `raw_goodday`) |
| `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PRIVATE_KEY_PATH`, `SNOWFLAKE_ROLE`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_SCHEMA` | Profile | Snowflake connection for the Snowflake targets. They default to empty so dbt can render the local profile without credentials |
| `DBT_STATEMENT_TIMEOUT_SECONDS` | Profile | Snowflake statement timeout (default `0`, no limit) |
| `ANALYTICS_ENVIRONMENT` | `dbt_docs` job | Namespace the docs site documents |

Snowflake sessions use UTC. Every target runs one thread.

### Raw files on the local stack (ADR 0029)

Each loader writes its own raw DuckDB file under the ingestion state volume.
The local Compose stack mounts that volume read-only at
`/var/lib/analytics/ingestion` on the transformations service and its run
containers, and sets `DBT_RAW_ROOT` to it. CI leaves both out.

With `DBT_RAW_ROOT` set on the DuckDB target, a source that names a
`meta.raw_file` reads it as its own database, and an `on-run-start` hook
(`macros/raw_files.sql`) attaches each existing file read-only:

| Source | Database | File under `DBT_RAW_ROOT` |
| --- | --- | --- |
| `shopify_orders` | `raw_shopify_orders` | `shopify_orders/local/shopify_orders.duckdb` |
| `goodday` | `raw_goodday` | `goodday/local/goodday.duckdb` |

File names differ from the raw schema names on purpose: a DuckDB file's
catalog takes the file's name, and a catalog named like its schema makes the
loader's unqualified schema references ambiguous.

A missing file is never created. A run whose selection reads that source,
directly or through upstream models, fails before any model runs with
`Raw file for source <name> is missing: <path>`; other selections still run.
Commands without hooks, such as `dbt show`, do not attach the files. Staging
views in `warehouse.duckdb` name the attached databases, so read them through
dbt or attach the same files.

The Shopify orders loader lowercases its schema on DuckDB, so the source does
too when `DBT_RAW_ROOT` is set. dlt creates a nested table only once a loaded
order needs it, and a short live window can lack one, such as refund duties.
Order child sources are read through `sparse_source`, which turns a missing
table into no rows with the declared columns on this target only.
Without `DBT_RAW_ROOT`, and on Snowflake targets, sources read the target
database. The Shopify customers and Loop Returns sources name no raw file yet.

See [dbt transformations](../../docs/adrs/0013-transform-data-with-dbt-core.md)
and [local/shared scope](../../docs/adrs/0028-develop-locally-before-shared-snowflake-release.md).

## Model checks

`analytics_transformations.manifest_checks` runs every parsed-manifest check as
one gate and stops at the first failure: layer and naming rules, model
declarations, documentation, column declarations and PII. The package check runs
it on the default manifest and on a manifest parsed with
`SHOPIFY_CUSTOMERS_ENABLED=true`, so disabled customer models stay checked.

```sh
DBT_NAMESPACE=check python -m analytics_transformations.manifest_checks dbt/target/manifest.json
```

### Model declarations

`analytics_transformations.model_declarations` requires every model, in every
layer, to declare `config.meta.owner`, `grain`, and either a `key` or a
`no_unique_key_reason`. Key columns need blocking `not_null` tests. A one-column
key needs a blocking `unique` test; a composite key needs a blocking
`unique_combination` test over exactly the key columns.

Every data test uses native dbt severity: `error` fails the build and `warn`
does not. The project sets `error` as the default. A severity other than `error`
or `warn` fails the check.

It also requires a description on every model, source and source table, and one
`meta.schema_description` per model schema, set beside `+schema` in
`dbt_project.yml`.

### Column declarations

`analytics_transformations.column_declarations` requires every model output
column to be declared with a description. Types are optional except where
correctness depends on them: names ending in `_amount`, `_count` or `_quantity`,
and columns with `currency` or `temporal` metadata. Amounts need exact decimals,
counts need integers and quantities need a numeric type.

Models under `models/marts/` must declare every type, expose each amount's
currency in a local column, give every date and timestamp temporal meaning, and
declare identity meaning for `id` and `*_id` columns. Source identities are
source-qualified, such as `shopify_order_id`.

```yaml
- name: shopify_order_id
  data_type: varchar
  config:
    meta:
      identity: {kind: source, source: shopify}
- name: total_shop_amount
  data_type: decimal(38, 9)
  config:
    meta:
      currency: {column: shop_currency_code}
- name: ordered_at
  data_type: timestamp
  config:
    meta:
      temporal: {kind: instant, timezone: UTC}
- name: order_date
  data_type: date
  config:
    meta:
      temporal: {kind: derived_date, instant: ordered_at, timezone: America/Los_Angeles}
```

The currency field must name a declared text column. Staging and intermediate
amounts may instead name a parent model, join keys, currency column and blocking
correctness test, as in
`currency: {model: stg_shopify_orders__orders, join: {shop_id: shop_id, order_id: order_id}, column: shop_currency_code, test: shopify_orders_currencies_match}`.
The test must depend on both models. Source calendar dates use
`temporal: {kind: source_date}`. Derived dates name a declared instant and a
valid zone; an optional `temporal.test` names a blocking test that checks the
conversion.

`validate_model_outputs` (`macros/validate_model_outputs.sql`) compares each
built model's actual columns with its declarations, and compares declared types,
including decimal precision and scale. The package tests run it after each
fixture build.

### PII declarations

Every declared model, source, seed and snapshot column needs a boolean
`config.meta.pii`, including disabled definitions. There are no exceptions and no
default.

```yaml
- name: email_address
  config:
    meta: {pii: true}
- name: customer_count
  config:
    meta: {pii: false}
```

`src/analytics_transformations/source_fields.json` is the reviewed inventory of
source columns. The check fails when a declared source column is missing from
the inventory, or when an inventoried column is no longer declared. Remove an
entry only when ingestion and every consumer stop using the field. To propose
inventory changes after adding source declarations, run from
`packages/transformations/` in the package environment:

```sh
python -m analytics_transformations.pii dbt/target/manifest.json > /tmp/source-fields.proposed.json
```

The command prints a proposal only. Review it, then apply approved changes by
hand. It does not classify fields. See
[the platform policy](../../engineering-standards.md#personal-data-classification-and-cube-exposure).

### Layer dependency check

`analytics_transformations.layer_dependencies` fails on any violation of
ADR 0022's direction:

| Resource | Must live in | May read |
| --- | --- | --- |
| `stg_<source>__<entity>` model | `models/staging/` | Sources, seeds, snapshots, package models, and staging models of the same source |
| `int_<entity>__<purpose>` model | `models/intermediate/` | Staging and intermediate models |
| `fct_<entity>` or `dim_<entity>` model | `models/marts/` | Staging, intermediate, and mart models |
| Snapshot | Snapshot path | Sources |
| Exposure or semantic model | Any | Mart models |

A model's folder and name prefix must name the same layer, and a model outside
the three layer folders fails. Data tests, unit tests, analyses and metrics may
read anything. Only this project's resources are checked; models from installed
dbt packages count as staging inputs, like sources. There are no per-model
exceptions.

Descriptive naming rules are review guidance. The checker keeps folder and
prefix agreement and the staging source segment it needs for layer checks.

Persisted identifiers have a portable 255-character limit and cannot collide
case-insensitively within a schema. Models use `<namespace>_staging`,
`<namespace>_intermediate` and `<namespace>_marts`; seeds, snapshots and stored
test failures use `_seeds`, `_snapshots` and `_test_failures`. The schema macro
rejects any other custom schema and requires a lowercase snake_case namespace.
Raw schemas belong to ingestion.

The check also scans model and snapshot SQL with a pattern check, not a SQL
parser. Each FROM or JOIN target must be one of:

- `{{ ref(...) }}`, `{{ source(...) }}`, `{{ sparse_source(...) }}` or `{{ this }}`;
- a CTE name defined anywhere in the query;
- a subquery;
- a row-generating table function: `unnest`, `flatten` (also inside
  `table(...)`), `range`, `generate_series`, `split_to_table`, or `generator`.

Anything else fails, including schema-qualified or unknown names, other Jinja
expressions, functions that read storage such as `read_parquet(...)`, quoted
file paths and Snowflake stages. SQL produced inside macros is not scanned, and
CTE scoping is not tracked; review covers those cases. This scanner guards
dependencies; it is not a security boundary.

The fixture projects in `tests/fixtures/layer_dependencies/` show permitted and
rejected graphs.

## Tests

The ordinary suite is offline and uses DuckDB. `tests/model_build.py` builds a
project in a disposable DuckDB file through the same gates: `dbt parse`, the
model checks, `dbt build` with every data and unit test, and
`validate_model_outputs`. `tests/fixtures/sample/` is a small project that
exercises those gates, including a mart.

`tests/project_build.py` builds the real project once per test session over one
synthetic dataset, with Shopify customers enabled. Tests read that build, or edit
a copy and run only the check that guards the change. A test that executes real
dbt project nodes declares them with `pytest.mark.dbt`, and the check skips tests
a change cannot affect
([ADR 0026](../../docs/adrs/0026-scale-dbt-checks-with-the-change.md)).

`tests/test_naming_adapters.py` also runs on Snowflake when
`NAMING_LIVE_ACCEPTANCE=development`, `ANALYTICS_ENVIRONMENT=development` and the
Snowflake connection settings are set. It creates and removes only its own
disposable schemas. Ordinary checks never connect to Snowflake.

## Raw Shopify validation

`shopify_raw_validation` runs the `refresh_freshness` asset check on
`raw/shopify/shopify_orders_raw`. The check runs `dbt source freshness` on
`source:shopify_orders.ingestion_batches`, defined in
`dbt/models/staging/shopify_orders/_shopify_orders__sources.yml`. The policy
lives in that file: the latest `window_end` of a complete refresh warns after
25 minutes and errors after 30. Lookback and reconcile batches, and refreshes
that finished incomplete, do not count.

`shopify_raw_validation_schedule` runs the check every five minutes and skips
while a validation run is queued or running. It ships stopped. A missing
dataset, failed query or dbt timeout reports an error, never a pass. Results
carry `status` and `age_seconds` metadata. Raw freshness alone does not show
that downstream models are fresh or complete.

`stg_shopify_orders__ingestion_batches` keeps the window evidence and marks
verified windows explicitly. Verification requires complete status and
pagination, zero rejects, a known nonnegative order count, revision-3-or-later
query lineage, and valid window chronology. A successful zero-order extraction
still counts. A missing evidence table yields no verified coverage; a malformed
table fails.

## Shopify customer freshness

With `SHOPIFY_CUSTOMERS_ENABLED=true`, the `shopify_customers_validation` job runs
two asset checks on `raw/shopify/shopify_customers_raw`:

| Check | Source | Warns after | Errors after |
| --- | --- | --- | --- |
| `customer_profile_freshness` | `shopify_customers.profile_evidence` | 25 minutes | 30 minutes |
| `customer_sweep_freshness` | `shopify_customers.sweep_evidence` | 26 hours | 30 hours |

Only complete, zero-reject incremental windows count toward profile freshness;
empty successful windows count. Shopify summaries can change without advancing
`updatedAt`, so their coverage comes from full sweeps. Per-field-group access
statuses distinguish a real null from `access_denied`, `unsupported`, `excluded`
or `unknown`. `shopify_customers_validation_schedule` runs both checks every five
minutes, skips overlapping runs and ships stopped.

### Customer activation

1. Set `SHOPIFY_CUSTOMERS_DATASET` and the customer seed, then run the first
   ingestion sweep. See the [ingestion guide](../ingestion/README.md).
2. Measure the full sweep and refresh durations, including scheduler wait. The
   sweep's blocking time plus the refresh cadence and runtime must fit the
   30-minute profile freshness limit.
3. Check the access evidence in `stg_shopify_customers__field_access`.
4. Set `SHOPIFY_CUSTOMERS_ENABLED=true` for the transformations code location
   and its run containers. The Compose files do not set it; add it in your
   environment override.
5. Run the customer validation job, then start the refresh, sweep,
   reconciliation and validation schedules in Dagster.

## dbt documentation site

The `dbt_docs` job runs `dbt docs generate --static` and writes the single
self-contained page to `/var/lib/analytics/docs/index.html`, the Compose `docs`
volume that the [docs site](../../deploy/compose/README.md#dbt-documentation-site)
serves. It then runs [dbt-colibri](https://github.com/b-ned/dbt-colibri) on the
same manifest and catalog and writes a column lineage page to
`lineage/index.html`. Colibri traces columns by parsing each model's compiled
SQL, so a trace can be wrong for SQL it cannot resolve.

The job documents the namespace named by `ANALYTICS_ENVIRONMENT`, so it reads
the `<environment>_staging` and `<environment>_marts` schemas. It refuses to
start without a known environment. The catalog has column types and table sizes
only for relations built in that namespace. For the lineage page, the job fills
the columns of unbuilt models from their declarations. The page holds no table
rows. The job turns off dbt's and Colibri's usage statistics
(`DO_NOT_TRACK=1`), and builds, loads and grants nothing. It stages both pages
beside their targets and renames them into place only after both build, so a
failed build keeps the previous site.

`dbt_docs_release_sensor` requests one build per transformations image, keyed
by `DAGSTER_CURRENT_IMAGE`, so each release rebuilds the site within five
minutes. The sensor ships stopped. Launch `dbt_docs` by hand to refresh table
sizes between releases.

Colibri uses the separately locked `lineage/` environment because its SQL parser
conflicts with `dagster-dbt`. Images install it at `/opt/lineage` and set
`COLIBRI_EXECUTABLE=/opt/lineage/bin/colibri`. Worktree setup also syncs
`lineage/.venv`; to prepare it by hand, run
`uv sync --project packages/transformations/lineage --frozen` from the repository
root.
