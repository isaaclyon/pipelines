# Transformations

Owns the dbt project, transformation models and tests, and the transformation
Dagster code location. This independent Python project owns its `src/`,
`tests/`, dependency declarations, and uv lockfile.

```sh
./scripts/platform local check transformations
./scripts/platform local dg transformations list defs
./scripts/platform local dbt parse --quiet
```

Run commands from the repository root. Checks exercise the installed package,
strict typing, dependency and import boundaries, Dagster definitions, dbt
parsing, and SQLFluff. The dbt project is included in the wheel and runtime image.
Dagster/dbt asset mapping for business models is not wired yet.

[ADR 0022](../../docs/adrs/0022-publish-marts-through-forward-only-transformation-layers.md)
defines forward layer dependencies and detailed marts as the supported consumer
interface. The [testing baseline](../../engineering-standards.md#transformation-contracts-and-tests)
distinguishes universal model contracts from conditional data assertions.
The publication infrastructure below implements the consumer boundary.


## Shopify order models
| Layer | Model | Grain |
| --- | --- | --- |
| Staging | `stg_shopify_orders__orders` | One row per `(shop_id, order_id)`; adds the reporting-zone `order_date` |
| Staging | `stg_shopify_orders__order_lines` | One row per `(shop_id, order_id, line_item_id)` |
| Staging | `stg_shopify_orders__transactions` | One row per `(shop_id, order_id, transaction_id)` |
| Staging | `stg_shopify_orders__refunds` | One row per `(shop_id, order_id, refund_id)` |
| Staging | `stg_shopify_orders__refund_transactions` | One row per `(shop_id, order_id, refund_id, transaction_id)` |
| Intermediate | `int_shopify_orders__line_rollups` | One row per order with lines: count, quantity and exact decimal line value |
| Intermediate | `int_shopify_orders__classifications` | One row per order: channel and order-characteristic flags with their rule evidence |
| Mart | `fct_orders` | One row per `(shopify_shop_id, shopify_order_id)`; orders without lines get zero rollups |
| Mart | `dim_order_contacts` | One row per `(shopify_shop_id, shopify_order_id)`: email, phone and addresses (personal data) |
| Mart | `fct_order_lines` | One row per `(shopify_shop_id, shopify_order_id, shopify_line_item_id)`; source line detail and parent reporting context |

Intermediate models are views by default. The commercial ledger, commercial
components, net units, allocations and payment reconciliation models are
tables, set in `dbt_project.yml`. On Snowflake, creating the stacked views for
them never finished, and a view that reads the payment reconciliation checks
fails to compile. The reconciliation proof uses joins for the same reason:
Snowflake cannot evaluate nested correlated subqueries.

`int_shopify_transactions__payment_flows` keeps transaction identity and timing
alongside the shared cash-flow classification. It identifies both movements of
proven failed-attempt reversal pairs without removing them from gross payment
reconciliation. Refund-object linkage takes precedence over reversal proof.
The flow classification itself is not a full-payment verdict.
`int_shopify_orders__payment_history` checks parent observation lineage and exact
transaction counts, reconciles dated USD agreement totals to the original and
current headers, and evaluates net cash against the obligation at each logical
event. Header reconciliation subtracts only the validated ledger handling fees;
the payment timeline retains those fees in the obligation. An arbitrary refund
adjustment or an off-ledger Loop fee is not subtracted from the header. Linked refund cash and its credit are applied together. Full-payment
proof survives refunds but resets at a later order edit. Verified credits can
reduce the remaining obligation before full payment. Unsupported RETURN
agreements, missing evidence and mismatches stay unresolved; current financial
status never substitutes for this proof.

`int_shopify_orders__revenue_eligibility` applies revenue-specific exclusions and
preserves all exclusion and unresolved reason identifiers. Test orders,
gift-card-only orders, and proven unpaid obligations are excluded. Unknown
required evidence stays null unless another proven exclusion applies. Published
`fct_orders` carries these fields, net customer payment and refund flags.
`shopify_published_revenue_eligibility` blocks contradictory published verdicts
and drift from the owning model. AOV eligibility is not implemented.

`fct_orders.reporting_market` is CA for Canada shipping destinations and US for
all others, including missing country; actual geography remains unchanged.
`is_draft_order` uses the exact `shopify_draft_order` source marker and retains
null when source name is missing. These dimensions support scope selection;
they are not universal revenue exclusions.

Shopify staging casts line and sale quantities, refund quantities, tax and duty
positions, and discount positions and indexes to their declared `bigint` outputs. This normalizes dlt's Snowflake `NUMBER(19,0)` storage to dbt's
`NUMBER(38,0)` representation; DuckDB uses its native `BIGINT`. Raw fixtures
retain the source precision so publication checks catch type drift.

Contacts sit in their own mart so contact edits and access rules stay separate
from order facts; Cube joins the two one-to-one (see `packages/metrics`).
Blocking tests reject duplicate orders or lines, a non-USD shop currency, and
line currencies that differ from their order. `tests/test_shopify_orders_marts.py`
reads the shared synthetic build and verifies each bad-data case fails the model
gates on an edited copy. Snowflake acceptance verifies publication preservation.

`fct_order_lines` retains repeated product lines, nullable SKUs/product IDs,
gift-card items, and lines belonging to test or cancelled orders. It exposes
`is_gift_card_item` (the line sells a gift card, not gift-card payment tender),
source ordered/current quantities, original unit prices, source discounted totals,
and original line values in both currencies. Original line value is ordered
quantity times original unit price at `DECIMAL(38, 9)` precision; it is not
governed gross sales or net sales. Source discounted totals omit order-level
discounts, and source current quantity alone does not establish net ordered quantity
or physically kept quantity. Source product attributes describe the latest
observed order line, without a current catalog lookup or an immutable snapshot claim.

The line mart carries parent dates, customer identity, statuses, currency codes,
and billing/shipping geography. Direct contact names, email, phone and street
addresses remain in `dim_order_contacts`; order monetary totals remain in
`fct_orders`. Blocking checks compare line fields to staging and reconcile each
shop/order's line count, gross ordered quantity and original line value in both
currencies to `fct_orders`, including zero-line orders. Fixtures also prove that
equal and opposite per-order errors cannot pass by preserving global totals.

### Order classification
`int_shopify_orders__classifications` applies the rules in the
[order marts spec](../../docs/specs/20261011-shopify-order-marts.md) to staged
source name, tags and discount codes. `fct_orders` publishes the resulting
flags. Retail, specialty and bulk are exclusive commercial channels in that
precedence order. D2C is the residual channel: it is true only when every
commercial rule was evaluated and none matched. Employee, gifted, replacement,
exchange, Shopify Collective and broadcast flags can overlap with any channel.

The flags are nullable. Null means that the evidence is unavailable, for example
an order last observed before extraction revision 2 selected tags, discount
codes and email (`has_classification_evidence = false`). A POS order is still
retail without that evidence. Gifting matches an approved tag or a non-test,
non-replacement employee order whose entire original total after discounts,
including shipping and tax, is zero. The fallback uses staged
`original_total_price_shop_amount` (`originalTotalPriceSet`), never the current
total after edits or refunds. Missing or negative original amounts remain
unknown unless another condition resolves the rule; a known nonemployee, test,
replacement or positive original total rules out the inference. Explicit gifting
tags still win. Existing currency checks validate the amount's currency.
Draft-order exclusion remains open. Unit tests cover each rule; the
blocking `fct_orders_channels_consistent` test checks both channel consistency
and gifting against the published evidence. A corrupt gifting verdict cannot
replace a valid publication.

Validated commercial money/net units and discount, shipping and refund
allocations are separate work, as are Cube product/SKU line reporting and
governed measures. The existing Cube `orders` view does not expose
the line mart. Current catalog enrichment remains separate work.

### Net ordered quantity
`int_shopify_order_lines__net_units` uses source `current_quantity` once. For
cancelled lines with remaining units, it subtracts the source unfulfilled
quantity. It does not subtract refund quantities again or zero all cancelled
lines. Invalid quantity bounds, missing parents, and missing cancellation
evidence produce null quantities with an explicit `net_quantity_status`.
Lines already at zero need no additional cancellation evidence. Noncancelled
lines use their valid current quantity. These fields do not claim physical
return or kept-unit coverage.

`fct_order_lines` publishes the net quantity and its evidence status.
`fct_orders` publishes the summed quantity and `unresolved_quantity_line_count`.
An order with any unresolved line has a null total, not a partial sum. An order
without lines has zero units. The blocking
`shopify_published_net_units_reconcile` test checks each published line and order
against the calculation models. Both reviewed mart-contract snapshots include
the additive nullable fields.

Revision-4 raw line columns can be absent before the first expanded source load.
The staging macro emits typed nulls in that case. It also preserves nulls in old
rows after schema expansion. New extraction and historical re-extraction are
separate operations; these model changes do not perform them.

### Payment evidence staging
Payment staging reads the existing raw transaction, refund and refund-transaction
link tables. It preserves every row, including authorizations, failed/pending
movements, test transactions and unknown enum values. Transaction `kind` and
`status` use trimmed uppercase values. Instants use UTC timestamps; money keeps
native `DECIMAL(38, 9)` precision and each record's shop/presentment currencies.
Refund totals do not prove successful settlement. Links carry identities, not
additional amounts. Refund free-text notes are not exposed.

Payment-history proof remains unresolved while a refund is `PENDING` or
`AWAITING_RESPONSE`. Such refunds are not successful cash movements, but their
agreement credits cannot be treated as verified noncash credits to establish
full payment. Settlement must resolve before the history can prove eligibility.

Blocking key checks reject duplicate or null identities. The raw-fixture tests
in `tests/test_shopify_payment_staging.py` check normalization, nullable evidence,
precision and duplicate rejection. Child tables have no independent observation
timestamp; later evidence models must use the parent order's observation.

The commercial component and payment reconciliation models build on these
staging models and feed `fct_orders` and `fct_order_lines`. The staging models
themselves do not calculate eligibility, payment completeness or refund
classification. See the
[evidence contract](../../docs/specs/20261018-order-payment-eligibility.md)
for the commercial mapping and classification dependencies.

### Average eligibility foundation
The order mart adds nullable `is_aov_eligible`, `aov_exclusion_reasons` and
`aov_unresolved_requirements`. The line mart retains that parent context and
the parent revenue verdict/reasons, and adds its own `is_asp_eligible`,
`asp_exclusion_reasons` and `asp_unresolved_requirements`. Parent AOV flags on
lines are context, not permission to count repeated orders or calculate AOV
at line grain. No order monetary totals are copied to lines.

The intermediate average-eligibility model reuses revenue's verified payment
and commercial prerequisites, then applies the separate recorded average rules.
Both averages exclude cancelled, gifted, replacement and exchange orders.
AOV additionally excludes full monetary refunds and nonpositive retained
commercial value, using net revenue without tax or handling fees. ASP instead
requires a non-gift-card line with known net ordered units and resolved
merchandise sales, discounts and refund allocations. A full amount-only refund
does not remove its remaining units from ASP. A zero-unit line with residual
net sales remains eligible; neither positive value nor positive line quantity
is an ASP gate. Employee status and channel membership add no exclusion.

A proven exclusion gives false even when other evidence is unknown. Otherwise,
missing required payment, commercial or classification evidence gives null,
never true. Reasons retain every known exclusion and unresolved requirement,
including on excluded rows. The published-column descriptions list their
pipe-delimited identifiers. The blocking
`shopify_published_average_eligibility` check reconciles both marts to these
decisions and verifies parent context without changing line identity.

This is a schema expansion, not Cube activation. Existing Cube queries do not
read the new columns. Before a later consumer release, an explicitly authorized
orders-group publication must build and validate both expanded marts, pass
compatibility against the **actual previous publication marker**, and make the
new columns available in the configured reader destination. Deployment alone
does not prove publication. Reviewed code snapshots provide offline compatibility
evidence only; they are not a substitute for that marker. Live publication and
Snowflake acceptance require separate authorization. the governed AOV/ASP
aggregations, zero aggregate denominator handling and Cube line interface remain
a later stage; the canceled live acceptance work is not resumed.

## Model checks

### Model declarations

`analytics_transformations.model_declarations` requires every staging and
intermediate model to declare its purpose, owner, grain, and a key (with
blocking not-null and uniqueness tests) or the reason it has none. Every data
test uses native severity: `error` blocks publication and `warn` does not.
There is no duplicate `publication_blocking` declaration. Marts are checked by
the mart contract check below.

It also requires a description on every root-project model, source and source
table, and one `meta.schema_description` per model schema, set beside `+schema`
in `dbt_project.yml`. Every staging, intermediate and mart output column needs
a description too. `scripts/check-python.sh` runs the CLI for the default
manifest and again with `SHOPIFY_CUSTOMERS_ENABLED=true`, so disabled customer
models stay documented.

Every root-project exposure needs a description. An exposure's
`meta.cube_fields` maps each Cube field to `<model>.<column>` references; each
must name a declared column of a model in the exposure's `depends_on`.

The same CLI checks column inventories and supplied correctness metadata
(`analytics_transformations.column_declarations`). Published marts require all
types and their currency, temporal and identity meanings. Internal fields need
names, descriptions and explicit PII decisions, but ordinary types are optional.
Money, counts, quantities and explicit currency/temporal metadata still need types;
supplied types are checked against actual outputs. Internal currency and temporal
metadata are optional: document and test meaning at the transformation that defines
it instead of repeating it through every pass-through model. Existing explicit
associations remain validated. No type or lineage analysis supplies a PII decision.

Use `config.meta` on published columns (descriptions and explicit `pii` decisions
are also required; omitted here to focus on semantic metadata):

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

The currency field must be a declared text column. Internal amounts may instead
name a parent model, join keys, currency column and blocking correctness test:
`currency: {model: stg_shopify_orders__orders, join: {shop_id: shop_id, order_id: order_id}, column: shop_currency_code, test: shopify_orders_currencies_match}`.
The test must depend on both models; key/relationship tests alone do not establish
currency correctness. Published marts expose the currency locally. Tests still
establish the association's actual values; metadata alone cannot prove them.

Source calendar dates use `temporal: {kind: source_date}`. Derived dates name a
declared instant and a valid conversion zone; instants declare UTC regardless of
SQL timestamp storage. An optional `temporal.test` names a blocking correctness
test attached to the model. Existing value fixtures cover Los Angeles midnight,
both DST transitions and missing processing-time fallback.

Column prefixes and suffixes are review conventions, not blocking style rules.
Recognized `_count`, `_quantity` and `_amount` fields still require integer, numeric
and exact-decimal types respectively. Mart `id`/`_id` columns and columns with
explicit identity metadata retain source or analytical meaning, with
source-qualified source IDs. Raw names remain ingestion-owned.

Before publication, `validate_model_outputs` compares actual output names with
all declarations and compares adapter-resolved types for columns that declare
them, including decimal precision/scale. This covers every executed project
model and ephemeral ancestor. Undeclared outputs or mismatched supplied types
reject the private candidate. Internal declarations do not establish stable
consumer contracts.

### Generated review proposals

After parsing, run these commands in the transformation package's Python
environment, from `packages/transformations/`:

```sh
python -m analytics_transformations.mart_contracts dbt/target/manifest.json --snapshot > /tmp/mart-contracts.proposed.json
python -m analytics_transformations.pii dbt/target/manifest.json > /tmp/source-fields.proposed.json
```

Both commands print proposals only. Compare them with `dbt/mart-contracts.json`
and `src/analytics_transformations/source_fields.json` respectively. Review and
apply approved changes; do not redirect generation over a reviewed baseline.
The inventory includes disabled sources and does not classify fields or approve
retirement. Contract compatibility is still checked against the actual previous
publication, not just the committed snapshot. Cube exposures already use
[`scripts/platform local exposures`](../metrics/README.md#dbt-exposures).

### PII declarations
Every declared model and source column now has explicit boolean
`config.meta.pii`. The inventory also includes the existing GoodDay and Loop
headers, so there are **no legacy unclassified declared fields** and no exception
list to expand. New seeds and snapshots have the same requirement. Authors must
inventory columns when adding a source; the checker does not discover warehouse
columns omitted from dbt declarations. The reviewed
`src/analytics_transformations/source_fields.json` inventory prevents removal
of a maintained source column or entire source from silently reducing coverage,
including disabled sources. Generate proposed inventory changes after adding
source declarations, rather than hand-authoring both lists. Remove inventory
entries only for reviewed field retirement after updating ingestion and downstream
consumers; never to excuse missing declarations.

```yaml
- name: email_address
  config:
    meta: {pii: true}
- name: customer_count
  config:
    meta: {pii: false}
```

`analytics_transformations.pii.check_pii` runs from the model-declarations CLI
and before candidate builds in the publication runner. It checks enabled and
disabled definitions, rejects empty column inventories, and accepts only actual
booleans. Removing a classification blocks publication without replacing the
previous output. The ratchet starts at complete coverage of declared fields:
there is no grandfathering or missing-tag default.

Cube-generated exposures carry `cube_pii` decisions for every exposed field and
`cube_direct_columns` for direct aliases (null for expressions and measures).
A direct alias cannot relabel its mart column; derived-field decisions are
independent, not inherited from lineage. The metrics check verifies the generated
file against the real compiled Cube model in both customer configurations.

The initial classification review treats contact names, contact channels,
street lines, coordinates, tax identifiers, customer notes/metafield values,
free-form discount titles and potentially personal free-text order-line labels
as restricted. Per the initial classification review, discount codes (including normalized
codes), order tags and customer tags are outside the restricted category.
Source IDs, coarse geography, status/consent flags, numeric amounts and
independently derived business flags also remain outside the narrow restricted
category. Customer-level data is still linkable: false does not mean
anonymous. The exact decisions live beside each field; initial review approval
must be recorded by the reviewer, not inferred from a passing test.

See [the platform policy](../../engineering-standards.md#personal-data-classification-and-cube-exposure).
No data is removed or reclassified automatically by SQL analysis. The existing
warehouse grants and restricted publication behavior remain unchanged.

### Layer dependency check

The package check runs `analytics_transformations.layer_dependencies` on the
manifest from `dbt parse` and fails on any violation of ADR 0022's direction:

| Resource | Must live in | May read |
| --- | --- | --- |
| `stg_<source>__<entity>` model | `models/staging/` | Sources, seeds, snapshots, package models, and staging models of the same source |
| `int_<entity>__<purpose>` model | `models/intermediate/` | Staging and intermediate models |
| `fct_<entity>` or `dim_<entity>` model | `models/marts/` | Staging, intermediate, and mart models |
| Snapshot | Snapshot path | Sources |
| Exposure or semantic model | Any | Mart models |

A model's folder and name prefix must name the same layer, and a model outside
the three layer folders fails. Ephemeral models follow their folder's layer.
Data tests, unit tests, analyses, and metrics may read anything. There are no
per-model exceptions: move logic to the layer that may read what it needs.
Only this project's resources are checked. Models from installed dbt packages
count as staging inputs, like sources.

Descriptive naming rules are review guidance, not publication gates. The checker
keeps folder/prefix agreement and the staging source segment needed for layer
checks, but does not ban words such as `final`, `new`, `copy` or `_table`, enforce
intermediate-purpose suffixes, or require aliases to repeat logical naming grammar.
Native dbt versions retain their manifest identity check. Published relation names
remain protected by compatibility checks regardless of descriptive style.

Persisted identifiers have a portable 255-character ceiling and cannot collide
case-insensitively within a database/schema. With `DBT_NAMESPACE` selected, models
must use `<namespace>_staging`, `_intermediate` and `_marts`; seeds, snapshots and
stored failures use `_seeds`, `_snapshots` and `_test_failures`. The schema macro
routes auxiliary resources to these dedicated schemas and rejects other custom
schemas. The `analytics` shared namespace and separate developer/CI namespaces
are supported. Raw schemas remain ingestion-owned. Review covers pluralization,
familiar business terms and whether a name describes its data accurately.

CI runs these checks after parsing; publication repeats layer, identifier and
column checks before building a candidate, then validates its actual outputs
before promotion.

The check also scans model and snapshot SQL with a pattern check (not a SQL
parser). Each FROM or JOIN target must be one of:

- `{{ ref(...) }}`, `{{ source(...) }}`, or `{{ this }}`;
- a CTE name defined anywhere in the query;
- a subquery;
- a row-generating table function: `unnest`, `flatten` (also inside
  `table(...)`), `range`, `generate_series`, `split_to_table`, or `generator`.

Anything else fails, including schema-qualified or unknown names, other Jinja
expressions such as `{{ var('orders_table') }}` or `{{ target.schema }}.orders`,
functions that read storage such as `identifier('raw.orders')`, `table('...')`,
and `read_parquet(...)`, quoted file paths, and Snowflake stages (`@stage`).
Add a function to `ROW_FUNCTIONS` only when it produces rows without naming
stored data. SQL produced inside macros is not scanned, CTE scoping is not
tracked, and the first relation inside a parenthesized join is not checked;
review covers those cases. This scanner is a narrow dependency guard, not a
security boundary or a substitute for warehouse privileges.

```sh
./scripts/platform local check transformations
```

The fixture projects in `tests/fixtures/layer_dependencies/` show permitted and
rejected graphs.

## Raw Shopify validation

`shopify_raw_validation` runs the `refresh_freshness` asset check on
`raw/shopify/shopify_orders_raw`. The check runs `dbt source freshness` on
`source:shopify_orders.ingestion_batches`, defined in
`dbt/models/staging/shopify_orders/_shopify_orders__sources.yml`. The policy
lives in that file: the latest `window_end` of a complete refresh warns after
25 minutes and errors after 30. Lookback and reconcile batches, and refreshes
that finished incomplete, do not count.

The schedule ships stopped. When enabled it checks every five minutes, all day,
alongside the development refresh (`*/15 * * * *`).
This covers the morning reporting deadline. Raw freshness alone does not
establish published-answer freshness or completeness. Deployments preserve the
enabled state.

`stg_shopify_orders__ingestion_batches` retains source-window evidence and marks
verified updated-at windows explicitly. Verification requires complete status and
pagination, zero rejects, a known nonnegative order count, revision-3-or-later
query lineage, and valid window/observation chronology. A successful zero-order
extraction still counts. A missing evidence table yields no verified coverage;
a malformed existing table fails rather than fabricating evidence.
`int_shopify_orders__window_coverage` merges only connected verified intervals
per shop, including nested and adjacent windows. A recent successful load cannot
hide a gap; a successful retry can fill it. `fct_data_availability` captures the latest connected interval with each
publication. Updated-at coverage alone does not prove an initial order-date
population, deleted-order coverage, or an end-to-end freshness guarantee.
The same mart captures the latest Loop snapshot endpoint and its independent
load/count verdict from `stg_loop_returns__snapshots`, including empty snapshots.
It emits one row per known shop, or one explicit unknown row before discovery.
The Cube status view reads this published evidence, never live raw tables.

dbt reads `SHOPIFY_ORDERS_DATASET`, which the Compose run context passes, using
the transformation service's Snowflake identity. Local and CI tests build a
DuckDB `ingestion_batches` fixture. A missing dataset, failed query or dbt
timeout reports an error, never a pass. Results carry `status` and
`age_seconds` metadata.

### Profiles and local databases

`dbt/profiles.yml` supports `local`, `snowflake-development`, and `production`.
The remote Compose configuration requires account, identity, role, database,
warehouse, isolated schema, and key-path values. Unused Snowflake outputs have
empty defaults so dbt can render the local profile without live credentials.

DuckDB currently supplies deterministic tests; the accepted
[local-first design](../../docs/specs/20261101-local-first-development.md) extends
it to single-user development serving with separate analytical-only files.
That copy/export/stop-validate-restart lifecycle is not implemented yet. Each
worktree must own its writable data/state; copy only after readers/writers stop,
checkpoint and close. No hot copies or raw/restricted files mounted into Cube.
dbt creates a working file when needed; independent fixtures own disposable files.
`tests/model_build.py` runs the shared candidate validation directly in those files,
without locks, export, file replacement or permission emulation.
`tests/project_build.py` builds the whole project once per test process over one
synthetic dataset. Tests read that build, or edit a copy and run only the check
that guards the change, instead of building the project themselves
([ADR 0026](../../docs/adrs/0026-scale-dbt-checks-with-the-change.md)). Development
currently publishes through Snowflake after successful writes to enabled inputs;
see the next section. This remains the shared-release path, not a prerequisite
for every local experiment. Shared activation still requires executed Snowflake
parity, actual-predecessor compatibility and permission checks.

#### Raw files on the local stack (ADR 0029)

Each loader writes its own raw DuckDB file under the ingestion state volume.
The local Compose stack mounts that volume read-only at
`/var/lib/analytics/ingestion` on the transformations service and its run
containers, and sets `DBT_RAW_ROOT` to it. CI leaves both out.

With `DBT_RAW_ROOT` set on the DuckDB target, a source that names a
`meta.raw_file` reads it as its own database, and an `on-run-start` hook
(`macros/raw_files.sql`) attaches each existing file read-only:

| Source | Database | File under `DBT_RAW_ROOT` |
| --- | --- | --- |
| `shopify_orders` | `raw_shopify_orders` | `shopify_orders/local/raw_shopify_orders.duckdb` |
| `goodday` | `raw_goodday` | `goodday/local/goodday.duckdb` |

File names differ from the raw schema names on purpose: a DuckDB file's
catalog takes the file's name, and a catalog named like its schema makes the
loader's unqualified schema references ambiguous.

A missing file is never created. A run whose selection reads that source,
directly or through upstream models, fails before any model runs with
`Raw file for source <name> is missing: <path>`; other selections still run.
Commands without hooks, such as `dbt show`, do not attach the files.
Staging views in `warehouse.duckdb` name the attached databases, so read them
through dbt or attach the same files; marts are tables and need no raw file.
The Shopify orders loader lowercases its schema on DuckDB, so the source does
too when `DBT_RAW_ROOT` is set. dlt creates a nested table only once a loaded
order needs it, and a short live window can lack one, such as refund duties.
Order child sources are read through `sparse_source`, which turns a missing
table into no rows with the declared columns on this target only.
Without `DBT_RAW_ROOT`, and on Snowflake targets, sources read the target
database exactly as before. Loop Returns and Shopify customers do not have raw
files yet.

See [dbt transformations](../../docs/adrs/0013-transform-data-with-dbt-core.md)
and [local/shared scope](../../docs/adrs/0028-develop-locally-before-shared-snowflake-release.md).

## Development publication
Each enabled consistency group exposes its dbt models, dependencies and supported
data tests through `dagster-dbt`, plus its publication asset. A job always builds
and validates the whole group; individual models or checks cannot be retried as
partial publications. Orders and availability remain together, while customer
profiles and inventory publish independently. Shared ancestor models use
`<group>/<model>` Dagster keys because each group builds its own candidate copy;
disjoint models keep their existing keys. Checks and execution events use the
same keys, and raw source keys still link to their ingestion assets.

`defs/development_publication.py` declares each input's group, raw assets
(including source-status evidence), successful writer jobs and enablement flag.
These entries generate dependencies, source validation and sensor selectors:

| Input | Group | Enablement | Successful runs that request publication |
| --- | --- | --- | --- |
| Shopify orders | `orders` | Always | `shopify_orders_dlt_load`, `shopify_orders_reconcile_counts` |
| Loop headers | `orders` | `LOOP_RETURNS_ENABLED=true` | `loop_returns_dlt_snapshot` |
| Shopify customers | `customers` | `SHOPIFY_CUSTOMERS_ENABLED=true` | `shopify_customers_dlt_load`, `shopify_customers_reconcile` |
| GoodDay inventory | `inventory` | `GOODDAY_INVENTORY_ENABLED=true` | `goodday_inventory_capture` |

| Group | Job / publication asset | Schema setting | `analytics/publication-input` value |
| --- | --- | --- | --- |
| Orders | `development_marts_publication` / `marts/development_publication` | `ANALYTICS_MARTS_SCHEMA` | `development-marts` |
| Customers | `shopify_customers_publication` / `marts/shopify_customers_publication` | `ANALYTICS_CUSTOMERS_MARTS_SCHEMA` | `development-customers` |
| Inventory | `goodday_inventory_publication` / `marts/goodday_inventory_publication` | `ANALYTICS_INVENTORY_MARTS_SCHEMA` | `development-inventory` |

Each job has a success sensor named `<job>_sensor` watching only its inputs in
`ingestion`. Manual repairs, lookbacks, sweeps and reconciliation qualify.
Failed runs and disabled inputs do not trigger publication. A queued or
not-started publication covers later successes **only in its own group**;
starting or running jobs do not. Run keys deduplicate repeated success events.
Sensors ship stopped and publication jobs have no retries. Failed validation
preserves the group's previous publication.

Writers and their publisher share a queue limit of one for their group's entire
run. Orders and Loop serialize together; customers and inventory use separate
limits. Read-only checks and unrelated groups need no shared slot. Direct
publisher calls must provide equivalent input isolation. The Dagster publisher
compares the selected group's parsed dbt source dependencies with its declared
inputs and rejects drift before building. Standalone callers can supply
`expected_sources` for the same guard.

To add a source, declare its group and apply that group's queue tag to its
writer jobs. Also map its dbt source name to the ingestion asset in
`CandidateTranslator.get_asset_key` in `dbt_execution.py`. Test the enabled
graph and keep flags identical in the code location and run containers; reload
definitions when flags change. Deployment leaves `GOODDAY_DATASET` unset, so
dbt reads `raw_goodday`, the schema ingestion always writes.

Namespaces come from the schema settings above, with
`ANALYTICS_MARTS_READER_ROLE` for grants. Missing, malformed or duplicate schema
settings fail before publication. Existing combined publications require the
[separate-destination migration below](#independent-jobs-and-migration).
Pause writers and publication sensors and drain queued/running jobs before
deploying both code locations and the coordinator. Old run tags cannot provide
the new isolation. No remote activation accompanies this change.

The Snowflake publisher supplies `DBT_PUBLICATION_ID` to candidate builds.
Offline fixtures supply a fixed generation when checking those model fields. `fct_orders.publication_id` carries that generation with the
facts, matching the publication marker rather than a later independent read.
Standalone dbt builds without this publisher context leave the field null.
`fct_data_availability.publication_id` stores the same generation with source
evidence. Cube consumers use ordinary queries with eventual consistency; they
need not request or compare generations. Separate metric/status queries may
observe different publications. Status does not certify separately queried facts;
the ID alone is not proof of source completeness or freshness.
Availability capture is atomic with the facts, and failed candidates preserve both.

### Dagster model visibility
The models and publication marker form one non-subsettable multi-asset: selecting
one requires selecting the whole publication group. This preserves the candidate
lock, complete validation, and atomic replacement. Model materializations describe
**candidate execution**, not consumer availability. Only the publication marker
means promotion succeeds. Model/check events are collected through `dagster-dbt`
and emitted when the publisher returns or fails, including successful models and
failed checks from a rejected candidate. They are not live per-model progress.

Definitions parse the project offline using current feature flags, so enabled
customer and GoodDay models appear without warehouse access. Model keys omit the
run-specific candidate schema. Source dependencies map to ingestion's existing
raw asset keys. The Pythonic integration supplies selection, graph and check
mapping; the stock component's independently selectable models would not preserve
this publication boundary.

Offline event tests call `build_candidate` in disposable DuckDB files. They
verify model/check events and retained diagnostics, not publication or serving.
The Snowflake coordinator uses the same build API; injected-runner tests check
that rejection prevents promotion. Live Snowflake acceptance remains necessary
for atomic replacement, concurrency and warehouse permissions.

Single-model data tests appear as asset checks. The maintained cross-model
reconciliation tests declare `config.meta.dagster.ref` to attach their checks to
the model they validate. New multi-model singular tests without that metadata
appear as observations on their dependencies. The pinned integration does not emit
Core unit tests as asset checks. All required tests, including these cases, still
block publication through the complete-result validation. Custom manifest checks
remain publication preconditions (coverage, grants, contracts and output types),
not a second execution-event translator.

## dbt documentation site
Colibri uses the separately locked `lineage/` environment because its SQL parser
version conflicts with `dagster-dbt`. Images install it at `/opt/lineage` and set
`COLIBRI_EXECUTABLE=/opt/lineage/bin/colibri`; no dependencies install at runtime.
Worktree setup also syncs `lineage/.venv`. To prepare it manually, run
`uv sync --project packages/transformations/lineage --frozen` from the repository
root. The CLI exchanges manifest/catalog files with the transformation process;
Python imports never cross between the two environments.

The `dbt_docs` job runs `dbt docs generate --static` and writes the single
self-contained page to `/var/lib/analytics/docs/index.html`, the Compose `docs`
volume that the [docs site](../../deploy/compose/README.md#dbt-documentation-site)
serves. It then runs [dbt-colibri](https://github.com/b-ned/dbt-colibri) on the
same manifest and catalog and writes a column lineage page to
`lineage/index.html`. Colibri traces columns by parsing each model's
compiled SQL, so a trace can be wrong for SQL it cannot resolve; check a
surprising trace against the model's code. The job turns off dbt's and Colibri's usage
statistics (`DO_NOT_TRACK=1`), and the site's content security policy blocks
any connection either page tries to make. The page holds the manifest and catalog: every source, model, column
description, test and lineage edge, plus column types and table sizes for
relations that exist. It contains no table rows. The catalog reads warehouse
metadata with the service identity; the job builds, loads and grants nothing.

The documentation command sets the `documentation_publications` dbt variable.
Only in this mode, mart schemas resolve through the orders, customers, and
inventory publication schema settings; restricted marts use each destination\'s
`_restricted` schema. Unset group settings retain the legacy
`ANALYTICS_MARTS_SCHEMA` default (`<environment>_marts`). Ordinary candidate builds
keep every selected model in the candidate namespace. Staging and
intermediate models exist only inside publication candidates, so the catalog
has no column types for them; their YAML descriptions still appear. For the
lineage page only, the job fills those models' columns from their declarations,
which publication already requires to match the actual outputs. The job
refuses to start without a known `ANALYTICS_ENVIRONMENT`. It stages both pages
beside their targets and renames them into place only after both build, so a
failed build keeps the previous site.

`dbt_docs_release_sensor` requests one build per transformations image, keyed
by `DAGSTER_CURRENT_IMAGE`, so each deployed release rebuilds the site within
five minutes. The sensor ships stopped; deployments preserve its enabled state.
Launch `dbt_docs` by hand to refresh table sizes between releases.

### Cube exposures
[`models/exposures/_cube__exposures.yml`](dbt/models/exposures/_cube__exposures.yml)
declares one dbt exposure per public Cube view, so the docs lineage graph
continues past the marts to `cube_orders` and, when customers are enabled,
`cube_customers`. Each exposure page lists the marts the view reads and a
table of its Cube fields with the mart columns they use. The metrics package
[generates the file](../metrics/README.md#dbt-exposures); do not edit it
by hand. The file lives outside `models/marts/` because publication builds
every resource under that path.

Additional `cube_model_classifications` entries inventory all Cube dimensions,
including hidden and unused ones, for classification validation. They are not
queryable public views, though their dependencies appear in the lineage graph.

## Published mart contracts
`analytics_transformations.mart_contracts` validates dbt manifests. Every model
under `models/marts/` must have:

- A physical table or incremental materialization, `access: public`, and an
  enforced dbt column/type contract. Other layers default to views.
- Model purpose, every column's meaning/type, `config.meta.owner`, and
  `config.meta.publication.grain` plus its `key` column list.
- Blocking `not_null` tests for every key column and a blocking `unique` test
  for a single-column key. Composite keys require a blocking `unique_combination`
  test whose `columns` are exactly the key. The checker finds that test from its
  native metadata; no `unique_check` name reference is needed in any layer.
- `config.meta.publication.checks`: explicit names of consequential correctness
  and upstream reconciliation tests. Review establishes whether their SQL checks
  the promised behavior; declaring a test name does not establish its meaning.
- Attached data tests use native severity (default `error`): errors block
  publication; warnings are informational. A warning cannot
  satisfy a required check. Model grants are prohibited: publication grants only
  the validated relation allowlist.

See `tests/fixtures/publication/` for a synthetic project using this interface.
It is an acceptance fixture, not a business dataset. The ordinary container gate
runs its real dbt builds, tests rejection, replacement and internal-data denial.

The checked-in `dbt/mart-contracts.json` is the reviewed physical contract
snapshot. The package gate compares it with `dbt/target/manifest.json` after
parsing. Generate a [review proposal](#generated-review-proposals) and review it
alongside the model change. The publication marker also stores the
contract snapshot and rejects changes incompatible with the actual previous
publication, even when a newer code release changed its committed baseline.

Compatibility preserves existing physical relation names, column names/types,
key and grain. New columns are nullable; an existing column cannot become
required. Removing a NOT NULL constraint is compatible. dbt's SQL contract check
and data tests verify candidate data separately. Semantic meaning, units,
exclusions, privacy and downstream positional SQL still require human review.

For a breaking change, add a new dbt model version with a distinct physical
alias alongside the old one. Keep both in the publication group, move consumers
in a subsequent deployed release, and retire the old version only after all
readers have migrated under [ADR 0020](../../docs/adrs/0020-keep-consecutive-releases-compatible.md).
The current runtime deliberately rejects destructive retirement: retiring a
published version requires an explicit reviewed migration of its compatibility
baseline after the migration window, rather than silently bypassing this gate.

When useful consumer detail is missing, extend the appropriate mart deliberately:
record the analytical need, grain, source identity, meaning/units, canonical
transformation and applicable access restrictions; add nullable documented
columns and tests; review compatibility and expose governed aggregation through
Cube where appropriate. Avoid raw payload columns and unrestricted source
passthrough. Raw/staging/intermediate remain diagnostic interfaces.

## Validated publication

`analytics_transformations.publication` exposes one typed publication entry point:

```python
publish_snowflake_marts(
    environment="development", namespace="dev_orders", publication_group="orders",
    project_dir=Path("/project"), profiles_dir=Path("/profiles"),
    reader_role="restricted_mart_reader",
)
```

Call this from the owning Dagster business job after source validation. Keep its
existing dataset-exclusive run tag; publication additionally holds its own lock.
Each call selects one complete **publication group**: outputs that must share a
validated version. `config.meta.publication_group` assigns each mart to a group;
the maintained project declares these in `dbt_project.yml`:

| Group | Outputs that publish together |
| --- | --- |
| `orders` | Order facts, lines, order contacts/geography, and `fct_data_availability` |
| `customers` | Current customer profiles and their restricted details/children |
| `inventory` | GoodDay current inventory |

Give each group a distinct Snowflake namespace. Each destination has its own
lock, marker, compatibility
history, and recovery. Do not serialize independent Snowflake groups with a
shared publication tag. Shared raw DuckDB writes still follow ingestion's
serialization rule. The existing 240-second command bound remains in place;
only the selected group's work must fit it.

`publication_group` is required when multiple groups are enabled. Unknown or
empty groups fail closed. A standalone project without group declarations keeps
one implicit `default` group; once any mart declares a group, all must do so.
A mart dependency or mart check spanning groups is rejected: move genuinely
shared transformation logic upstream or declare the outputs in the same group.
An ancestor's attached check is never silently dropped. If it requires a model
outside the selected dependency graph, publication rejects the unsupported scope;
make its required model dependencies explicit before retrying.

Builds select only the group's marts, ancestors, and checks. Cautious indirect
test selection prevents tests on unrelated descendants of shared ancestors from
entering the build. Required checks that inspect additional raw evidence are
selected explicitly. All model schemas route through `DBT_NAMESPACE`; model
grants are prohibited. Project parsing and static naming, declaration, and PII
policy checks still cover the project; independence is not protection from
invalid project code. Runtime validation and physical output inspection cover
only the selected group. Publication reads fresh manifest/run-results artifacts
with the same invocation ID and requires each selected mart, executable ancestor,
and associated test result. Ephemeral ancestors compile into their consumers; their dependencies
and tests remain required. Missing, duplicate, stale, skipped, failed and errored
results reject publication.
Declared informational warnings can pass. Runtime artifact checks supplement
`dbt build`; the caller cannot substitute one successful test for the suite.

Each successful publication contains `<namespace>_marts._publication` with one
row: `publication_id` (UUID text), `published_at` (UTC timestamp), and `contracts`
(JSON text). Return metadata includes the ID and completion time. The marker is
published in the same boundary as the marts; use its timestamp for publication
age, and the failed Dagster run for refresh status. Failure does not advance the
marker. An unpublished schema has no successful business publication.
Cube uses this marker for eventual cache refresh; separate requests can observe
different generations. It is not a first-request freshness SLA.

### Independent jobs and migration
`development_marts_publication` now selects only `orders`.
`shopify_customers_publication` and `goodday_inventory_publication` select their
own groups. Their success sensors request publication after their own input
writers finish, independently of orders. All sensors ship stopped; owners choose
which to enable. Source schedules do not change.

**Existing combined publications require a migration.** A subset cannot replace
an old combined schema: compatibility checks reject removing its other
contracts. Do not bypass this check or delete the old publication.

1. Pause publication sensors and input writers, then drain old publisher and writer runs. Keep the last good
   combined publication available to readers. Select fresh, distinct namespaces
   for orders, customers, and inventory as applicable.
2. In Dagster, launch each publication job with its `namespace` run config set
   to the new namespace. This builds before changing consumer configuration. For
   example, the order job accepts:

   ```yaml
   ops:
     marts__development_publication:
       config:
         namespace: development_orders
   ```

   Customer and inventory op names are `marts__shopify_customers_publication`
   and `marts__goodday_inventory_publication`. Feature flags must enable the
   selected group. The Python APIs also accept explicit namespaces and groups.
3. Verify each marker, business checks, and reader grants. Verify that orders and
   availability carry the same generation. An inventory failure must not prevent
   an order publication. No warehouse data changes occur during code deployment.
4. Set `ANALYTICS_MARTS_SCHEMA` to the new order schema and
   `ANALYTICS_CUSTOMERS_MARTS_SCHEMA` to the new customer schema. Set
   `ANALYTICS_INVENTORY_MARTS_SCHEMA` for the inventory job. These must be distinct.
   Cube uses the customer schema's own marker and connection; public order and
   customer views keep their names. The hidden legacy order/customer join is not
   offered across independent publications. Enable the desired group sensors
   and resume input writers after verifying their group-specific queue tags.
5. Keep the old combined destination during the compatibility window. Rollback
   restores the previous image **and its old schema settings**; it does not
   delete new data. Retire the old destination only after all consumers migrate,
   as separate reviewed data work under ADR 0020/0021.

Leaving the customer schema setting blank preserves Cube's legacy combined
reader during migration. It does not restore the old all-marts publisher.
Live Snowflake acceptance and remote activation require explicit approval;
offline fixtures do not establish deployment.
### Retained publication diagnostics
Each attempt retains separate `parse/`, `build/`, `validate-outputs/` and
publication-macro directories under
`$DBT_TARGET_PATH/publications/<publication_id>/`. Without `DBT_TARGET_PATH`, the
root is `<project_dir>/target`. Container images use
`/var/lib/analytics/artifacts/dbt`, on the existing persistent artifacts volume.
Cleanup does not overwrite build manifests or run results. Success metadata
includes `artifacts_directory`; failure logs name the private directory.

Each invocation retains available manifest/results, compiled SQL, `dbt.log` and
`console.log`, including failures and timeouts. Direct CLI publication calls use
the same retention. Attempt and invocation directories are owner-only (0700).
Raw errors can contain sensitive values: do not serve these files through the
docs site or copy them into tickets. Dagster shows safe status, known failing
node IDs and paths rather than raw SQL, row values, or adapter error text.

There is no automatic expiry. Operators must monitor artifacts-volume capacity
and remove completed attempts after their incident/audit retention requirements
are met. Never remove an active attempt or evidence needed for publication
recovery. Snowflake candidate cleanup is separate from artifact retention.

### Restricted marts

Every mart declares `config.meta.publication.access`; a missing or unknown value
fails closed.

- `analytical`: readable by the default reader (Cube). `fct_orders` is
  `analytical`. `dim_order_contacts` is `analytical` only while
  `SHOPIFY_CUSTOMERS_ENABLED` is not `true`, so a deployment without customer
  marts can keep its existing snapshot while it migrates.
- `restricted`: for personal or sensitive detail. It never reaches the default
  Cube reader.

The access value is part of the reviewed contract snapshot. A snapshot entry
without `access` reads as `analytical`. Changing a published relation's access
in either direction fails compatibility until an explicit migration exists.

The one reviewed migration is `REVIEWED_ACCESS_TIGHTENINGS` in
`mart_contracts.py`: only `dim_order_contacts` may move from
`analytical` to `restricted`. The Snowflake publisher accepts it only
for those relations and only in that direction; the Snowflake macro receives
the list as `access_tightenings`. Loosening, any other relation, and a customer-enabled
deployment that returns to `SHOPIFY_CUSTOMERS_ENABLED=false` still fail. The
migration drops the relation from the default reader's grants and
keeps all rows in the restricted publication.

`publish_snowflake_marts` revokes every table grant for the default reader on
the cloned candidate, then grants SELECT only on analytical marts plus the
marker. It then refuses to continue if the default reader or PUBLIC can read a
restricted relation. Pass `restricted_reader_role` (different from `reader_role`)
to grant SELECT on restricted marts and the marker to that role. Without it,
restricted marts are granted to no consumer and `restricted_relations_unpublished`
names them. The Snowflake marker holds contract metadata (names, types,
descriptions, no values) for every mart so compatibility holds across swaps;
the default reader can read that metadata. The table-grant check covers direct
grants; role inheritance remains an administrator responsibility, as above.

### Snowflake

The supported execution target is explicit development. Production enablement
and service-identity provisioning are separate reviewed operational changes.
The publisher creates `<namespace>_publication_lock` without `IF NOT EXISTS`;
Snowflake's atomic DDL makes this a cross-process/cross-host publisher guard.
Its lease table contains the generation ID. It clones the old mart schema using
native zero-copy cloning, or creates an empty candidate for first publication.
The dbt build targets `<namespace>_candidate_<id>_<layer>` schemas. Existing
incremental models can update their clones; ordinary tables still rebuild. This
infrastructure introduces no Shopify changed-key/deletion policy or performance
claim. Incremental adoption remains evidence-driven work.

Before swap it verifies the exact physical table allowlist and prior contracts,
writes the marker, grants SELECT on each approved table plus the marker, then
grants candidate schema USAGE and performs one schema SWAP (initial RENAME).
No view is published. Granting USAGE creates a narrow interval during which the
validated, immutable candidate can be read by its temporary name. Unvalidated
candidates have no consumer schema USAGE. Database future grants are rejected;
admin provisioning must avoid broad inherited roles, PUBLIC schema grants and
future grants that bypass this boundary. dbt `access: public` controls dbt refs;
it does not grant any warehouse privilege.

The restricted consumer role needs database and warehouse USAGE, with SELECT
and schema USAGE supplied by publication. Diagnostic roles receive separate,
explicit internal-layer privileges. Neither is the writer role. Verify access
using the consumer role with secondary roles disabled, including denial tests;
owner-role query success is insufficient. Full cross-tool identity policy is
out of scope.

After a successful swap, cleanup drops the former publication at the candidate
name and the candidate internal schemas, then releases the lease. The previous
generation is protected throughout validation; this is not a retention/archive
service. A failed build cleans its candidate while retaining published data.
Preparation failures retain the guard, because a partial clone may exist.

If swap returns an error or times out, the publisher reads the stable published
marker. It cleans up only when that marker confirms its generation; it never
blindly repeats SWAP. If confirmation fails, the guard remains. Cleanup failure
also leaves explicit recovery work and does not prove the new publication failed.
All dbt subprocesses have a 240-second maximum; publication Snowflake statements
have a 60-second timeout. Long builds need a separately designed bounded execution
policy rather than removing these limits.

**Crash recovery:** stop/terminate the original publisher and verify no owned
query is running. Read the stable marker and compare its ID to the lease. If it
matches, publication committed; otherwise retain the last published output.
Inspect and remove only that lease's candidate schemas, then drop the lock schema.
Never swap back automatically, steal a lease based only on elapsed time, or run
cleanup while an unconfirmed original query may still publish. Recovery SQL is
operator incident work, recorded on the issue under ADR 0021.

### Explicit development acceptance

`tests/test_publication_snowflake.py` is skipped ordinarily. It requires
`PUBLICATION_LIVE_ACCEPTANCE=development`, `ANALYTICS_ENVIRONMENT=development`, the
standard explicit Snowflake connection variables, and two freshly provisioned
roles in `PUBLICATION_READER_ROLE` and `PUBLICATION_DIAGNOSTIC_ROLE`. Point the connection at a
development database and warehouse. Run it in the transformation check image:

```sh
pytest tests/test_publication_snowflake.py -xq
```

The test creates uniquely named disposable schemas, executes successful and
rejected dbt publications, rejects a competing publisher, verifies candidate and
internal-layer denial plus diagnostic access, and removes only its own schema
prefix. Role provisioning/cleanup is
separate and must be coordinated. It performs no production or Shopify data
changes. Ordinary local validation does not establish Snowflake execution;
record opt-in live evidence separately. 
`tests/test_naming_adapters.py` executes naming acceptance on DuckDB in
ordinary checks and on Snowflake with `NAMING_LIVE_ACCEPTANCE=development`,
`ANALYTICS_ENVIRONMENT=development` and the same explicit connection variables.
It exercises native versions, authored aliases, case folding, the 255-character
limit, rejected identifiers/collisions, auxiliary schemas and two concurrent
namespace workers. It updates and cleans one namespace while verifying the other
retains its distinct outputs; shared DuckDB writers are serialized. It creates
and removes only disposable schemas under its generated prefixes. 
`tests/test_output_types_adapters.py` checks ephemeral decimal precision and
scale drift through candidate validation. DuckDB runs ordinarily; Snowflake
checks publication as well and needs
`NAMING_LIVE_ACCEPTANCE=development`, the same explicit connection/target guards,
and a restricted, separately provisioned `PUBLICATION_READER_ROLE`. After rejection,
the Snowflake reader must still see the previous publication ID and values.
Snowflake inspection uses zero-row query results and `DESCRIBE RESULT` to retain exact types, because the pinned
adapter's query-schema API loses numeric precision and scale. Unbounded `varchar`
declarations permit Snowflake's different default string capacities; explicit
string lengths and numeric precision/scale still compare exactly.

The live test in `tests/test_shopify_orders_snowflake.py`
is skipped ordinarily. Its offline regression runs the complete harness on two
separate DuckDB files through the connector/build seams: fixture loading, both
builds, exact contract selection, every value comparison and exact-run cleanup.
Only the three order marts and their availability evidence may be built or
selected as contracts. This local connector substitute does not prove Snowflake
SQL, privileges, physical types or connector conversions. The build selects
`+path:models/marts/shopify_orders`; the pytest declaration also includes the raw
source dependencies of the selected currency-consistency test. Customer and
inventory marts are outside this parity check. The shared fixture setup pins all
three optional source flags to false, regardless of the invoking environment.
With
`ORDERS_LIVE_ACCEPTANCE=development` and the standard Snowflake connection
variables, it loads the good order fixtures into a disposable
`orders_accept_<id>_raw` schema, builds the order marts on DuckDB and Snowflake,
and requires identical columns, value types and exact values. Decimal comparison
never rounds through the Python decimal context. The sole runtime-generated
field, `fct_data_availability.evidence_captured_at`, must be a UTC-naive timestamp
within its own adapter build's UTC start/end window; Snowflake bounds come from
two `select sysdate()` queries on the same connection, not the client's clock.
Two separate builds cannot have identical capture times. Every other availability value still
compares exactly. Offline corruptions reject changed values, missing/duplicate
rows, reordered columns, out-of-window timestamps and unexpected contracts. It also
validates every model's output against its declarations on both adapters. It needs no
provisioned roles, uses the same target guards and drops only its own schema
prefix. Run only the live comparison with
`pytest -n 0 tests/test_shopify_orders_snowflake.py::test_order_marts_match_on_duckdb_and_snowflake -q`.
The payment-flow failed-attempt indicator explicitly casts to its declared integer
type: Snowflake otherwise infers `NUMBER(1,0)` from the zero/one literals rather
than the declared integer's `NUMBER(38,0)`. Exact output validation stays enabled.

## Shopify current customers
Customer models activate with `SHOPIFY_CUSTOMERS_ENABLED=true`; the default is
`false` for environments without customer raw tables. Set
`SHOPIFY_CUSTOMERS_DATASET` to the independent ingestion dataset. Customer
publication reads only customer inputs; it does not build order models.

`dim_customers` contains one current row per `(shopify_shop_id,
shopify_customer_id)`, including customers without orders. It retains Shopify
summaries as source values, with their source currency. It adds no platform
lifetime-value, first-order, cohort or segment calculations. Current profile
changes do not change `dim_order_contacts`, which retains order-time details.

With customers enabled, `dim_order_contacts` is `restricted`: its email, phone,
names and street lines never reach the default reader or Cube. Approved city
and postal code stay available in the `analytical` `dim_order_geography` mart
(one row per order, same grain), alongside billing and shipping province and
country names and codes. `fct_order_lines` also stays analytical. Enabling
customers on an existing deployment applies the reviewed access tightening
above. Publish with a restricted destination or role to keep the order-contact
detail reachable; without one it is built and tested but exported to no consumer.

The restricted marts are `dim_customer_details`, `dim_customer_addresses`,
`dim_customer_tags`, `dim_customer_metafields`, `dim_customer_tax_exemptions` and
`dim_customer_company_contacts`. They retain source personal details and full
child collections. Metafield values include only approved namespace/key pairs.
Company contact roles use the source list position in their
key so one contact can have multiple location assignments. All child keys
include both shop and customer. Free text, direct contact values, street lines,
coordinates, tags, metafields and company contacts never enter the customer Cube
model. Approved default-address city and postal code are available in
`dim_customers` and Cube as `default_address_city` and
`default_address_postal_code`. Both are null when no default address is available.
The email-format flag stays in restricted customer details; it does not enter
the analytical customer mart.

Per-field-group access statuses distinguish a legitimate null from
`access_denied`, `unsupported`, `excluded` or missing evidence (`unknown`).
The profile evidence source warns after 25 minutes and fails after 30 minutes;
only complete, zero-reject incremental windows count. Empty successful windows
count. The full-sweep source warns after 26 hours and fails after 30 hours.
Shopify summaries can change without advancing `updatedAt`, so their coverage
uses full sweeps rather than the 30-minute profile claim. The stopped
`shopify_customers_validation_schedule` runs both dbt checks every five minutes
when enabled and skips overlapping validation runs.

`dbt/mart-contracts.json` records the default order-only deployment.
`dbt/customer-mart-contracts.json` records the enabled project declarations,
not one publication: 12 marts across the `orders` and `customers` groups.
Every snapshot column carries its reviewed `pii` flag. Cube reads column
descriptions, types and PII flags from this file (see the
[metrics README](../metrics/README.md#dbt-column-metadata)), so
regenerate the Cube exposures after changing a description that a Cube field reads.
The customer group has one analytical mart and six restricted marts; order
contacts remain in the order group even when customer activation restricts them. `fct_order_lines` carries approved
order city and postal code and stays analytical with customers enabled.
Customer fixture builds check the enabled snapshot, declarations, output types,
keys and reconciliation. Do not disable customer models after publication:
removing published relations requires an explicit compatibility migration.

### Customer activation

Before loading customer personal data, migrate Cube to its dedicated reader user
and key. Grant that user only the analytical role. Verify raw and restricted
SELECT denials and `USE ROLE <writer>` denial with its credentials.
City/postal members and analytical order-line access stay available after
customer activation; those consumers do not migrate. The reviewed access
tightening applies only to direct order-contact details and preserves every
other physical column, type, key, grain and nullability compatibility rule.

Measure the full sweep and refresh durations before starting schedules.
The blocking time, cadence and refresh runtime must fit the 30-minute profile
limit. Review historical order-contact retention and redaction separately.
Customer deletion does not redact historical order contacts.

1. Set the isolated customer dataset and run the ingestion seed and full sweep.
2. Verify extraction access evidence and customer validation results.
3. Publish the `customers` group to its own destination with
   `SHOPIFY_CUSTOMERS_ENABLED=true` and a separate analytical reader role; verify
   restricted SELECT denial. Publish the `orders` group separately to apply its
   order-contact access tightening.
4. Set `SHOPIFY_CUSTOMERS_ENABLED=true` for the transformation code server and
   Cube, and set `ANALYTICS_CUSTOMERS_MARTS_SCHEMA` to the validated customer
   schema. Follow the migration procedure below for an existing combined schema.
5. Start the refresh, sweep, reconciliation and validation schedules in Dagster.

The opt-in `tests/test_customer_snowflake.py` uses synthetic fixtures only.
It requires `CUSTOMERS_LIVE_ACCEPTANCE=development`, the explicit development
Snowflake variables and a separately provisioned `CUSTOMERS_READER_ROLE`.
Ordinary fixture checks do not establish live Shopify access or Snowflake grants.

The integrated customer test requires all eleven marts, including
`fct_order_lines`, to match DuckDB columns, physical types and values. The
temporary analytical reader must not SELECT the seven restricted marts or raw
tables. A failed candidate preserves the published generation. The temporary
role and disposable schemas are removed, and the exact fixture namespace cleanup
is verified. These checks use disposable readers, not deployed credentials.

### Loop fee evidence and USD allocation
`stg_loop_returns__headers` is empty unless `LOOP_RETURNS_ENABLED=true`.
Its source is the separate `LOOP_RETURN_HEADERS_RAW` dataset, configurable
through `LOOP_RETURNS_DATASET`. It does not fall back to legacy returns.
It converts the numeric Loop provider order ID to a Shopify Order GID before
joining maintained Shopify records. Header and snapshot evidence must share a
completed dlt load package. A closed return alone does not establish a fee match.
Loop amounts retain their own currency label; they are not assumed shop money.

Before relying on the join, check a bounded sample of headers against Shopify
orders in one shop, including shop and presentment currencies. A sample does
not establish a currency basis for cross-currency orders or historical coverage.

`shopify_allocate_usd` allocates a resolved component in exact USD cents.
It uses value weights, then quantity weights, then equal shares. A fallback
requires every preceding weight to be known zero. Missing weights do not become
zeros. Largest remainders receive the remaining cents; source line ID breaks
ties. Signed amounts conserve the original total. Fractional-cent input totals
remain unresolved, and numeric overflow fails rather than approximates.
Source assignment and merchandise eligibility remain the caller's responsibility.
No input lines produce no allocations; the order retains its unallocated amount.
The macro passes DuckDB fixtures and four read-only development Snowflake parity
fixtures. Wider Snowflake fixtures and validation on real development data
remain open.

`int_shopify_order_lines__allocations` applies the macro to each line-level
remainder. Discount is allocated first. Refund, shipping and shipping refund
weights use gross merchandise plus the final discount. Gift-card lines take no
share. An unknown gift flag leaves every component of its order unresolved.
`int_shopify_orders__allocations` reports each order's unallocated residual and
an `allocation_status`. A NULL residual means unknown, not zero. Both models are
intermediate. `fct_order_lines` and `fct_orders` publish their results as
nullable additive fields. The blocking `shopify_published_commercial_reconcile`
and `shopify_published_net_units_reconcile` tests compare every published line
and order with these models, so a wrong candidate cannot replace the last
validated publication. Tests cover a corrupted line allocation, line net
quantity and order net revenue.

## GoodDay current inventory
The three models activate only with `GOODDAY_INVENTORY_ENABLED=true`. Set
`GOODDAY_DATASET` to the independent raw capture dataset (default `raw_goodday`).
The dataset must contain `inventory_by_item`, `inventory_captures` and
`_dlt_loads` from the maintained ingestion connector. In DuckDB it must share
the working database with other enabled sources.

| Model | Meaning |
| --- | --- |
| `stg_goodday__inventory_captures` | Capture evidence joined to successful dlt loads (`status = 0`), including empty results |
| `stg_goodday__inventory_items` | Source-shaped captured rows, exact quantities and UTC timestamps |
| `fct_inventory__current` | Latest complete capture, at merchant × item × channel × location grain |

Select the latest capture by observation time, then extraction time, capture ID
and load ID for deterministic ties. Rows join evidence on **both capture and
load identity**. Failed or uncommitted loads are ineligible. Missing successful
capture evidence fails validation; a successful zero-row capture publishes an
empty mart rather than retaining older items. Successful-load count mismatches
fail validation instead of silently selecting an older capture. Checks cover
all retained successful captures, not just the latest one.

Channel and location source IDs may be null and remain null. Merchant and item
IDs must be present in current inventory. A blocking business-key check groups
null dimensions together and rejects duplicate balances. The physical contract
key is `(inventory_capture_id, inventory_load_id, inventory_row_number)`: this
identifies a retained observation without inventing a source channel or location.
It is not a stable cross-capture product identity.

Quantities retain GoodDay's separate on-hand total, sellable, unsellable,
reserved, committed and available values; available, ATS, inbound total and
available, fulfilled, transfer-in-transit and return quantities. Every quantity
uses `decimal(38, 9)` and preserves zero, negative and null values. No stock
formula, cost/valuation, SKU cleanup, normalized label or product enrichment is
added. Raw SKU casing/whitespace, source labels and item status remain unchanged.

`observed_at` is when the source read started; `captured_at` is capture start.
The mart has no BigQuery job ID; `inventory_load_id` identifies the dlt load. `source_updated_at` is the item's source update time. None
establishes vendor refresh freshness, and rebuilding does not change them.
This slice does not implement the one-hour business freshness policy or
schedule. PO/transfer detail and live acceptance remain separate parent work;
Daily and intraday historical reporting is future work.

Publish with `publication_group="inventory"` to its own Snowflake namespace. Enabling GoodDay does not add inventory builds or correctness checks to
order/customer publication. Inventory publication needs no Shopify raw tables.
The `goodday_inventory_publication` job uses
`ANALYTICS_INVENTORY_MARTS_SCHEMA` and the `development-inventory` publication-input
queue tag shared with inventory capture. Its stopped-by-default success sensor
can request publication after each successful capture.
It shares no publication lock or recovery with orders or customers.
The reviewed `dbt/goodday-mart-contracts.json` remains a project-wide declaration
snapshot (orders plus enabled GoodDay), not the contents of a publication marker.
An inventory marker contains only the inventory contract. An empty/disabled
inventory group cannot publish and cannot erase a prior inventory publication.

`tests/test_goodday_inventory.py` uses synthetic DuckDB fixtures and the real
model validation gates to check selection, empty captures, failed loads, load
matching, nullable business keys, exact values, repeat builds and bad-data rejection. These tests are not live GoodDay or Snowflake acceptance. No
schedule, deployment configuration, live data load or consumer metric is added.
