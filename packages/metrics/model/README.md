# Semantic definitions

Shared Cube definitions currently serve published Snowflake dbt marts. Offline
fixtures execute the same definitions on DuckDB. The accepted
[local-first design](../../../docs/specs/20261101-local-first-development.md)
reuses them for single-user serving. The bounded synthetic demo uses the same
models; the real-data lifecycle still needs implementation. New AOV/net-sales,
AOV coverage and the entire line interface require
`GOVERNED_ORDER_METRICS_ENABLED=true`, accepted only with explicit local DuckDB.
The default-off configuration preserves existing remote members and exposure
lineage. Local results do not prove Snowflake publication or reader readiness.

| File | Public interface | Marts |
| --- | --- | --- |
| `orders.js` | views `orders`, `revenue_coverage` | `fct_orders`, `dim_order_geography` when customers are enabled; legacy `dim_order_contacts` otherwise |
| `order-lines.js` | view `order_lines` | `fct_order_lines`, `fct_orders` for shared scope dimensions |
| `customers.js` | view `customers` | `dim_customers` (allowlisted columns only) |

Each maintained cube reads its tables and the shared publication refresh key
from the configured published schema:

```js
const { publishedMarts } = require('../publication.cjs');
const marts = publishedMarts();

cube('example', {
  sqlTable: marts.table('fct_orders'),
  refreshKey: marts.refreshKey,
});
```

Dimensions that read a mart column under its own name come from
`dbtDimensions()` in `mart-columns.cjs`, which takes their description, type and
`meta.pii` from the dbt mart contract. Edit their wording in dbt, not here. See
[dbt column metadata](../README.md#dbt-column-metadata).

Every dimension, measure and segment needs an explicit boolean `meta.pii`,
including hidden members. Generated lists carry a decision per entry; do not
add a default that silently classifies future fields. The compiled-model check
rejects missing declarations and all restricted PII until the platform provides
privileged access. See the [PII publication gate](../README.md#pii-publication-gate).

Model files cannot read `process.env` directly: Cube compiles them in a
sandbox. See the package README for the schema setting, cache convergence and
reader lifecycle guarantees.

## Publication identity
Use ordinary queries for revenue and availability. Publication IDs are optional
diagnostics, not required consumer inputs. Independently cached queries may
observe different publications; consumers do not compare IDs or retry a mismatch.
Availability describes captured pipeline evidence and does not certify the age
or completeness of separately queried revenue. Missing evidence and empty results
do not prove completeness. See the package README's eventual-consistency contract.

## Governed revenue
`orders.net_revenue_usd` sums only orders with explicit true revenue eligibility.
Its components use exactly the same population: `gross_sales_usd`,
`discounts_usd`, `merchandise_refunds_usd`, `shipping_revenue_usd`, and
`shipping_refunds_usd`. Discounts and refunds are signed negatives; add all five
components to reconcile net revenue. Amounts exclude tax, gift-card sale value,
and separate handling fees. Existing source-total measures are unchanged.
Replacements, exchanges, gifts and cancellations are not universal exclusions.

`reporting_market` is the shipping country for each market listed in the dbt
`separate_reporting_markets` var, and `home_reporting_market` for all others,
including a missing country. It is not actual shipping geography. For the
home-market D2C revenue view, filter the home market, `is_d2c = true`, and `is_draft_order = false`. The draft field uses the
exact `shopify_draft_order` marker; missing source name is unknown, not false.
These scope filters do not replace the revenue measure's eligibility rule.

The independent `revenue_coverage` view reads all order facts, including tests.
It separates eligible, excluded and unresolved counts and exposes all exclusion
and unresolved-requirement identifiers. Group by D2C, draft and test flags to
retain unknown scope; filtering only true D2C would hide unknown channels.
Counts by requirement may overlap. Coverage does not estimate omitted revenue
or establish source freshness. Publication IDs remain optional diagnostics.
`orders.net_sales_usd` uses revenue eligibility and excludes shipping.
`orders.aov_usd` divides `aov_revenue_usd` by `aov_order_count`, using explicit
AOV eligibility, not revenue eligibility. It sums before dividing and returns
null for a zero denominator. Whole-order AOV stays at order grain; line filters
are not an AOV interface. `revenue_coverage` also exposes AOV verdicts, reasons
and eligible/excluded/unresolved counts, retaining tests and unknown scope.

`order_lines` exposes the shop/order/line source IDs, latest source `sku` and
original `order_date`. It excludes tests but retains other line activity.
Its default `net_ordered_quantity` and `net_sales_usd` use revenue eligibility,
not ASP eligibility. Unknown quantities contribute no value, not zero. Negative
activity remains visible. `asp_usd` divides summed `asp_net_sales_usd` by summed
`asp_net_ordered_quantity` over the same explicitly ASP-eligible line population;
a zero denominator returns null. A full amount-only refund can leave units with
zero sales. Eligible/excluded/unresolved line counts and reason dimensions keep
coverage separate from known amounts. `unresolved_quantity_count` identifies
revenue-eligible non-gift-card lines with missing net units, even when revenue
eligibility is known. Counts do not prove loaded-history coverage.

The line view joins orders many-to-one only for shared `reporting_market`,
`is_d2c` and `is_draft_order` dimensions. It never exposes order measures or AOV.
SKU is not a unique line identity; group by the source-qualified IDs when a line
is required. Refunds restate the original order day, and reporting uses the
reporting time zone. Historical catalog enrichment is not part of this interface.

**Release gate:** this local-only slice can merge only with tested default-off
remote behavior: no new members, line model or enabled exposure dependencies.
Shared Snowflake activation remains disallowed until expanded marts are actually
published, actual-predecessor compatibility passes and the deployed reader can
read the new columns. Offline fixtures and a successful service deployment do
not satisfy those deferred shared-release gates.
