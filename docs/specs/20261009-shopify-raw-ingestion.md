# Shopify order raw field contract

Date: 2026-10-09\
Status: In force. Extraction, state and recovery follow the
[dlt-native rebuild specification](20261013-shopify-orders-dlt-rebuild.md) under
[ADR 0024](../adrs/0024-load-shopify-orders-with-dlt-incremental-merge.md).
This document keeps the raw field contract those use.

## Line fulfillment evidence (revision 4)

The maintained paged query adds `LineItem.unfulfilledQuantity`,
`nonFulfillableQuantity`, and `requiresShipping`. These map to nullable
`orders__lines.unfulfilled_quantity`, `non_fulfillable_quantity`, and
`requires_shipping`. New observations require all three fields; a missing field
rejects the observation rather than recording zero. Older stored rows remain
nullable under ADR 0020. The current query fingerprint is `d921d92881f5b6e3` and
`_extraction_revision` is 4. Revision 3 identifies the dlt-native rebuild; the
revision-2 sections below describe the earlier field expansion, not the current
query fingerprint.

These fields preserve source evidence. Ingestion does not calculate business
net units. `current_quantity` already excludes refunded and removed units;
`non_fulfillable_quantity` is not a second refund deduction. The transformations
package uses the unfulfilled quantity to remove additional cancelled units.
It keeps quantities unresolved if cancellation evidence is absent or invalid.

Read-only API checks on 2026-10-20 confirm the fields on a fulfilled partial
refund, an amount-only refund, and cancelled unfulfilled lines, including two
cancelled orders with partial quantity refunds. Those samples do not establish
historical coverage or a physical-return model. The existing paged line reads
select the new fields on every page. Parser and raw-schema compatibility tests
cover the additive fields and missing selections.

Revision 4 does not re-extract history by itself. After deployment,
use the existing Dagster re-extraction job for the agreed reporting creation
windows. Estimate runtime and warehouse cost before that operation. Do not
infer coverage from the most recent extraction timestamp alone. Transformations
handle both absent physical columns before the first revision-4 load and null
values in older rows.

## Responsibility boundary

Ingestion owns the extraction timeline: explicit UTC windows, source cursors and
versions, observation order, completeness, and load recovery. It reports
factual timestamps, coverage evidence and limitations.
Batch completion means the requested work passed its technical checks.

Dagster owns when work runs: cadence, window planning and overlap configuration,
priorities, catch-up, whole-run retry coordination, and execution deadlines.
Platform freshness checks consume ingestion evidence and apply consumer targets,
warning thresholds and reporting deadlines. The transformation package owns
these expectations and checks alongside its source/model contracts; Dagster runs
them and acts on their results. Consumer-specific serving checks extend the
contract at the serving boundary. Follow
[freshness ownership](../../engineering-standards.md#freshness-ownership).

The source/loader accepts an explicit window and execution limits; it does not
read a business freshness target or classify data as late. API throttling,
bounded request retries, I/O timeouts and cancellation remain ingestion concerns.
A technically complete batch can still miss a platform freshness target; timely
execution cannot make an incomplete batch complete.

## API and raw field contract

Pin **2026-10**, listed as latest stable in Shopify's
[versioning schedule](https://shopify.dev/docs/api/usage/versioning), checked
2026-10-09. It was released October 1, 2026 and is accessible until **October 16,
2027 at 15:00 UTC**. The research version 2026-01 is accessible until **January 16,
2027 at 15:00 UTC**. Preserve those versions in historical evidence.

The versioned Order, LineItem, Refund and OrderTransaction references were
inspected for the selected shape. Execute the exact expanded queries against the
pinned API and validate scopes before accepting a change. Compare X-Shopify-API-Version with
the requested version on every response; fail on silent fallback. Review upgrades
quarterly and before retirement; never dynamically select “latest.”
read_all_orders enables older access but does not prove complete history.

Use an explicit environment database and the raw dataset named by
`SHOPIFY_ORDERS_DATASET` (default `SHOPIFY_ORDERS_RAW`). Commit exact queries and
a field/type/nullability/key mapping before accepting a source change.

| Logical table / grain | Required source fields |
| --- | --- |
| orders: store/order | id, name, createdAt, updatedAt, processedAt, test, sourceName, cancelReason, cancelledAt, closedAt, customer ID, displayFinancialStatus, displayFulfillmentStatus, currencyCode; totalPriceSet, currentTotalPriceSet, subtotalPriceSet, currentSubtotalPriceSet, totalDiscountsSet, currentTotalTaxSet, totalRefundedSet; transactionsCount count/precision as evidence |
| order_lines: store/order/line | id, parent order ID, sku, name, quantity, currentQuantity, originalUnitPriceSet, discountedTotalSet, product ID, variant ID |
| refunds: store/order/refund | id, parent order ID, createdAt, updatedAt, totalRefundedSet |
| refund_lines: store/refund/refund-line | id, ancestor IDs, referenced line-item ID, quantity, subtotalSet, totalTaxSet |
| refund_adjustments: store/refund/adjustment | id, ancestor IDs, amountSet, taxAmountSet |
| refund_shipping_lines: store/refund/refunded-shipping-line | id, ancestor IDs, subtotalAmountSet, taxAmountSet |
| order_transactions: store/order/transaction | id, parent order ID, parent transaction ID, createdAt, processedAt, kind, status, gateway, test, errorCode, amountSet |

Retain both shop and presentment amount/currency for every selected MoneyBag.
Preserve decimal precision without floating-point conversion; keep source text
where precision matters. Use tested decimal raw columns or source text where
lossless conversion is not assured. Store timestamps as UTC instants; missing
timestamps and product/variant/customer references remain null.

dlt may flatten objects and nest physical tables. Record actual physical names
in the mapping, and expose stable source IDs plus explicit ancestor IDs without
requiring dlt internal keys. Include store identity in logical keys.

Retain canceled orders and failed/pending transactions. No valid-order filters,
net-sales calculation, payment-to-merchandise-refund inference, or fabricated
refund objects. Revision 1 excluded contact/addresses, notes, allocations, tax
detail and shipping charges. Revision 2 adds the selected evidence below.
Payment credentials/details, gateway receipt payloads, full fulfillment and
physical-return objects remain outside this contract.

### Revision 2: order detail and economics
The maintained query/parser contract now uses extraction revision **2** and
query fingerprint **`61387e9ee64092d9`**. Exact documents live in
[`queries.py`](../../packages/ingestion/src/analytics_ingestion/shopify/queries.py);
[`model.py`](../../packages/ingestion/src/analytics_ingestion/shopify/model.py)
declares every column, type and storage nullability. The committed
[schema snapshot](../../packages/ingestion/tests/fixtures/shopify_raw_schema.json)
guards additive compatibility. New columns on existing tables are nullable for
revision-1 history; a revision-2 observation must actually contain every selected
field, including explicit nulls and empty lists. GraphQL access errors and partial
responses fail extraction. A source null is retained without inventing a reason.

Every child carries `shop_id`, `order_id` and its intermediate ancestor IDs.
Money bags retain shop/presentment amounts and currencies at decimal `(38, 9)`.
The following names are physical tables under the configured raw schema:

| Table / additional grain | Revision-2 evidence |
| --- | --- |
| `orders` | `email`, `phone`, `taxes_included`; `original_total_price_*`, `total_tax_*`, `total_shipping_price_*`; flattened `billing_address_*` and `shipping_address_*` |
| `orders__tags` / tag; `orders__discount_codes` / code | Source tags and discount codes |
| `orders__lines` | Separate `title`, nullable `variant_title`, `is_gift_card` |
| `orders__discount_applications` / application index | Concrete type, allocation method, target selection/type, code/title/description where selected, value type and money or percentage value |
| `orders__lines__discount_allocations`, `orders__shipping_lines__discount_allocations` / source list position | Exact allocated amount and the order-scoped `application_index` |
| `orders__shipping_lines` / shipping-line ID | Title, code, source, removal flag; original, discounted and current discounted prices; removed shipping lines are included |
| `orders__tax_lines`, `orders__lines__tax_lines`, `orders__shipping_lines__tax_lines` / source list position | Title, rate, nullable channel liability, price money bag |
| `orders__agreements` / agreement ID | Concrete type, happened-at timestamp, reason; refund/return ID on the applicable agreement type |
| `orders__agreements__sales` / sale ID | Concrete type, action/line type, nullable signed quantity, total amount/tax and discounts before/after taxes; product/gift-card/tip line ID, shipping-line ID, duty ID, additional-fee ID/name/price or return-fee ID/type where applicable |
| `orders__agreements__sales__taxes` / sale-tax ID | Source tax amount and tax-line detail |
| `orders__refunds` | Note, processed-at timestamp, nullable return ID |
| `orders__refunds__adjustments` | Nullable source reason; signed amounts retain source meaning |
| `orders__refunds__shipping_lines` | Shipping-line ID |
| `orders__refunds__transactions` / transaction ID | Refund-to-payment identity; join to `orders__transactions` for payment kind, status, parent and amount |
| `orders__refunds__duties` / refund-scoped source position | Duty refund amount and nullable original-duty ID; a deleted/unavailable duty does not discard the amount |

Address fields are first/last name, company, address1/2, city, province and code,
postal code (`zip`), country and code, and phone. These are order observations;
they do not establish independently refreshed customer or catalog attributes.
Position keys are zero-based within the owning source list and are replaced with
the complete parent observation. They are not durable source IDs.

The paginated source reads the expanded headers and line items, then completes
discount applications, shipping lines, agreements, refunds and transactions with
follow-up pages. Sales agreements are not GraphQL Nodes: further sales pages
resolve through the owning order's agreement-ID filter and must return exactly
that agreement. All connections require a final page and advancing cursors;
duplicate identities fail extraction. Each merged order replaces its complete
selected child sets.

Deploying this code does not enrich unchanged history. Follow the
[re-extraction procedure](../../packages/ingestion/README.md#recovery)
after release, beginning with a bounded development run. Revenue rules, fee
classification, failed-payment reversal classification and financial component
reconciliation remain transformation work; ingestion preserves their evidence.

Raw acceptance does **not** authorize a revenue-consumer migration. If a
consumer needs an excluded entity, expand the contract and remeasure it first.
