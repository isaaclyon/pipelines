# Order payment evidence and eligibility

Date: 2026-10-18

Status: Design contract and deterministic fixture examples. Transaction/refund
source declarations, staging models and raw-fixture staging tests are implemented.
The net revenue outcome implements conservative payment history, revenue-only eligibility and
published fields with offline fixtures. AOV remains unimplemented. Development
adapter/source validation and outcome acceptance remain open; fixture success
is not live acceptance.


## Governing rules

The [order marts specification](20261011-shopify-order-marts.md) and
[glossary](../../GLOSSARY.md) define the business rules. This document defines
the evidence boundary and examples for those rules. It does not replace them.
Follow [ADR 0016](../adrs/0016-parse-data-at-trust-boundaries.md),
[ADR 0015](../adrs/0015-support-duckdb-and-snowflake.md), and
[ADR 0022](../adrs/0022-publish-marts-through-forward-only-transformation-layers.md).

Revenue and AOV have separate nullable eligibility booleans. True means eligible;
false means a proven exclusion; null means unresolved evidence without a proven
exclusion. Preserve all proven exclusions and unresolved requirements for each
metric. A proven exclusion resolves the flag to false even when other evidence
is missing. Do not discard the remaining evidence requirements.

Keep all source orders at `(shopify_shop_id, shopify_order_id)` grain. Later
payments and refunds retain the original `order_date`. Unresolved eligibility
can be published with queryable coverage. Consumers select explicit true values
for the relevant metric; a universal eligibility filter is incorrect.

## Ownership and model boundaries

The payment evidence work owns transaction/refund staging, payment evidence, business refund flags,
and revenue/AOV eligibility. The commercial money work owns the validated money mapping needed to
establish payment obligations and retained commercial value. The classification work owns the
classification flags consumed by eligibility. Shared classification logic must
live upstream of the mart; intermediate eligibility models cannot read
`fct_orders` to obtain it.

The intended flow is raw transactions/refunds → staging → order payment evidence
→ order eligibility → `fct_orders`. Classification and commercial evidence join
at order grain. Aggregate each child before joining to avoid multiplying money.
Physical model and column names are assigned when those models are implemented;
the logical inputs below are not existing published columns.

The Cube `orders` view exposes eligibility and appropriate reasons without
changing existing source-total measures. Keep its existing test-order filter.
Coverage needs a separate published path that includes test orders, so Cube's
order filter cannot silently change coverage totals. Coverage counts eligible,
excluded, and unresolved orders separately for each metric and summarizes
unresolved requirements. Per-requirement counts can overlap and must not be
summed as distinct unresolved orders.

## Existing source evidence

The following paths are the current implementation, rather than proposed models:

| Source | Existing evidence | Limit |
| --- | --- | --- |
| [GraphQL documents](../../packages/ingestion/src/analytics_ingestion/shopify/queries.py) | Transaction ID, parent ID, times, kind, status, gateway, test flag, error code and money; refund transaction links; agreements and sales | No validated reconstruction of historical full-payment obligations |
| [Raw row contract](../../packages/ingestion/src/analytics_ingestion/shopify/model.py) | `orders__transactions`, `orders__refunds`, `orders__refunds__transactions`; explicit shop/order IDs; decimal money | A source-shaped transaction is not a business eligibility decision |
| Same raw row contract | `transactions_count`, `transactions_count_precision`, `_query_version`, `_extraction_revision`, `_observed_at` | Current parser requires exact transaction counts and rejects incomplete observations; old retained observations still need coverage evaluation |
| [dbt source declarations](../../packages/transformations/dbt/models/staging/shopify_orders/_shopify_orders__sources.yml) | Orders, order lines, transactions, refunds, refund-transaction links and ingestion batches | Payment staging preserves native amounts, UTC instants and links; it does not establish payment completeness or eligibility |
| [Current order fact](../../packages/transformations/dbt/models/marts/shopify_orders/fct_orders.sql) | Order status, test/cancellation evidence and source totals | No payment-history or classification inputs |

The [raw field contract](20261009-shopify-raw-ingestion.md) records
physical tables and revision coverage. Real payment/refund examples show
patterns, not complete eligibility verdicts.
Do not assume deployed raw history matches the current query merely because
source code selects a field.

## Logical input contract

### Observation and transactions

Each order needs a complete-observation verdict tied to its source observation,
query version, extraction revision, and applicable source status. Missing or
incompatible coverage produces unknown evidence; it does not produce an empty
transaction history. A verified empty history is a distinct input.

Transactions retain their source identity, parent identity, timestamps, kind,
status, test flag, error code, gateway, both exact money bags, and currency.
Transaction identities are unique within the order. Refund-to-transaction links
reference those identities. A missing required link remains unresolved.

Normalize enum case in staging. Successful `SALE` and `CAPTURE` movements supply
collection evidence. `AUTHORIZATION` supplies no collection, even when it has a
successful status. Failed or pending movements supply no successful collection
or refund. A capture and its authorization contribute the capture amount once.
Gift-card tender counts like other successful tender; a gift-card sale item is a
separate commercial exclusion.

Preserve successful gross collections and refunds for payment reconciliation.
Business payment/refund amounts exclude only positively identified failed-attempt
charge/reversal pairs. Parent links and payment-attempt evidence must establish
such a pair. Gateway, `PAID` status, or a missing refund object is insufficient.
Ambiguous pairs remain unresolved; genuine post-purchase gift-card refunds count.

### Payment obligations: the commercial money work boundary

Successful collection is necessary but does not prove full payment. The evidence
model needs the amount due at the applicable obligation, its effective time,
currency, completeness, and source references. It also needs any later edit that
changes the amount due. All amounts use exact decimals.

A fixture can supply a verified obligation directly. Production must reconstruct
or obtain that obligation through the validated the commercial money work mapping. Do not substitute
`financial_status`, a positive capture, current post-refund totals, or an assumed
creation-time total for missing obligation evidence.

Preserve evidence that the order reaches full payment before subsequent refunds.
A refund does not turn that history into partial payment. The net revenue outcome resolves the
edit policy: an additional unpaid balance makes the whole order ineligible until
the revised obligation is fully collected; do not prorate revenue to cash
collected. For a fully paid $100 order edited to $120, eligibility is false while
the added $20 remains unpaid and true once it is collected, subject to other
exclusions. Preserve the original order date. Mark the payment verdict unknown
when source evidence cannot resolve that revised obligation or payment history;
financial status is not a shortcut.

The net revenue outcome additionally confirms that a verified credit can reduce the remaining
obligation before an order ever reaches full payment. Distinguish a noncash
credit from a cash refund. For $100 due and $50 collected, a verified $50 credit
with no cash returned leaves a fully covered $50 obligation. Returning that $50
in cash leaves the $50 obligation unpaid. Evaluate the refund agreement and its
linked cash together, not as a temporary credit followed by a later cash event.

### Commercial and classification inputs

The commercial money work supplies resolved gift-card sale exclusions and retained commercial value
for merchandise and shipping, excluding tax and handling fees. Missing component
mapping remains unknown, even when net customer payment retained is positive.
AOV needs positive retained commercial value; revenue eligibility has no universal
positive-value requirement.

The classification work supplies nullable gifting, replacement, and exchange flags with completeness
and match evidence. Employee status alone does not exclude a paid purchase.
Unknown classification can leave AOV unresolved but does not by itself exclude
revenue. Channel membership is not an eligibility gate.

## Derived evidence and decisions

Logical order-level outputs include successful gross collection/refund amounts,
resolved business collection/refund amounts, net customer payment retained,
full-payment evidence, partial/full-refund flags, and unresolved requirements.
Amounts carry their currency and source observation. Unknown amounts remain null,
not zero. Refund evidence includes tax and gift-card tender, before fees.

With complete resolved business payment evidence, derive refund flags as follows:

| Evidence | Partially refunded | Fully refunded |
| --- | --- | --- |
| No previous collection and no successful refund | False | False |
| Previous collection and no successful refund | False | False |
| Positive refund and positive net payment retained | True | False |
| Previous collection and zero net payment retained after refunds | False | True |
| Negative retained payment or ambiguous reversal evidence | Null unless independently conclusive | Null unless independently conclusive |

Do not invent a rounding tolerance or silently clamp negative balances. Unknown
required evidence cannot prove no refunds. The two business refund flags must
never both be true.

Revenue eligibility excludes tests, unresolved or unsatisfied required payment
evidence, and gift-card-only commercial scope. Unsatisfied payment produces false;
unresolved payment produces null unless another proven exclusion applies.
Cancellation, gifting, replacement, and exchange do not independently exclude
actual revenue or refund accounting.

AOV additionally excludes cancelled, gifted, replacement, exchange, fully refunded,
gift-card-only, and zero/nonpositive retained commercial-value orders. Partial
refunds reduce commercial value without independently excluding AOV. A fully paid
employee purchase passes when all other AOV requirements pass.

Retain all proven exclusion reasons and all unresolved requirements per metric.
Use stable reason identifiers with documented meanings. The fixture vocabulary
below separates `payment_not_fully_collected` from `payment_evidence_incomplete`,
`payment_obligation_unknown`, `payment_edit_unresolved`,
`refund_reversal_unresolved`, `payment_balance_unresolved`,
`classification_evidence_incomplete`, and `commercial_value_unknown`.
Reason storage must preserve all reasons on both supported adapters; its physical
representation is an implementation choice, not a source assumption.

## Deterministic fixture examples

[The fixture data](fixtures/payment-evidence.json) contains complete logical
inputs and explicit expected verdicts. These are design examples, not registered
dbt unit tests or evidence of a passing model implementation. All IDs are
synthetic. Money uses decimal strings in USD; null means unknown, not zero.

The fixture envelope uses `observation_complete`, `obligation_amount`,
`obligation_verified`, `edit_unresolved`, and `refund_classification_complete`.
`confirmed_reversal_transaction_ids` identifies both movements of a pair already
proven to reverse a failed attempt. It is not a raw Shopify field. The raw-to-pair
classifier needs separate tests proving its parent/attempt evidence; these
examples must not be used to bypass that work.

`commercial_value` and `has_merchandise` are resolved the commercial money work inputs for the example.
Classification booleans are resolved the classification work inputs. Expected output names describe
the contract, not a claim that those columns already exist.

### Collection and timing cases

| Cases | Required result |
| --- | --- |
| `unpaid`, `authorized`, `partially_paid`, `fully_paid` | Payment verdict changes only at full collection; all share the same order and original date |
| `gift_card_capture`, `failed_or_pending`, `split_capture` | Count successful captures/sales only; never count authorization twice |
| `obligation_unknown`, `observation_unknown`, `edit_unresolved` | Preserve unknown payment rather than using current status or assumed totals |
| `duplicate_transaction`, `currency_mismatch` | Reject invalid evidence through blocking correctness checks |

### Refund and classification cases

| Cases | Required result |
| --- | --- |
| `partial_refund`, `full_refund`, `tax_only_retained` | Preserve full-payment history; separate business refund flags from AOV retained value |
| `failed_attempt_reversal`, `genuine_gift_card_refund`, `ambiguous_reversal` | Remove only established failed-attempt pairs from business flags; retain gross movements |
| `negative_retained`, `commercial_value_unknown` | Retain unresolved evidence rather than inventing commercial components or balance tolerances |
| `test_unknown`, `cancelled_unknown`, `classification_unknown` | Proven exclusions win for the affected metric; preserve every unresolved requirement |
| `cancelled_paid`, `gifted_paid`, `replacement_paid`, `exchange_paid`, `employee_paid`, `gift_card_only`, `multiple_exclusions` | Apply metric-specific rules and retain all proven exclusions |

The full-refund example occurs in the following month and keeps the original
order date. Header status is deliberately not an input to the logical payment
decision. A raw mapping test must also hold transaction/obligation evidence fixed
while changing header status; the verdict must not change from that alone.

## Implementation and acceptance sequence

The staging portion of step 1 is implemented in
[`_shopify_orders__payments.yml`](../../packages/transformations/dbt/models/staging/shopify_orders/_shopify_orders__payments.yml)
and the three associated SQL models. Executable raw-fixture tests cover enum
normalization, UTC conversion, decimal precision, nullable evidence, empty
sources, shop-scoped keys, and duplicate/null-key rejection. The 30 cross-metric eligibility
examples above remain design fixtures; the net revenue outcome adds executable revenue-only
chronology and decision tests in the transformations package. Observation lineage and completeness
must be resolved in the later evidence model; child records have no independent
observation timestamp. Staging does not aggregate, filter or infer settlement.

1. Declare and stage transaction/refund sources with identity, currency, UTC
   timestamps, observation lineage, and completeness checks. Start model behavior
   work with meaningful failing tests using the existing container test path.
2. Implement payment evidence against verified synthetic obligations. Add raw
   parent/attempt tests for reversal identification. Keep unresolved source mappings
   explicit until the commercial money work validates them; do not publish assumed full-payment facts.
3. Connect the commercial money work commercial evidence and the classification work upstream classification models.
   Add executable eligibility tests from these cases and update the reviewed mart
   contract only when the actual output matches it.
4. Add publication failure fixtures for duplicate transaction identities,
   cross-order links, currency mismatch, contradictory verdicts, and dropped or
   multiplied orders. Expected unresolved evidence remains publishable.
5. Execute DuckDB fixtures and explicit development Snowflake parity, then test
   Cube eligibility queries and coverage from the same published generation.
   Cover repeat observations, changed payment/refund state, reason completeness,
   unchanged order date, and preservation of the previous publication on failure.

The payment evidence work remains blocked for delivery by the commercial money
and classification work. Staging, synthetic evidence
fixtures, and the payment-evidence model can proceed independently. No live
validation, history re-extraction, deployment, or governed metric calculation is
part of defining this contract.
