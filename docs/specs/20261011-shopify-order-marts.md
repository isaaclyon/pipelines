# Shopify order marts

Date: 2026-10-11

Status: Core business rules agreed and consolidated into decision tables and
worked cases. The first mart slice implements the first slice: `fct_orders` and
`dim_order_contacts` (source totals, `order_date`, line rollups and order
contacts) and the Cube `orders` view. The order-lines slice adds `fct_order_lines` with source
line attributes, ordered/current quantities, source money, original line values,
and parent reporting context. The commercial money work adds commercial money components, net
ordered quantities and line allocations to both marts, validated on DuckDB
fixtures; development Snowflake and real-data validation are still open.
Governed revenue measures and eligibility are not yet implemented.

[Payment evidence and fixture design](20261018-order-payment-eligibility.md)
defines the eligibility evidence boundary and synthetic acceptance examples.
It is design work, not an implemented eligibility model. The classification work owns channel and
order-characteristic classification; the commercial money work owns validated commercial money mapping.

Follow [ADR 0022](../adrs/0022-publish-marts-through-forward-only-transformation-layers.md),
[metric ownership](../adrs/0014-govern-metrics-with-cube-core.md), and the
[engineering standards](../../engineering-standards.md). This specification
consolidates the planning discussion; the status above lists what is built.
Start with [the consolidated review](#consolidated-decision-tables-and-worked-cases)
for the rules together, then the source mapping for implementation gaps.

## Outcome: yesterday's home-market D2C net revenue

### Agreed outcome and availability

First acceptance is through the development Cube `orders` view, with a separate
`data_availability` query; no dashboard or response wrapper is required. These
are implementation requirements, not features already delivered.

- Ready by an agreed morning deadline every day for the previous calendar day in
  the reporting time zone. Required source changes must reach the user-visible
  answer within 30 minutes end-to-end, not merely reach raw storage.
- Use the existing processed-at/created-at fallback and latest recorded shipping
  address. Later refunds, edits and address corrections can restate prior days.
- The home reporting market (`home_reporting_market`) includes destinations not
  listed in `separate_reporting_markets` and missing shipping country. Preserve actual geography separately. Select D2C using
  the existing classifier, exclude tests and the exact source-name marker
  `shopify_draft_order`. That marker is the agreed operational exclusion; a
  retained source sample supports it, but does not prove exhaustive draft-origin
  coverage or official API semantics.
- Return the latest validated published answer when late or incomplete. Show
  separately queried source cutoffs and limitations, without claiming they certify
  the displayed facts; do not relabel an older reporting day as yesterday,
  report missing evidence as zero, or present partial revenue as complete.
- The acceptance owner reviews dates, component totals and differences after initial results
  exist. Reference figures and tolerance are deferred to that review, not a
  prerequisite for implementation. Business acceptance remains required.

### Intraday and daily context

Use one canonical fact path and one metric definition, with distinct intraday
and daily availability contracts. Daily-only context may be joined to current
orders when its own cutoff and coverage are visible. Such an enriched answer
is not wholly 30-minute fresh. Optional context must not gate core publication;
inputs required to calculate or filter revenue correctly remain dependencies.
The net revenue outcome adds no speculative external context source or second daily fact pipeline.

Publish availability at source-dataset grain, separately from order rows. Include
source identity, publication identity, source coverage cutoff, coverage state and
known gaps, plus the applicable freshness/deadline policy. Freshness and coverage
are different: an up-to-date load can still have rejects or unresolved eligibility.
Use evidence captured with the published facts, not the latest raw load or the
maximum timestamp of an order. A recent row cannot prove a complete reporting day.
Evaluate age at query/review time; a frozen `fresh` label must not survive forever
in a cached result. dbt owns the policy; ingestion supplies evidence.

### Consistency of the two Cube queries

The current publisher builds/tests a candidate and swaps the published schema;
`_publication` stores its generation. Cube uses that identifier as a refresh key.
This invalidates caches but does **not** give two requests a shared snapshot.
The implementation now stamps `fct_orders.publication_id` during candidate
build and exposes `orders.publication_id`. `fct_data_availability` now captures
Shopify connected-window bounds and independent Loop snapshot evidence with the
same generation; `data_availability` exposes this through a separate Cube query.
Availability queries use UTC, including Cube's offset-free serialized time
values; revenue queries retain reporting-zone dates. Mixed availability and
reporting-member queries are rejected. This keeps source cutoffs unambiguous
across daylight-saving transitions.

#### Approved consumer contract: eventual consistency

The user selects eventual consistency instead of a coordinated facts/status
response. This replaces the earlier requirement for consumers to compare
publication IDs and retry mismatches. Consumers use ordinary Cube queries;
they need not request IDs, compare generations, or implement publication retries.
Independent facts and availability queries may temporarily return different
publications. Cached results are allowed; neither query promises the newest
publication. Availability describes captured pipeline coverage and source
cutoffs, not proof of the age or completeness of separately displayed facts.
Do not attach its cutoff or publication ID to a revenue result as certification.
Missing evidence and empty reporting periods must not silently become zero or a
claim of completeness. API failures remain failures.

Keep availability evidence in the same warehouse candidate as facts, rather than
reading live raw status. Preserve atomic publication, marker validation, UTC
availability timestamps, reporting-zone dates, and protection against
source-status joins multiplying order rows. Keep existing publication ID fields
for diagnostics and compatibility; clients are not required to select them.
There is no historical-generation pinning or cross-request snapshot guarantee.

The 06:00 Eastern deadline and 30-minute end-to-end freshness target remain
unchanged. Cache convergence is part of that end-to-end path. Eventual consistency
and a one-second refresh-key interval do not demonstrate either target; acceptance
must measure user-visible facts independently of the companion status query.
Live source coverage, end-to-end age, full-size timing and business acceptance
remain unverified.

#### Implementation and acceptance

The repository pairing helper and its dedicated retry tests are retired. The
Cube orders test uses ordinary revenue and availability queries without
publication dimensions, while retaining generation-bearing diagnostic checks.
The tests verify ordinary cache convergence, empty periods, invalid markers and
UTC/reporting-zone time handling. Warehouse atomicity and failed-publication checks
remain unchanged.

The metrics README and Cube member descriptions document eventual consistency.
Generated dbt exposure descriptions carry the same guidance. No mart columns,
public Cube fields or refresh keys are removed. Existing consumers can still
select publication IDs, but the supported query contract does not require them.

Validate with `scripts/platform local check metrics`. Acceptance requires
ordinary revenue and availability queries without publication dimensions, no
helper imports or required pair retries, and retained cache/marker regression
coverage. Documentation must not portray availability as certification of queried
facts. The net revenue outcome tracks validation and the remaining live/business acceptance.

No new shared client, server endpoint, dashboard, warehouse changes, deployment,
live acceptance run or freshness-policy change is part of this simplification.

### Minimal implementation slice and ownership

The net revenue outcome owns outcome integration and the remaining revenue-only payment eligibility
work using the [payment evidence contract](20261018-order-payment-eligibility.md).
Do not infer implemented eligibility from that contract.
The governed measures work remains the single owner of shared Cube measure
definitions; deliver its order-revenue subset for this outcome.
AOV, ASP, product/line reporting and additional sources are not prerequisites.

1. Reuse transaction/refund staging and the commercial money work commercial components. Implement
   complete-observation and payment-obligation evidence at order grain, including
   successful SALE/CAPTURE collections, verified failed-attempt reversal pairs,
   currency checks, and the chronology of full payment and later edits/refunds.
   Existing successful collection totals and commercial-resolution flags do not
   establish historical full payment. An unverified obligation/edit remains
   unknown; never substitute financial status or current post-refund totals.
2. Publish nullable `is_revenue_eligible` and all exclusion/unresolved reasons.
   Proven exclusions win over missing evidence without erasing those reasons.
   Tests, unsatisfied payment and gift-card-only scope are exclusions; cancellation,
   gifting, replacement and exchange are not universal revenue exclusions.
   Aggregate children before joining. Reuse classification; do not duplicate it.
3. Expose coverage for eligible, excluded and unresolved orders, including tests,
   through a separate coverage surface not subject to Cube's test-order filter.
   Source availability is not a substitute for reporting-population eligibility
   coverage. Unresolved market/channel/commercial inputs must remain visible.
4. In Cube, define USD gross merchandise sales, merchandise discounts, merchandise
   refunds, shipping, shipping refunds, and net revenue over the same explicitly
   revenue-eligible, commercially resolved population. Reuse existing canonical
   amounts: `gross_merchandise_shop_amount`, `merchandise_discount_shop_amount`,
   `merchandise_refund_shop_amount`, `shipping_shop_amount`,
   `shipping_refund_shop_amount`, and `net_revenue_shop_amount`. Reconcile net
   revenue to gross minus discounts minus merchandise refunds plus shipping minus
   shipping refunds when presenting deductions as positive amounts. The existing
   canonical discount and refund columns are signed negatives: add all five signed
   components to reconcile `net_revenue_shop_amount`; never subtract them twice.
   Exclude tax, gift-card sale value and separately reported
   handling fees. Preserve existing source-total measures unchanged.
5. Add reusable market/draft dimensions, availability and generation support;
   exercise the two queries with reporting-day boundaries, missing country,
   commercial/draft/test exclusions, partial/full refunds and paid replacements.
   Validate deterministic fixtures and development Snowflake/Cube before claiming
   acceptance. Unknown evidence can publish with coverage but cannot count as
   eligible or establish a complete answer.

The remaining technical risk is validated raw-to-obligation mapping, including
edits. The the payment evidence work logical fixtures are not evidence that this mapping exists.
Implementation can proceed conservatively, but a complete business answer cannot
be claimed until required evidence is resolved on the reporting population.

### Schedules

Refresh runs every five minutes by default (`SHOPIFY_REFRESH_CRON`), with
raw-freshness checks and the publication sensor following it. The nightly
lookback runs at 00:15 and the weekly count reconciliation at 07:00 on Sunday,
both in the reporting time zone (`ANALYTICS_REPORTING_TIME_ZONE`). Loads and
publication share an exclusive resource, so long lookbacks or reconciliations can
delay a refresh; a changed cron alone cannot guarantee freshness. Measure queue,
extraction, build, publication and Cube visibility together before claiming an
end-to-end freshness target.

## Grain and time

- `fct_orders`: one row per order, latest known state.
- `dim_order_contacts`: one row per order holding its order contact (email,
  phone, billing and shipping addresses), latest known state. Kept apart from
  `fct_orders` so contact edits and personal-data access stay separate; Cube
  presents both through one `orders` view.
- `fct_order_lines`: one row per source order line, latest known state. Repeated
  product lines remain distinct. Retain all ingested orders and lines.
- Use `processed_at` to derive `order_date` in `America/Los_Angeles`. When it is
  missing, fall back to `created_at` and preserve which timestamp supplied the
  date in `order_date_source`. Retain `created_at` separately. Validate differing
  dates against existing reporting.
- Store timestamp instants in UTC. Reporting display, filters and business-day
  grouping use the reporting time zone with daylight-saving rules.
- Later refunds restate the original order-date period. A July order refunded
  in August changes July order metrics, not August order-date activity.
- Separate future transactions and transaction lines use cashflow dates. Their
  exact grain and event-date mapping are outside this initial proof.

## Commercial amounts and eligibility

USD is the reporting currency, including international orders. Retain checkout
amounts and currency separately. Use exact decimals and explicit currency codes.

| Amount | Definition, excluding tax unless stated |
| --- | --- |
| Gross sales | Merchandise value before discounts and refunds |
| Discounts | Merchandise discounts |
| Merchandise refunds | Refunds reducing merchandise sales |
| Net sales | Gross sales minus discounts minus merchandise refunds |
| Shipping | Shipping charged after shipping discounts, before refunds |
| Shipping refunds | Refunded shipping |
| Total refunds | Merchandise refunds plus shipping refunds |
| Net shipping | Shipping minus shipping refunds |
| Net revenue | Net sales plus net shipping |
| Return handling fees | Finalized return fees retained separately for financial reconciliation; excluded from net sales, net revenue, AOV and ASP |
| Payment collected/refunded/retained | Successful payment amounts including tax and gift-card tender, before processing fees |

Retain tax charged and refunded for reconciliation. Reconcile commercial refund
components to payment refunds and expose discrepancies; do not assume every
payment adjustment maps directly to merchandise or shipping.

Return handling fees do not reduce the merchandise refund to inflate retained
merchandise revenue. Count finalized fees supported by refund/payment evidence;
a fee proposed on an open return is insufficient. For the confirmed two-unit
example, the remaining unit contributes 178.75 net sales and net revenue, with
15 reported separately as a handling fee. With otherwise eligible evidence and
zero net shipping, AOV and ASP are both 178.75. Retained payment 204.48 reconciles
to merchandise 178.75 + tax 10.73 + handling fee 15.

Use `is_revenue_eligible`, `is_aov_eligible` and `is_asp_eligible`, replacing the
proposed universal `is_reporting_order`. Keep exclusion reasons specific to each
metric. Channel classification is independent of these eligibility decisions;
a single categorical `order_type` is not required.

Preserve the agreed AOV/ASP exclusions for test, cancelled, gifted, replacement,
unpaid and partially paid orders, plus the exchange and refund rules below.
Payment must have been fully collected for those metrics; authorization alone
is insufficient. Subsequent refunds follow their own eligibility rules.

Revenue eligibility includes actual revenue from employee purchases, replacements
and exchanges. It must not inherit their AOV/ASP exclusions. Unpaid,
authorization-only and partially paid orders are revenue-ineligible until fully
paid; do not prorate revenue to cash collected. Once fully paid, recognize net
sales/net revenue on the original order date, subject to the other revenue rules.
Successful gift-card tender counts toward full payment. Later refunds reduce
revenue without reclassifying a previously fully paid order as partially paid.
This can increase historical order-date revenue when payment completes later.
An edit that adds an unpaid balance makes the whole order revenue-ineligible
until the revised obligation is fully collected. A paid $100 order edited to
$120 contributes no eligible revenue while the extra $20 is unpaid, then $120
on the original order date when paid (before other revenue adjustments). This
is a whole-order rule, not prorating revenue to collections. Later refunds do
not independently revoke prior full payment. Edited-order source mapping still
requires validation.
Test orders are revenue-ineligible. Cancellation and gifting do not independently
exclude actual revenue or refunds, subject to the full-payment rule. For example,
a fully paid order with $100 of tax-exclusive merchandise revenue that is
cancelled and receives an $80 merchandise refund contributes $20 net sales/net
revenue, while remaining AOV/ASP-ineligible. Gift-card sale value remains excluded
under the item rules below. Positive net revenue is not a universal eligibility
condition: refunds must still reduce the original channel's revenue.

- AOV is eligible net revenue divided by eligible order count. Exclude fully
  refunded orders and zero retained commercial value, including tax-only balances.
  Partial refunds reduce the numerator of otherwise eligible orders.
- ASP is summed eligible net sales divided by summed eligible
  `net_ordered_quantity`, excluding shipping and tax. This supersedes the earlier
  physically-kept-unit denominator. A quantity-based refund reduces the
  denominator even if the customer keeps the item; a goodwill amount refund
  without refunded units does not. Use the same eligible line population for
  numerator and denominator. Precise source mapping remains to be validated.
  A full amount-only refund leaves remaining net units in the ASP denominator
  with zero net sales, provided the usual ASP eligibility rules otherwise pass.
  Full monetary refund alone excludes AOV, not those remaining units from ASP.
- Exclude standalone exchange-order rows from AOV/ASP because their payment may
  represent only a price differential. Preserve financial activity separately.
  For the initial core marts, evaluate each original order under its own latest
  values and eligibility rules. Do not combine original and exchange orders or
  transfer exchange payments between them. Cross-order purchase summaries and
  exchange-adjusted AOV/ASP are deferred future work, not acceptance requirements
  for this proof. Initial metrics do not claim complete exchange-adjusted value.
- Fully refunded orders remain available for gross and refund-rate analysis.
  Metric-specific flags must not remove them from all commercial analysis.
- These order marts serve marketing/product analysis. Exclude gift-card items
  from sales and ASP; exclude gift-card-only orders from AOV. Mixed baskets use
  eligible merchandise and associated shipping without gift-card sale value.
  Retain the source rows and `is_gift_card_item`. Merchandise paid with gift-card
  tender remains eligible under ordinary rules. Future finance/cashflow transaction
  marts retain gift-card funding activity separately from redemption; redemption
  is not a second external cash receipt.
- Inventory and replacement analysis retain relevant excluded orders and use
  actual stock movement rather than commercial eligibility.

Gifting includes influencer, PR/press, corporate and employee gifting. Use the
approved tags below or the free-at-placement employee fallback. The fallback
excludes tests and replacements and uses order email with
an exact domain from the `employee_email_domains` dbt variable. Preserve classification evidence. Replacements
have separate identifying tags. Gifting discount-code rules are deferred and
do not block the initial flag definition.

## Channel and order flags

Expose `is_exchange`, `is_replacement`, `is_gifted`,
`is_partially_refunded`, `is_fully_refunded`, `is_bulk`, `is_specialty`, `is_retail`,
`is_shopify_collective`, `is_broadcast`, `is_d2c`, `is_commercial`, and `is_employee_order`
on orders and inherited order-line context. Employee status means employee email;
it is distinct from the free-order gifting inference. Employee, gifted,
replacement and exchange flags may overlap with each other and channel flags.
Commercial channel flags follow the exclusive rules below. Metric eligibility
is derived separately. Exact classification predicates remain to
be reviewed. Approved Collective/Broadcast rules and tag evidence are below.

D2C describes the sales channel and includes online employee purchases,
replacements and exchanges that do not belong to commercial channels. Those
flags do not prevent D2C classification or exclude their revenue from D2C totals;
AOV/ASP retain their own exclusions. Zero-revenue D2C activity, including a free
online replacement, retains its channel.

D2C is the residual channel: `is_d2c` is true when the retail, specialty and
bulk rules are all conclusively evaluated and none matches. No positive D2C
source predicate exists; revenue, amount and other source signals do not
establish or block D2C. This supersedes the earlier "established online D2C
activity" and positive pre-refund revenue fallback proposals (user decision,
2026-10-20). Shopify Collective and Broadcast belong to D2C when no commercial
rule matches, retaining their respective flags. Later refunds do not
remove D2C classification: an order can be D2C and fully refunded while ineligible
for AOV, and its refund still reduces D2C revenue. This supersedes the proposed
ordinary-purchase interpretation that excluded replacements and exchanges.
Gifting alone does not establish a sales channel. Missing commercial evidence is
unknown, distinct from evaluated rules with no match; apply the evidence policy
below. Draft-order identification still needs source mapping; the residual D2C
rule does not decide how draft orders are excluded from D2C reporting.

### Channel exclusivity and flag consistency

Retail, specialty and bulk are mutually exclusive and collectively exhaustive
(MECE) within commercial orders. Apply precedence **retail, then specialty, then
bulk** to the matched source evidence:

1. A POS match sets `is_retail`; `is_specialty` and `is_bulk` are false.
2. Otherwise, a specialty match sets `is_specialty`; `is_bulk` is false.
3. Otherwise, a bulk match sets `is_bulk`.
4. `is_commercial` is the OR of those resolved channel flags.

Preserve all matched tags/codes and rule evidence for explanation, including
matches that lose channel precedence. This replaces the earlier proposal that
published bulk, specialty and retail flags could overlap. D2C and commercial
are mutually exclusive; incomplete channel evidence must not silently become D2C.
Broadcast and Collective can coexist with D2C. Neither changes the commercial
hierarchy or overrides a commercial match. This supersedes the briefly proposed
exclusion of Broadcast from D2C.
Employee, gifted, replacement and exchange characteristics remain independent
of this channel precedence. Partially and fully refunded flags are mutually
exclusive under the payment-based definitions below.

Test that the number of true commercial subchannel flags equals one for every
commercial order and zero otherwise. With identical metric eligibility, dates
and currency, retail + specialty + bulk revenue must equal commercial revenue,
including refunds. This supports an additive stacked-area chart. Test POS orders
matching both specialty and bulk, and non-POS orders matching both specialty and
bulk, while preserving employee/replacement/exchange/gifting characteristics.

Future convenience segments may derive other combinations for a specific report.
Where segments must be mutually exclusive and collectively exhaustive (MECE),
declare the population and test that each order belongs to exactly one segment.
Preserve the underlying flags. A filter such as D2C excluding fully refunded
orders is a cohort, not a complete segmentation on its own.

### Flag review decisions

Brand-specific tag and discount-code rules live in `dbt_project.yml` variables,
so each business edits its own lists. Tags and codes are trimmed and lowercased,
then matched as SQL `LIKE` patterns: `%` matches any text and an exact value
matches only itself. An empty list never matches.

- **Broadcast:** `broadcast_order_tags` (default `broadcast%`). Broadcast is
  D2C when no commercial rule matches, with `is_broadcast` retained alongside it.
  Avoid broad substring patterns that match unrelated operational tags.
- **Shopify Collective:** `shopify_collective_order_tags` (default
  `shopify collective`). Collective belongs to D2C when no commercial rule
  matches; retain `is_shopify_collective` alongside the channel flag. Use
  Collective plus specific line SKUs for one-off partner analysis.
- **Commercial:** the commercial channels are bulk, specialty and retail/POS.
  Derive `is_commercial` from `is_bulk`, `is_specialty` or `is_retail`; keep
  commercial classification separate from reporting eligibility.
- **Retail:** `is_retail` is true when trimmed, lowercased `source_name = 'pos'`.
  All Shopify POS orders qualify, whatever their location. Retain order retail
  location for store/pop-up breakdowns.
- **Bulk:** `bulk_order_tags` (default `%bulk%`). Bulk evidence counts regardless
  of employee email. Resolve `is_bulk` only when neither retail nor specialty
  wins channel precedence. Reporting exclusions remain separate.
- **Specialty:** `specialty_order_tags` (default `%specialty%`, `%wholesale%`)
  or `specialty_discount_codes` (default `wholesale`). Add marketplace or
  wholesale-partner tags here. Set `is_specialty` only when retail does not win
  channel precedence. Preserve all match evidence.
- **Replacement:** `replacement_order_tags` (default `replacement`) or
  `replacement_discount_codes` (default `%replacement%`). Employee, bulk and
  specialty flags do not suppress this match. Preserve the agreed replacement
  reporting exclusions separately.
- **Exchange:** `exchange_order_tags` (default `exchange`) or
  `exchange_discount_codes` (default `%exchange%`). Other flags do not suppress
  this match; replacement and exchange can both be true. Keep the agreed AOV/ASP
  exclusions and deferred original-order linking scope.
- **Employee:** the order email, trimmed and lowercased, has a domain listed in
  `employee_email_domains` (default empty, so no order is an employee order).
  Paid and free orders both qualify; other flags remain independent. Current
  customer-profile email is a separate attribute and does not drive this flag.
  Missing or unavailable order-email evidence follows the unknown-evidence
  policy; no profile-email fallback is approved.
- **Gifted:** `is_gifted` covers the broad gifting category (influencer, PR,
  press and corporate gifting) through `gifting_order_tags`. Also apply the free-at-placement employee fallback,
  excluding tests and replacements from that inference. For this fallback,
  free-at-placement means the entire order total after discounts, including
  shipping, tax and any other charges, was exactly zero when placed (confirmed
  2026-10-28). Free merchandise with paid shipping does not qualify. Later edits
  or refunds do not establish free-at-placement value. Missing creation-time
  evidence remains unknown, not zero. The classification work uses staged
  `original_total_price_shop_amount` from Shopify `originalTotalPriceSet`, at
  extraction revision 2 or later, with the existing currency checks. Negative
  original totals are invalid evidence and remain unknown. A known disqualifying
  condition resolves the fallback to false even if another input is unknown.
  User confirmed these signals suffice for now; add no gifting discount-code
  predicates in the initial
  definition. Revisit codes when specific evidence is supplied. Explicit gifting
  tags may coexist with other characteristics; the test/replacement exclusions
  apply to the employee fallback inference. The agreed AOV/ASP gifting exclusions
  apply to this broader category; revenue eligibility is specified separately.

## Order and unit counts

Use gross measures for all source activity, including gifts, replacements and
exchanges. Source activity alone does not prove physical inventory movement.
`gross_order_count` counts source orders at order grain. Metric eligibility and
channel filters remain explicit, separate from quantity adjustments.
Default reported net order and net ordered-unit counts use revenue eligibility:
fully paid, non-test, with gift-card sale units excluded. Replacement/exchange
activity remains included when it meets those rules; AOV/ASP retain narrower
populations. Gross measures remain available before those eligibility filters.
`net_order_count` counts each order once when it has at least one net ordered
merchandise unit within the selected reporting population; gift-card units do not
qualify. Two units with one quantity-refunded unit still contribute one net order;
all merchandise units quantity-refunded contribute zero. A goodwill amount refund
does not change this count. Even a full amount-only refund can leave one net order
when net ordered merchandise units remain, while the order fails AOV eligibility.
The AOV denominator is its eligible order count, not automatically net order count.

- `gross_ordered_quantity`: ordered units before adjustments.
- `net_ordered_quantity`: units after cancellations, removals and quantity-based
  refunds, with overlapping reductions counted once. This is the default for
  “how many units were ordered?” and the agreed ASP quantity basis.
- `refunded_quantity`: units explicitly associated with line-item refunds.
  Do not infer units from an allocated goodwill refund amount.
- `returned_quantity`: physically returned units, supported by source evidence.
- `kept_quantity`: physically kept units, separate from net ordered quantity.

Two units ordered with one refunded but both kept have gross ordered quantity 2,
net ordered quantity 1, refunded quantity 1, returned quantity 0 and kept quantity
2. A goodwill amount refund with no refunded units leaves net ordered quantity
unchanged. Do not subtract refunded and returned quantities twice for the same
unit. Physical return evidence remains necessary for inventory/kept-unit reporting;
it is no longer the basis of the ASP denominator. AOV and ASP return null when
their eligible denominator is zero; display this as unavailable (for example,
an em dash), not as a zero-valued average.

## Consolidated decision tables and worked cases

These are approved business expectations for implementation fixtures, not results
from executing new models. Source-shaped fixture inputs still need to be mapped
and validated against Shopify. All flags and eligibility decisions retain their
evidence and applicable reasons.

### Channel decision table

Normalize each tag/code by trimming and lowercasing. Evaluate source matches
first, then apply the commercial precedence. Do not discard losing match evidence.

| Evidence, in resolution order | Resolved channel | Other flags |
| --- | --- | --- |
| Source name `pos`, with any specialty/bulk evidence | Retail; commercial true | Keep employee, gifting, replacement, exchange, Broadcast and Collective matches |
| No retail match; specialty evidence, with or without bulk evidence | Specialty; commercial true | Same independent characteristics remain |
| No retail/specialty match; bulk evidence | Bulk; commercial true | Same independent characteristics remain |
| Commercial rules conclusively evaluated with no match | D2C | Includes Broadcast, Collective and free replacements; keep their flags. Amount, refunds and metric eligibility do not change channel membership |
| Commercial evidence unavailable and no decisive rule | Unknown | Preserve known characteristics; do not manufacture D2C |

Employee, gifting, replacement and exchange are independent characteristics, not
additional commercial subchannels. Broadcast
and Collective do not introduce a fourth commercial channel. The commercial
subchannels must add exactly to the commercial total under identical filters.

### Metric decision table

Entries assume complete evidence and apply cumulatively: one row does not override
another exclusion. “Allowed” means the characteristic adds no exclusion; all
other payment, item and metric rules still apply. Net counts use the eligible
merchandise units and count an order once only when at least one such unit remains.

| Condition | Revenue | Default net order/unit counts | AOV | ASP |
| --- | --- | --- | --- | --- |
| Test order | Exclude | Exclude | Exclude | Exclude |
| Unpaid, authorization-only or partially paid | Exclude | Exclude | Exclude | Exclude |
| Fully paid employee purchase | Allowed | Allowed | Allowed | Allowed |
| Fully paid replacement or exchange | Allowed | Allowed | Exclude | Exclude |
| Fully paid cancelled or gifted order | Actual net revenue remains | Remaining net units determine counts | Exclude | Exclude |
| Gift-card sale line | Exclude sale value | Exclude units | Exclude gift-card-only orders; use eligible amounts for mixed baskets | Exclude line |
| Gift-card tender buying merchandise | Counts as payment | Ordinary rules | Ordinary rules | Ordinary rules |
| Partial quantity refund | Reduce sales/revenue | Reduce net units once; keep order if units remain | Reduced revenue if otherwise eligible | Reduced net sales / reduced net units |
| Goodwill amount refund without refunded units | Reduce sales/revenue | Units and net order count unchanged | Reduced revenue if otherwise eligible | Reduced net sales / unchanged net units |
| Zero retained commercial value, including tax-only retention | Retain accounting amounts | Remaining net units determine counts | Exclude | Amount-only refunds leave otherwise eligible net units included at zero net sales; zero aggregate denominator returns null |

Gross source order/unit counts remain available before eligibility filters.
Every selected population must use a consistent numerator and denominator; never
divide all revenue by AOV-eligible orders or all net sales by a differently filtered
ASP quantity. Do not average per-order AOVs or per-line ASPs.

### Refund status and unknown evidence

Refund flags use successful payment activity, including tax and gift-card tender,
before fees. Exclude identified reversals of charges from failed payment attempts
from business refund flags, together with their paired collections when assessing
previous payment. Retain both movements in payment reconciliation. Commercial
refund amounts and physical-return quantities remain separate. The table below
uses payment evidence after excluding those identified pairs:

| Payment evidence | `is_partially_refunded` | `is_fully_refunded` |
| --- | --- | --- |
| No successful refund and no previous collection | False | False |
| Collected payment with no successful refund | False | False |
| Only a failed-payment reversal, with or without a later successful purchase | False | False |
| Successful refunded amount > 0 and net payment retained > 0 | True | False |
| Previously collected payment > 0 and net payment retained = 0 after refunds | False | True |
| Required payment evidence incomplete | Unknown unless already conclusive | Unknown unless already conclusive |

Failed/pending refund attempts do not establish successful refunds. Tax-only
retention can therefore be partially refunded while AOV-ineligible. Negative
retained payment, disputes and payment reversals need separate reconciliation;
do not silently force them into fully refunded or invent a tolerance.

Establish a failed-payment reversal from transaction parent links and payment
attempt evidence. A gift-card gateway, `PAID` status, or missing refund object
alone is insufficient. Genuine post-purchase refunds to gift-card tender still
count. Ambiguous reversals retain unknown classification rather than being
silently excluded. Add a fixture with a successful gift-card charge of 50,
failed card payment, linked refund of 50 and subsequent successful purchase:
both business refund flags are false, merchandise revenue is unchanged by the
reversal, and gross payment movements still reconcile. A companion fixture with
a genuine post-purchase gift-card refund must retain the appropriate refund flag.

Unknown is distinct from false. A complete observed empty tag collection provides
no tag match; tags missing because the query never selected them provide unknown
evidence. Absent/unavailable order email does not justify an employee-domain
verdict or fallback to current customer email. A decisive known fact can still
resolve a rule: POS establishes retail despite unavailable tags, and a known test
order is metric-ineligible despite incomplete payment evidence. Unresolved
required evidence must not turn into an eligible population through null-to-false
defaults. Report classification/eligibility coverage alongside the affected
population; concrete nullability, reason columns and publication thresholds belong
to the source/model contract.

When the eligible AOV or ASP denominator is zero, return null. A nonzero numerator
with zero denominator also returns null and retains its amount for explanation;
do not coerce either value merely to produce an average.

### Worked monetary cases

Each row is an independent case. Unless stated otherwise, the order is fully paid,
non-test, online D2C, with two merchandise units at $100 each, no tax, shipping or
discounts, and complete evidence. The order has no other exclusion flags. Dollar
values below are reporting amounts after eligibility. Gross source order count
is 1 and gross ordered quantity is 2 in every row. No physical return is inferred.

| Case | Revenue | Default net units | Default net orders | AOV | ASP |
| --- | --- | --- | --- | --- | --- |
| W01: Ordinary purchase | $200 | 2 | 1 | $200 | $100 |
| W02: Same purchase paid with gift-card tender | $200 | 2 | 1 | $200 | $100 |
| W03: Paid employee purchase | $200 | 2 | 1 | $200 | $100 |
| W04: One unit refunded for $100; both goods kept | $100 | 1 | 1 | $100 | $100 |
| W05: One unit returned and refunded for $100 | $100 | 1 | 1 | $100 | $100 |
| W06: $20 goodwill refund, no refunded units | $180 | 2 | 1 | $180 | $90 |
| W07: Both units refunded for $200 | $0 | 0 | 0 | null | null |
| W08: Full $200 amount-only refund, no refunded units | $0 | 2 | 1 | null | $0 |
| W09: Only $100 collected; $100 outstanding | $0 | 0 | 0 | null | null |
| W10: W09 subsequently reaches full payment | $200 | 2 | 1 | $200 | $100 |
| W11: Replacement or exchange with the stated $200 charge | $200 | 2 | 1 | null | null |
| W12: Explicit gifting tag with the stated $200 charge | $200 | 2 | 1 | null | null |
| W13: Test order | $0 | 0 | 0 | null | null |

W09 retains source net ordered quantity 2; only its contribution to default
eligible net counts is zero. W10 changes the original order-date period, not the
payment-completion period. W04 and W05 have the same commercial quantities and
ASP but different physical-return evidence. W08 remains in the ASP denominator
at zero net sales, while failing AOV eligibility. As an aggregate check, combine
one otherwise eligible $100 one-unit order fully amount-refunded (net sales $0,
net units 1) with an unrefunded $100 one-unit order (net sales $100, net units 1):
ASP is $100 / 2 = $50, not $100. The refunded order's other ASP exclusions still
apply; a replacement or test order would remain excluded.

Additional fixture cases:

- **C01:** POS + a specialty tag + `bulk` resolves retail only; employee and replacement
  evidence survive. No approved location match is required.
- **C02:** Non-POS + a specialty tag + `bulk` resolves specialty only.
- **C03:** Non-POS + `bulk` + `shopify collective` resolves bulk/commercial, with
  Collective true and D2C false (a live bulk/Collective overlap was observed).
- **C04:** a Broadcast tag or `shopify collective`, with no commercial match,
  resolves D2C plus the corresponding characteristic.
- **C05:** Free-at-placement employee replacement remains employee + replacement;
  the employee fallback does not infer gifting. An explicit approved gifting tag
  can independently establish gifting on the same order.
- **C06:** Tags not selected cannot establish absence of specialty/bulk/gifting.
  A complete empty list with no other commercial match resolves D2C; missing
  tags leave the channel unknown.
- **M01:** $100 merchandise + $10 shipping + $11 tax, fully collected, then a
  $110 commercial refund leaving only $11 tax: revenue $0, partial-refund flag
  true, full-refund flag false, AOV null. Quantity results require refund-line data.
- **M02:** Fully paid $100 merchandise order, cancelled with an $80 merchandise
  refund: revenue $20, AOV/ASP null. Net counts depend on source quantity reductions,
  not the refunded dollar amount or cancellation flag alone.
- **M03:** Fully paid $100 merchandise + $50 gift-card sale, one unit each, no
  shipping/tax: reported revenue $100, net merchandise units 1, net orders 1,
  AOV $100, ASP $100. Preserve both source lines and the source payment of $150.
- **M04:** July order, August quantity refund: restate July revenue and units.
  Source timestamps around reporting-zone midnight and both DST changes must resolve to
  explicitly specified reporting dates.
- **A01:** Allocate $10 over eligible post-discount values $0/$0 with quantities
  2/1: $6.67/$3.33, exact conservation and quantity-fallback provenance. If total
  allocatable quantity is also zero, split equally across eligible lines. If no
  eligible lines exist, retain an explicit unallocated residual.

## Line allocations

Source line allocations take precedence. Allocate only the unallocated remainder:

| Component | Fallback weight |
| --- | --- |
| Discounts | Merchandise value before discounts |
| Refunds | Merchandise value after discounts |
| Shipping | Merchandise value after discounts |

When applicable value weights are all zero, use item quantities. When those
quantity weights are also zero, split equally across eligible lines. This fallback
requires known zero weights; missing evidence does not establish zero. If no
eligible lines exist, retain an explicit unallocated residual and mark allocation
incomplete. Do not invent a line or discard the amount.

Preserve direct source assignments. Allocate unassigned shipping and shipping
refunds only to non-gift-card merchandise lines. Keep gift-card-only residuals
separate. Record each allocation method. Use deterministic currency rounding,
with source line-ID ordering to resolve ties. Allocated amounts plus explicit
unallocated residuals must equal the resolved order component exactly. Complete
allocations have no residual. Unexplained payment differences are not resolved
commercial amounts and must never enter this allocation process.

Monetary allocation does not establish returned units. Shipping contributes to
line net revenue and remains excluded from net sales/ASP. An otherwise eligible
line with zero net ordered units retains its net sales in the ASP numerator.
Do not drop that value when other eligible lines have units. A zero aggregate
ASP denominator still returns null.

### Approved commercial money delivery design

The owner approves the full commercial money scope: commercial money mapping, net ordered
quantities, line allocations, and additive publication through the existing dbt
flow. These are design decisions, not claims of implemented or validated behavior.
No separate calculation service or general allocation framework is required.

1. Validate extraction coverage, tax basis, edits, refund components and
   cancellation evidence. Use existing evidence before proposing new
   extraction. Supply validated creation-time value evidence to the classification work early;
   current totals after refunds do not prove free-at-placement value.
2. Declare and stage the required raw children. Aggregate each child at its own
   grain before joins. Separate merchandise, discounts, shipping, tax, duties,
   finalized fees and failed-payment reversals. Preserve unexplained differences.
3. Derive net ordered quantities from validated current quantity and additional
   cancellation/removal evidence. Do not subtract refunds already reflected in
   current quantity. An order cancellation alone does not zero every line.
4. Allocate resolved components under the rules above. Preserve source amounts,
   evidence completeness, methods and reconciliation results.
5. Extend `fct_orders` and `fct_order_lines` with nullable, documented fields.
   Update their YAML contracts, the reviewed `dbt/mart-contracts.json` snapshot,
   currency checks and publication-blocking reconciliation tests. Preserve all
   source rows and existing grains. Publish only through the existing validated
   candidate boundary.

Likely changes stay in `packages/transformations/dbt/models/` (Shopify staging,
intermediate models and marts), `dbt/tests/`, `dbt/mart-contracts.json`, and the
package's fixture tests. Update the transformations README when behavior ships.
Coordinate shared mart and contract files with the classification work. The commercial money work supplies money and
quantity evidence, not a second channel classifier. The governed measures work owns governed measures.

Acceptance requires deterministic fixtures for edits, tax-inclusive prices,
mixed gift-card baskets, goodwill refunds, handling fees, failed-payment
reversals, partial cancellations and missing evidence. Add explicit value,
quantity and equal-share allocation cases, an empty eligible-line set, and
zero-unit value beside a positive-unit line. Assert exact conservation and
stable rounding independent of input row order. Execute representative fixtures
on DuckDB and explicitly selected development Snowflake. Prove that incorrect
candidate calculations cannot replace the last validated publication.

Classifier changes, governed Cube measures, physical-return tracking, production
rollout and the final historical backfill remain outside the commercial money work. Identify any
bounded re-extraction required for development validation separately under
[ADR 0021](../adrs/0021-run-data-changes-as-orchestrated-pipeline-work.md).
Approval of this design does not start a live extraction or deployment.

## Order attributes

The initial contract includes these groups; final column names, types, nullability
and source mappings must be reviewed before implementation:

- **Identity:** canonical and Shopify order/customer IDs, source order reference,
  human-readable order name/number.
- **Dates:** creation, processing, source update, cancellation, order date, and
  source observation timestamps.
- **Contact:** order email/phone, customer names, current customer email/phone,
  customer tags, and linked-customer availability. Keep order contact values
  distinct from current profile values.
- **Billing and shipping addresses:** names, company, address lines, city,
  province/state name and code, postal code, country name and code, and phone.
  Describe editable addresses as latest recorded order addresses; do not claim
  immutable checkout snapshots without evidence.
- **Context:** order tags, discount codes, source/channel, financial and
  fulfillment statuses, cancellation reason, classification flags and evidence.
- **Amounts:** the commercial/payment components above, tax reconciliation,
  original/current source totals, and corresponding checkout-currency amounts.
- **Quantities:** line count and ordered, fulfilled, outstanding, cancelled,
  returned and retained quantities where source evidence establishes them.
- **Eligibility:** revenue and AOV eligibility with metric-specific exclusion reasons;
  line-specific ASP eligibility belongs with line contracts.

Candidate enrichments for further design include order notes, selected custom
attributes/metafields, discount types/titles, replacement references, customer
account creation, landing/referring pages, UTM and affiliate attribution,
subscription context, shipping service/delivery method, fulfillment summaries,
payment gateways, gift-card amounts, outstanding balances, payment/refund dates,
closed/confirmation/return status, and allocation/reconciliation status.

Single-valued fields become columns; tags/codes remain collections. Repeated
shipments, payments and refunds belong in related detail relations with suitable
order summaries. Enrichment joins must preserve one row per order. Apply the
mart access contract to contact and address data.

## Wide order-line reporting

Publish useful parent-order context on each line: identities, dates, contact and
address information, customer and order tags, source/channel, statuses, and
classification/eligibility flags. Preserve one row per source order line.

Do not include repeated order-level monetary totals in `fct_order_lines`.
Its amounts are line amounts, including the agreed allocations when the commercial money work
implements them. The order-lines slice publishes source amounts and original line value only;
these do not establish gross sales, net sales or net revenue. Source current
quantity does not establish validated business net ordered or physically kept
quantity. The order-lines slice includes parent dates, customer identity, statuses and geography;
direct contact names, email, phone and street addresses remain in
`dim_order_contacts`. Order-level totals remain in `fct_orders`.
Count orders distinctly and define whole-order
AOV at order grain; do not average repeated order values or line averages.

Use `is_gift_card_item` to identify a gift-card item, separately from gift-card
tender used to pay for an order. Use separate `is_revenue_eligible`,
`is_aov_eligible` and `is_asp_eligible` rules with metric-specific reasons.
Inherited AOV eligibility describes the parent order; ASP eligibility is evaluated
at line grain. Revenue eligibility must respect line exclusions such as gift-card
sale value. Refunds remain part of revenue accounting even when AOV is ineligible.

## Product context

Retain as-sold SKU/title/variant attributes where available, current Shopify
catalog attributes, and external catalog attributes with clear provenance.
SKU matches the external catalog and cannot be reused for a different product
or variant. Preserve unmatched lines and reject joins that multiply facts.
External catalog integration is future work; it must not block the Shopify proof.
Product owns the curated product attributes. The field-by-field authoritative
source mapping should reflect Product's definitions while preserving both sources.

## Deferred exchange linkage

This research is retained for a future iteration and does not block core marts.

Legacy Loop Returns staging exposes `return_id`, original `shopify_order_id`,
`shopify_line_item_id` and `exchange_variant_id`. Its ingestion contract also
lands exchange lines with `return_id`, `exchange_id`, product/variant, SKU and
amounts. The inspected exchange-line projection does not expose an explicit
new Shopify order/line ID. Do not treat Loop `exchange_id` as a Shopify ID or
join returned and exchanged items many-to-many solely through `return_id`.
Shopify's native Return API exposes exchange line items, but whether the store's
Loop flows populate those relations must be verified. A bounded source-mapping
investigation should prove same-price, upsell, downsell, multi-line and repeated
exchange cases before claiming end-to-end retained-item reporting.

## Source mapping and open work

### Evidence and raw relation map

Source review on 2026-10-11 inspected the maintained
[query documents](../../packages/ingestion/src/analytics_ingestion/shopify/queries.py),
[typed raw mapping](../../packages/ingestion/src/analytics_ingestion/shopify/model.py),
and [merge resource](../../packages/ingestion/src/analytics_ingestion/shopify/source.py)
(an order read twice keeps its newest `updated_at`).
The extraction pins Admin GraphQL `2026-10`. This mapping establishes checkout
capabilities; it does not establish live warehouse coverage or app permissions.

| Source entity | Physical raw relation | Source key within shop |
| --- | --- | --- |
| Order | `orders` | `order_id` |
| Order line | `orders__lines` | `order_id`, `line_item_id` |
| Refund | `orders__refunds` | `order_id`, `refund_id` |
| Refund line | `orders__refunds__lines` | `refund_id`, `refund_line_item_id` |
| Refund adjustment | `orders__refunds__adjustments` | `refund_id`, `adjustment_id` |
| Shipping refund | `orders__refunds__shipping_lines` | `refund_id`, `refund_shipping_line_id` |
| Payment transaction | `orders__transactions` | `order_id`, `transaction_id` |

Every relation carries `shop_id` and ancestor IDs. Stage and aggregate each
child grain before combining order summaries; joining transactions directly to
refund lines would multiply amounts. Canonical mart keys must include source
and shop identity. Their exact encoding remains an implementation choice.

### Order mapping

“Available” below means selected and mapped into the raw contract. The order-detail raw revision adds
order detail/economics in extraction revision 2; see its
[field mapping and validation limits](20261009-shopify-raw-ingestion.md#revision-2-order-detail-and-economics).
Those columns remain nullable for older observations. Deployment and bounded
historical re-extraction must establish coverage before marts rely on them.

| Mart field/group | Available raw input | Gap or transformation requirement |
| --- | --- | --- |
| Order/shop/customer identity | `orders.shop_id`, `order_id`, `customer_id`, `name` | Preserve nullable customer ID. `name` is the display reference; a separate numeric order number is not selected. |
| Dates and cancellation | `created_at`, `processed_at`, `updated_at`, `cancelled_at`, `closed_at`, `cancel_reason` | Derive reporting-zone `order_date` from processing time. Parser currently requires `processed_at`; missing input becomes an ingestion exception. |
| Test, source and statuses | `test`, `source_name`, `display_financial_status`, `display_fulfillment_status` | Source name alone does not establish a full channel taxonomy. Cancellation can derive from `cancelled_at`. |
| Order email/phone and addresses | Revision 2: `email`, `phone`, flattened billing/shipping address fields | Preserve source nulls; missing selections or GraphQL access errors fail extraction. |
| Current customer names/contact/tags | Customer ID only | Add a customer extraction/refresh contract. A profile embedded in an order read is only observed with that order; independent profile edits need independent refresh evidence. |
| Tags/codes and classification | Revision 2: `orders__tags`, `orders__discount_codes`, order email | Review exact predicates before deriving gifting, replacement, exchange, bulk, specialty, commercial and D2C flags. |
| Source original/current totals | `total_price_*`, `current_total_price_*`, `subtotal_price_*`, `current_subtotal_price_*`, `total_discounts_*`, `current_total_tax_*`; revision 2 adds `original_total_price_*` and sales agreements | Retain source meanings. Do not rename `total_price` to immutable checkout value. Validate creation-time merchandise/shipping evidence for free-at-placement classification. |
| Gross sales and merchandise discounts | Line price/quantity and header discount total; revision-2 allocations, applications, tax detail and `taxes_included` | Separate merchandise from shipping discounts and gift-card sale value. Validate edits before choosing quantity and amount bases. |
| Shipping charged/discounted | Revision-2 shipping lines with original, discounted/current prices, removal flag, discounts and tax detail | Validate removed/edited shipping lines; derive net shipping after separate shipping refunds. |
| Merchandise/shipping/tax refunds | Refund-line `subtotal_*`, `total_tax_*`; shipping-refund `subtotal_amount_*`, `tax_amount_*`; adjustment `amount_*`, `tax_amount_*`; revision-2 reasons, duty amounts and refund/payment/return links | Resolve goodwill residuals and signs rather than treating every adjustment as merchandise. |
| Total commercial refunds | Derived from resolved merchandise and shipping components | Header/refund `total_refunded_*` is a reconciliation input, not the tax-exclusive commercial refund column. |
| Payment collected/refunded/retained | Transaction `kind`, `status`, `parent_transaction_id`, `gateway`, `amount_*`, timestamps | Map successful sale/capture/refund activity, excluding authorizations and failures. Validate gift-card tender and prevent parent/child double counting. Establish fully-paid-before-refund evidence, including edits and partial captures. |
| Revenue and AOV eligibility | Above inputs plus derived flags | Blockers include classification evidence, gift-card items, commercial amount reconciliation and payment-state validation. A current financial status alone does not prove the complete payment history. |
| Observation and completeness | Order `_observed_at`, `_batch_id`, `_query_version`, `_api_version`; `ingestion_batches`, `order_source_status` | Retain lineage and evaluate coverage independently of latest row time. Source deletion status must remain visible without silently discarding retained orders. |

All selected money bags map to `_shop_amount`, `_shop_currency`,
`_presentment_amount`, and `_presentment_currency` at decimal precision `(38, 9)`.
Require USD shop money for the USD reporting mapping; preserve presentment money.
Validate tax-inclusive orders before treating a source price as tax-exclusive.
Do not backsolve shipping from total minus subtotal and tax: duties, tips, fees,
edits and refund timing can make that residual misleading.

### Order-line mapping

| Mart field/group | Available raw input | Gap or transformation requirement |
| --- | --- | --- |
| Identity and parent context | `shop_id`, `order_id`, `line_item_id`; order join | Preserve line grain and copy descriptive parent context. Keep order-level monetary totals out of the line mart. |
| As-sold product context | `sku`, `name`, `product_id`, `variant_id`; revision-2 `title`, `variant_title` | Use separate titles. Validate which attributes remain historical after source edits. |
| Current Shopify product context | Product/variant IDs only | Add independently refreshed product/variant data. Preserve lines with deleted or missing catalog records. External Product-owned catalog remains deferred. |
| Gift-card sale flag | Revision-2 line `is_gift_card` | Map to `is_gift_card_item`. Do not infer from SKU/title or payment gateway. |
| Gross/net ordered quantities | `quantity`, `current_quantity`; refund-line `quantity` | Validate `current_quantity` as a candidate for net ordered quantity across cancellations, removals and quantity refunds. Do not subtract refunds again if already reflected. Neither source quantity establishes physically kept units. |
| Gross and discounts | `original_unit_price_*`, `discounted_total_*`; revision-2 allocations/application context and tax detail | Validate a tax-exclusive gross basis and all discount types. |
| Merchandise refund | Refund line linked by `line_item_id` | Aggregate direct amounts, then allocate only resolved unassigned residuals using the agreed weights. Refund quantity is distinct from physical return quantity. |
| Shipping/refunds allocated to line | Shipping refunds and revision-2 shipping charges/allocations | Allocate remaining amounts with recorded method and exact conservation. Use the approved merchandise-only fallback scope for mixed gift-card baskets. |
| Fulfilled/outstanding/cancelled/returned/retained quantities | No fulfillment or physical-return detail | Evaluate fulfillment and return sources, including Loop where required. Map completed physical events and line identity; a requested return does not prove receipt. This investigation does not require deferred cross-order exchange linking. |
| Net sales/revenue and ASP eligibility | Derived after money and net-quantity mapping close | ASP uses net ordered quantity, not physically kept quantity. Validate money and net-quantity scope together, including goodwill refunds and edited lines. |

The [LineItem reference](https://shopify.dev/docs/api/admin-graphql/2026-10/objects/LineItem)
describes `currentQuantity` as excluding refunded and removed units. It also
documents that `discountedTotalSet`, as currently queried, omits order-level
discounts and defaults to excluding code discounts. Thus existing line prices
cannot directly implement the agreed net-sales contract. Source allocations
also cover refunded/removed quantities, so their quantity scope must be tested.
The [Order reference](https://shopify.dev/docs/api/admin-graphql/2026-10/objects/Order)
distinguishes creation-time original totals from current financial fields.
The [ShippingLine reference](https://shopify.dev/docs/api/admin-graphql/2026-10/objects/ShippingLine)
is the source for revision-2 charge detail. These versioned documentation URLs
resolved to Shopify's `2026-10 latest` pages during review; no expanded query
was executed against the store during that initial review. The order-detail raw revision subsequently
validated the expanded query and raw mapping as recorded in the raw contract.

### Proposed ingestion work and closure checks

The order-detail raw revision implements the raw order-detail and commercial-money selections below,
including sales agreements and refund context. The business interpretation and
historical coverage checks remain necessary; successful extraction alone does
not close them. Independent customer/product refresh and physical returns remain
separate ingestion work.

A 2026-10-12 source-example investigation recorded seven real orders from a
separate legacy dataset, exact amounts, query provenance and concrete fixture
inputs. It confirms why payment,
commercial refund components and quantity changes need separate evidence. It
does not establish this repository's live raw coverage. In particular:

- A no-unit refund can be shipping-only, so it cannot automatically become
  merchandise goodwill.
- Refund-line subtotal plus tax can differ from the successful payment refund;
  observed discrepancy adjustments of +15 and -96.25 require explicit context
  and categorization. The legacy Loop follow-up identifies the specific +15
  case as a return handling fee, reported separately under the approved rule.
  Add supported adjustment reason/context to the maintained query, preserving
  unresolved financial meaning where necessary.
- Gift-card captures and their authorizations must not both count as payment.
- A bounded line-allocation scan found no greater-than-one-cent residual on
  fully quantity-refunded lines, but it lacked current/removal quantities and
  adjustment allocation. The approved zero-net-unit ASP rule below applies; source mapping still needs validation.

A follow-up check of a development target found that every loaded line already reflects quantity refunds
in `current_quantity`, but 25 cancelled lines retain positive current quantities.
Derive business net quantities with additional line cancellation/fulfillment
evidence; do not subtract refunds again or assume cancellation zeroes the source
field. Refund payment totals reconcile exactly to headers, while 98 orders have
differences in the candidate component/adjustment bridge. Live inspection explains
the 194.61 case as a new one-unit product line on the same order; only the original
refunded line has zero current units. Both lines already exist in development.
It is not evidence of a zero-unit order retaining merchandise revenue. Add sales
agreements, adjustment reason, refund-to-transaction identity, duty and relevant
return/fee context before asserting that bridge or resolving residual ASP scope. Completed extraction
batches do not by themselves establish this financial interpretation. Existing
legacy raw tables can support that investigation now: 89 of the 98 orders have
matching source versions and matching line-refund/adjustment/payment components
across platforms, with sales agreements exposing commercial product changes.
Nine legacy versions were older; fresh read-only Shopify inspection resolved
their differences as six price edits, one same-order product replacement and
two reversals of gift-card charges from failed payment attempts. Do not subtract
payment refunds again from prices already reduced by order edits, or treat tender
reversals as merchandise refunds. The approved refund-flag refinement excludes
identified failed-payment reversals from business refund flags while preserving
their actual payment movements for reconciliation.
Do not require new extraction merely to inspect already available legacy evidence.

Approved treatment for the confirmed handling-fee case: retain the 15 separately
from merchandise net sales and shipping. ASP remains 178.75 for the remaining
eligible unit; the fee is excluded from net revenue and AOV. Preserve evidence
that the fee was finalized, rather than merely proposed on an open return.

1. **Order and line enrichment:** add contact/addresses, tags/codes, gift-card
   flag and separate line titles. Verify protected customer data access and
   distinguish unavailable fields from legitimate nulls or empty collections.
   Confirm the exact classification rule lists against representative orders.
2. **Commercial money:** add discount allocations/application context, shipping
   charges, tax-inclusion/detail and original-value evidence. Reconcile source
   lines to headers and commercial refunds to successful payment refunds. Cover
   order/code/shipping discounts, edits, tax-inclusive prices, goodwill refunds,
   mixed gift-card baskets and international orders. Differences need explained
   components; do not allocate unexplained payment residuals as merchandise.
3. **Net and physical units:** validate gross/net ordered quantity for cancelled
   unfulfilled items, removed lines, quantity refunds with goods kept and goodwill
   amount refunds. Prove no double subtraction when source current quantity already
   reflects refunds/removals. Separately prove physical-return evidence, including
   a return without a refund, for returned/kept-unit and inventory reporting.
   Physical-return coverage does not gate the revised ASP definition; net-quantity
   source validation does. Fulfillment status alone is insufficient inventory evidence.
4. **Current enrichments:** give customer and product refreshes their own keys,
   observation times and freshness contracts. A customer/catalog edit with no
   order update must refresh current attributes without changing as-sold values.
5. **Query expansion and historical coverage:** update bulk and point documents,
   parsers, raw columns, schema fixtures, extraction revision, query version and raw-contract docs
   together. Test every added child connection beyond its first page and test
   complete child replacement after a source edit.

Query expansion needs deliberate historical re-observation. Retained old files
cannot supply fields never selected, and ordinary updated-order refresh will
not revisit every unchanged historical order. Ordinary historical backfill
preserves equal-timestamp differing content. The extraction revision work now provides
[bounded historical re-extraction](../../packages/ingestion/README.md#recovery)
with ordered extraction revisions and guards against older-revision replay.
Use that operation for enrichment: bump the extraction revision when changing
the query/parser contract, declare the bounded source windows, and verify the
expanded fields and child collections survive replay. Record coverage by
extraction revision and query version before admitting enriched history to the
published contract. Coordinate that data operation under
[ADR 0021](../adrs/0021-run-data-changes-as-orchestrated-pipeline-work.md).

Closure requires deterministic fixtures for these cases, an explicitly selected
development API check of the expanded query and permissions, and validation of
the resulting raw shape on both destinations. API documentation alone does not
prove store population, complete history or source freshness. Exchange linking,
external catalog integration and cashflow marts remain deferred.

Remaining design includes exact source mapping for approved rules, current-customer
freshness, source evidence for kept units, edited-order balances, goodwill refund
mapping, FX/refund differences, disputes/over-refunds, source allocation scope after
edits or returns, and SKU normalization/duplicates. The approved allocation rules
resolve zero weights, an empty eligible-line set and zero-net-unit value in ASP.
Their source mapping still needs validation. Do not silently reuse AOV's refund
exclusions or drop a residual to make amounts reconcile. Zero aggregate denominator
behavior remains null.

## Acceptance examples

- Unpaid → authorized → partially paid → fully paid changes eligibility only
  at full collection while preserving original order-date attribution.
- Partial refund, full refund, tax-only retention and gift-card payment exercise
  distinct payment and commercial eligibility rules.
- A cross-month refund restates the original order period; reporting-zone midnight and
  both daylight-saving transitions have explicit expected results.
- Source, proportional and quantity-fallback allocations conserve order totals;
  goodwill refunds do not fabricate returned units.
- Two eligible $100 units with a $100 one-unit refund and both items kept yield
  net sales $100, net ordered quantity 1 and ASP $100. A $20 goodwill refund with
  no refunded units yields net sales $180, net ordered quantity 2 and ASP $90.
  A returned-and-refunded unit is not deducted twice from net ordered quantity.
- Missing catalog matches retain lines; duplicate matches cannot multiply money.
- Candidate validation failure leaves the last validated mart available with
  visible freshness failures. No previous publication means unavailable output.
- Execute representative fixtures on DuckDB and development Snowflake and verify
  a Cube query over published marts.
