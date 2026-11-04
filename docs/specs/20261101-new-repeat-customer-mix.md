# Yesterday's new versus repeat home-market D2C customer mix

## Status and outcome

This outcome asks: **What was our new versus repeat customer mix in home-market
D2C yesterday?** A named business owner accepts the result.
This specification records user decisions from 2026-11-01. It is design only: it
does not establish implemented models, loaded history, live availability or
business acceptance. Implementation scope requires separate approval, and live
actions need their own authority.

Use the [glossary](../../GLOSSARY.md), the
[order-marts contract](20261011-shopify-order-marts.md) and the
[payment eligibility contract](20261018-order-payment-eligibility.md). This
specification adds a customer-day classification; it does not change any
existing metric, eligibility rule or channel definition.

## Definitions

### Identity

A customer is the Shopify customer ID within its shop, as recorded on the order
(`fct_orders.shopify_shop_id`, `fct_orders.shopify_customer_id`). This is a
source identity, not a stitched analytical identity.

- An order without a customer ID is **unidentified**. Count such orders
  separately. Never treat them as distinct customers or add them to the buyer
  denominator.
- Use each order's latest observed customer ID. If Shopify changes the customer
  on an order and the change is observed, results restate on that evidence.
  There is no email, phone or address matching, no merge detection and no
  guarantee that unobserved identity changes are reflected.
- Do not read customer profiles, `dim_customers` or Shopify lifetime counters
  (`shopify_order_count`, lifetime spend, last order, RFM group). They describe
  the latest profile read, not the state on an order date, and they publish in
  a separate consistency group
  ([ADR 0027](../adrs/0027-isolate-mart-publication-by-consistency-group.md)).

### Qualifying purchase

A qualifying purchase is an order whose existing AOV eligibility
(`fct_orders.is_aov_eligible`) is true. Use the implemented rule as is,
including its three-valued result: a decisive exclusion is false even when other
evidence is unknown; otherwise any unknown input is null.

| Result | Orders |
| --- | --- |
| False (excluded) | Test; gift-card-only; current obligation not fully collected (unpaid, authorization-only, partially paid, or a paid order edited upward with an unpaid balance, until fully collected); cancelled; gifted; replacement; exchange; fully refunded; net revenue zero or below (including only tax remaining) |
| True when evidence is complete | Partially refunded orders with positive retained value; paid employee purchases; orders mixing gift cards and merchandise; draft-origin orders; retail/POS, specialty and bulk orders; orders in any market |
| Null (unknown) | Extraction revision below 2 (gifting, replacement or exchange unknown) or below 3 (payment evidence incomplete); unverified obligation; unresolved payment chronology, such as pending refunds; unresolved commercial value or refund status |

AOV eligibility embeds no market, channel or draft filter. That matches the
all-market, all-channel prior history below. Changes to the AOV rule
change this outcome; record any such change here.

### Reporting day and population

Yesterday is the previous `America/Los_Angeles` calendar day, including
daylight-saving changes. Order dates use the existing order-date basis:
`processed_at`, with `created_at` fallback.

The **daily population** for reporting day *D* is every identified customer
(shop, customer ID) with at least one order on *D* that is:

- AOV-eligible (true);
- D2C (`is_d2c` true);
- in the home reporting market (`home_reporting_market`: any shipping country
  not listed in `separate_reporting_markets`, or a missing one);
- not a test order; and
- not from the exact draft source marker `shopify_draft_order`.

Each customer counts once per day, however many orders they place.

### Prior history

Prior history for customer *c* on day *D* is every order for the same shop and
customer ID with an order date before *D*. It covers all time, all markets, all
channels and all source names. The home-market D2C and draft filters apply only to the
daily population, never to prior history. An AOV-true draft-origin, commercial,
other-market or employee order therefore counts as a prior qualifying purchase.

Orders on *D* itself never make a customer repeat. A customer whose first
qualifying day is *D* is new on *D*, including when they place several orders
that day or a qualifying order in another market or channel on *D*.

### Customer-day verdict

For each customer in the daily population:

| Verdict | Rule |
| --- | --- |
| **Repeat** | At least one prior order is AOV-true. This takes precedence and holds even when other history is missing, unavailable or unresolved. |
| **New** | No prior order is AOV-true, every loaded prior order is AOV-false, and negative-history coverage is proven for this customer and day. |
| **Unknown** | No prior order is known to be AOV-true, and either a prior order's eligibility is unresolved or negative-history coverage is not proven. |

An AOV-false prior order never blocks a new verdict, so do not require revision-3
evidence for an order that is already decisively excluded (for example, a
cancelled or test order). Record why each unknown verdict is unknown, at least
separating **unresolved prior eligibility** from **unproven history coverage**.

### Negative-history coverage

A new verdict needs evidence that no unloaded order could change it. The evidence
must establish that the shop's order population is complete for every order that
could have an order date before *D*. That covers all creation instants from the
shop's first order through a recorded source coverage cutoff. Imported orders can
have a `processed_at` earlier than `created_at`, so coverage is defined by
creation, not by order date.

- Ingestion supplies the evidence and dbt owns the policy (see
  [engineering standards](../../engineering-standards.md#freshness-ownership)).
- Connected `updated_at` windows (`int_shopify_orders__window_coverage`) do not
  establish an initial population and are insufficient alone.
- The weekly per-creation-month count reconciliation in ingestion is a candidate
  input. A deletion and a gap in the same month cancel in the counts, and its
  result is not yet modeled in dbt, so it is not proof by itself.
- An order marked `unavailable` in `order_source_status` is accounted for, not
  irrelevant. The mark records that the source stopped returning the order; it
  does not establish the order's customer, order date or AOV result.
- Present population coverage may not be proven. Verified creation-window
  continuity can stop short of today, with partial later history and revision-3
  evidence for only part of the range. Until history coverage work delivers
  the coverage below, expect **no provable new verdicts**: identified buyers are repeat or
  unknown. The user accepts this partial report.

Negative-history proof for customer *c* on day *D* is resolved only when every
order that could be a prior qualifying purchase of *c* is either loaded with
decisive evidence (AOV-false) or ruled out by independent evidence. An order is
potentially relevant while its customer, its order date before *D* or its AOV
result is unknown. That includes an order that is missing, marked unavailable,
or loaded without the evidence needed to settle those facts. Unless a known
prior AOV-true order already proves repeat, any such order keeps the verdict
unknown, with the reason **unproven history coverage**, for every customer it
could belong to. Repeat takes precedence over unresolved history. Independent evidence means a retained
observation or source record that establishes the fact, for example a different
customer ID, an order date on or after *D*, or a decisive exclusion such as a
test or cancelled order.

The history coverage dependency is:

1. Population proof: every shop order from the first order through the coverage
   cutoff is accounted for, either loaded or explicitly marked unavailable. The
   unavailable mark accounts for an order but does not resolve it (see above).
2. Customer ID, creation and processing times, test flag and cancellation for
   every order, including retained evidence for orders later marked unavailable,
   so that relevance can be ruled out rather than assumed.
3. Every order whose AOV result is null and could become true has the evidence
   to resolve it, currently extraction revision 3 or later with resolved payment
   and commercial evidence. Decisively excluded orders need no re-extraction.

## Reported answer

For the reporting day, report:

- counts of new, repeat and unknown customers, and the total population;
- the **new**, **repeat** and **unknown** shares over all customers in the
  population, summing to 100% (null when the population is empty, never 0%);
- unknown counts by reason;
- separately, the count of **unidentified qualifying orders**: orders that meet
  every population condition except having a customer ID; and
- separately, as coverage disclosure, the count of identified customers whose
  in-scope orders on *D* are only eligibility-unknown, and of orders on *D* whose
  D2C classification is unknown. These are not in the denominator. The reporting
  market is never unknown: a missing shipping country is in the home-market
  bucket, as approved, so there is no unknown-market count.

There is no maximum unknown share; a partial report is usable when it carries the
warnings below. Unknown is never reported as new, and a missing result is never
reported as zero.

**The population can be incomplete, and no share bounds the full-day mix.**
The population, and therefore every share's denominator, covers only identified
buyers whose orders on *D* are loaded and resolved as qualifying at publication.
When eligibility, classification, identity or source coverage for *D* is
unknown, buyers can be missing from it. A share can then move in either
direction. For example, one observed repeat buyer shows a 100% repeat share;
if nine omitted buyers are new, the full-day repeat share is 10%. Show this
incompleteness prominently with every answer whenever the companion counts or
coverage state are not clean.

No reported count or share is a bound on the full day or on later publications.
Later refunds, cancellations, observed customer ID changes and other
latest-state revisions can remove buyers or change verdicts, including repeat.
The only monotonic statement is narrow: within one publication's fixed
population and fixed order evidence, resolving an unknown verdict can only make
it new or repeat. Do not label any figure a minimum or maximum. Keep the
unidentified qualifying orders and the unresolved companion counts separate from
the population.

The primary measure counts customers. Order and revenue mixes are out of scope.

### Restatement

Classify from the latest evidence at publication time. Past days restate when:

- eligibility changes, such as later refunds, cancellations, payments, edits or
  tags. A buyer can leave the population, and when a first qualifying order loses
  eligibility a later order becomes the first;
- an observed customer ID changes on an order;
- history loads or coverage proof changes, which can turn unknown into new or
  repeat; or
- address corrections move an order into or out of the home reporting market.

No point-in-time snapshot of earlier answers is kept.

## Availability

The owner chose these terms explicitly for this outcome.

- On-demand access through **development Cube**. No scheduled answer or deadline
  is guaranteed, and no dashboard is required.
- Shopify freshness is **best effort**, with no numeric source-to-answer guarantee.
- Show source coverage cutoffs and the negative-history coverage state with the
  answer. A partial answer carries a clear partial warning, the unknown share and
  its reasons, and the unidentified order count. Never claim it is complete.
- Use the existing orders-group availability evidence (`data_availability`). It
  describes captured coverage and does not certify the displayed facts
  (eventual consistency, see the order-marts contract).
- API failures remain errors, not empty results. Later corrections restate past
  days as above.

These decisions resolve the availability-design gate in the
[delivery policy](../delivery-policy.md#availability-for-every-question). They do
not establish measured availability.

## Implementation direction

The direction follows
[ADR 0022](../adrs/0022-publish-marts-through-forward-only-transformation-layers.md),
[ADR 0027](../adrs/0027-isolate-mart-publication-by-consistency-group.md),
[ADR 0014](../adrs/0014-govern-metrics-with-cube-core.md) and
[ADR 0025](../adrs/0025-use-duckdb-for-deterministic-tests.md).
Implementation needs a separate bounded approval.

1. **dbt, orders group:** derive the customer-day verdicts from order history
   only, in the orders consistency group, so the answer shares a version with
   orders and availability. Add negative-history coverage policy and evidence
   models. Every new output column needs a description and an explicit privacy
   classification. Customer IDs are already classified `pii: false`. No email,
   phone or address enters these outputs. Names and grain are chosen in
   implementation; they are not published contracts yet.
2. **Cube:** expose the customer counts, shares, reasons and companion counts
   through ordinary queries over the published mart. Coordinate with any
   concurrent work on `orders.js`, the shared metrics tests, generated exposures
   and governed measures rather than editing those files at the same time.
3. **History coverage:** deliver population proof and the needed history as
   separate work, under its own approval. This outcome adds no backfill, schedule or load.
4. **Live acceptance:** separately authorized development publication and Cube
   queries, then results for the acceptance owner.

Additive columns must be verifiably published before consumers use them.

## Acceptance

The owner defers reference figures, comparison dates, the reference source and the
tolerance until separately authorized initial live results. Record the
environment, observation cutoffs, coverage state and explained differences then.
Only the owner's explicit acceptance completes the outcome.

Offline checks must cover:

- one count per customer per day with several orders, including a qualifying
  order in another market or channel on the same day;
- repeat proven by a prior AOV-true order in another market, a commercial
  channel, a draft order or an employee purchase, despite other unresolved,
  missing or unavailable history that could belong to the customer;
- a new verdict only with proven coverage and all prior orders AOV-false,
  including decisively excluded orders below revision 3;
- unknown, absent a known prior AOV-true order, for an unresolved prior order,
  and separately for unproven coverage;
- for a customer with no known prior AOV-true order: unknown, not new, when a
  prior order marked unavailable or missing customer, date or AOV evidence could
  belong to the customer, and new only when independent evidence rules that
  order out;
- no figure labeled as a bound, and an incomplete-population warning when
  companion counts or coverage are not clean;
- unidentified qualifying orders counted outside the customer denominator;
- exclusion from the population of test, draft-marker, commercial, Canada,
  non-AOV-eligible and eligibility-unknown orders, with the companion counts;
- Pacific day boundaries, including daylight-saving changes, and the
  `created_at` fallback;
- `processed_at` before `created_at` for imported orders;
- restatement after a refund, a cancellation or a full collection, after an
  observed customer ID change, and after loaded history or new coverage;
- the same customer ID in two shops kept distinct; and
- an empty population that returns null shares and an explicit status, not zero.

Run relevant portable transformations on both adapters when authorized. Owning
container checks, independent review and merged code show technical progress,
not live acceptance.

## Related outcomes

Customer acquisition cost needs an acquired-customer definition. This
new-customer definition is a candidate for reuse there, subject to its own
approval; it is not imposed. Lifetime value, time to repeat and repeat rate may
reuse the identity, qualifying-purchase and coverage work under
their own decisions.

## Provenance

User decisions from the 2026-11-01 detail pass:

- customer mix by distinct identified buyers, counted once per day;
- Shopify customer ID per shop, with missing IDs counted as separate orders;
- all-time, all-market and all-channel prior history;
- AOV eligibility as the qualifying purchase, with the edge cases above
  approved, including draft-origin, commercial and paid employee purchases;
- repeat proven by any known earlier qualifying order, and new only with
  negative-history proof;
- shares over all identified qualifying buyers, with no maximum unknown share;
- restatement on latest evidence without merge detection;
- the on-demand development, best-effort availability terms; and
- a named acceptance owner, with reference figures deferred.

It does not change the glossary or existing metric definitions.
