# Analytics Platform glossary

Shared meanings for platform requirements and analytical data contracts.

## Language

**Mart**:
A published business-facing dataset with documented meaning, row grain, and keys.
A mart may retain detailed records or contain aggregates.

**Publication group**:
The smallest set of marts whose outputs must share one validated version for a
business answer to be consistent. Unrelated groups can refresh independently.

**Row grain**:
What one row represents in a dataset, including the distinctions needed to tell
two legitimate records apart.

**Model declaration**:
The documented purpose, owner, row grain, and keys that every transformation
model states, or its reason for having no unique key. It describes a model
without promising consumers that it will stay stable.
_Avoid_: Model contract

**Mart contract**:
The published, compatibility-checked interface of a mart: its relation, columns,
types, row grain, keys, and required checks. Consumers may depend on it.
_Avoid_: Model contract, contract (unqualified)

**Source identity**:
An entity's identity within a named source, including the scope needed to make
it unique. A Shopify order's source identity includes its shop.

**Reject**:
A version of a source record that failed parsing at ingestion, recorded
separately instead of loaded. It stays open until a version at least as new
loads, or until reconciliation confirms the record deleted at source after it.
_Avoid_: Exception, error record

**Analytical identity**:
An identity a mart defines for a business entity. Its relationship to source
identities is explicit; combining records from multiple sources requires a
specified mapping.

**Order contact**:
The email, phone, and billing and shipping addresses most recently recorded on
an order. Editable after checkout, so it is not a checkout snapshot.
_Avoid_: Customer details, customer contact

**Customer profile**:
A customer's current details in Shopify, which can change without any change
to their orders.
_Avoid_: Customer record, order contact

**SKU**:
A merchandise identifier used to match Shopify data with the external catalog.
A SKU cannot be reused for a different product or variant.

**Governed metric**:
A shared measure with an agreed definition, units, aggregation rules, time basis,
and exclusions used consistently by reporting consumers.

**Reporting currency**:
USD, the common currency for commercial reporting, including international
orders. Customer checkout amounts retain their original currency separately.

**Order type**:
A characteristic of an order; multiple characteristics may apply to the same
order. Channel classifications follow their declared exclusivity rules and
remain distinct from metric eligibility.

**Commercial channel**:
One of retail/POS, specialty or bulk. Each commercial order belongs to exactly
one of these channels, with retail taking precedence over specialty and specialty
over bulk when evidence overlaps. Their revenue adds up to commercial revenue
under the same reporting scope. D2C and commercial are mutually exclusive.

**Reporting segment**:
A named group derived from order characteristics for a specific reporting
purpose. A complete MECE segmentation assigns every order in its declared
population to exactly one segment.

**Metric eligibility**:
Whether an order or line qualifies for a particular metric under that metric's
rules. Revenue, AOV and ASP have distinct eligible populations. Channel revenue
includes revenue from employee purchases, replacements and exchanges even when
those orders do not qualify for another metric.

**D2C order**:
An order in the direct-to-consumer channel, distinct from the platform's bulk,
specialty and retail/POS commercial channels. D2C is the residual channel: an
order is D2C when it matches no commercial channel rule, and unknown when that
commercial evidence is incomplete. Employee purchases, replacements
and exchanges can be D2C orders; their revenue contributes to D2C revenue even
when the orders are excluded from AOV or ASP. Later refunds do not change the
channel classification. Zero-revenue activity, including a free online replacement,
can also belong to D2C; channel membership does not require metric eligibility.
Shopify Collective and Broadcast activity belong to D2C when no commercial
classification applies, retaining their respective characteristics.

**Order-date basis**:
Attribution of an order's latest known values to its original order date, including
later refunds. Use the processing date when available; otherwise use the creation
date and preserve which timestamp supplied the date. This is the default for orders
and order lines, so historical period totals can change as those orders change.

**US D2C reporting market**:
A reporting population of D2C orders whose latest recorded shipping country is not
Canada, plus orders with no recorded shipping country, which are assigned to the US
reporting bucket. Preserve the recorded geography separately; this bucket does not
claim every order physically shipped to the United States. Later address edits can
change an order's historical market assignment.

**Cashflow-date basis**:
Attribution of payment and refund activity to the date of the corresponding
financial event. This is the basis for separate transactions and transaction lines.

**Replacement order**:
An order supplying a replacement for a previous purchase, identified separately
from gifting and new customer purchases.

**Gifted order**:
An order classified as a gift through approved gifting tags, including influencer,
PR/press, corporate and employee gifting, or inferred from a free-at-placement
employee order that is neither a test nor a replacement.

**Free-at-placement order**:
An order whose entire total after discounts, including shipping, tax and any other
charges, is zero when placed. Free merchandise with paid shipping does not qualify;
later edits or refunds do not establish that an order was free at placement.

**Net customer payment retained**:
Successfully collected customer payments minus successfully refunded payments,
including gift-card tender, shipping, and tax, before payment-processing fees.

**Fully refunded order**:
An order with previously collected payment and zero net customer payment retained
after refunds. An order with no collected payment is unpaid, rather than fully refunded.
Identified failed-payment charges and their reversing refunds do not establish this status.

**Partially refunded order**:
An order with a positive amount successfully refunded and positive net customer
payment retained. Payment amounts include tax and gift-card tender, before fees.
Identified reversals of failed-payment charges are excluded; an order cannot be
both partially and fully refunded.

**Unpaid order**:
An order with no successfully collected payment, including an authorization-only
order. It remains available for investigation but is excluded from commercial
reporting eligibility; successful gift-card payment counts as collection.

**Partially paid order**:
An order with some payment collected but an outstanding amount still due,
including a previously fully paid order whose edit creates an unpaid balance.
The whole order is excluded from commercial reporting until the revised amount
is fully paid; later refunds follow the separate refund eligibility rules.

**Commercial value retained**:
Customer payment value retained for merchandise and shipping after refunds,
excluding tax and before payment-processing fees; gift-card tender counts as value.

**Average order value (AOV)**:
The internal average of net revenue across eligible orders.
Eligibility excludes test, cancelled, gifted, replacement, unpaid,
partially paid, and fully refunded orders, including orders whose only remaining
payment is tax. Partial refunds reduce the value of otherwise eligible orders;
zero commercial value retained excludes an
order even when its payment reconciliation balance remains positive.

**Net sales**:
Gross merchandise value minus discounts and refunds, excluding shipping and tax.
Net sales is the amount basis for average selling price.

**Order-line allocation**:
Attribution of discounts, refunds, or shipping to individual order lines for
line-level reporting. Source line allocations take precedence; amounts without
source line allocations use agreed value or quantity weights, with equal shares
when all eligible weights are zero.

**Net revenue**:
Net sales plus shipping, excluding tax. Net revenue is the amount basis for AOV.

**Return handling fee**:
A finalized fee retained for processing a return, reported separately from
merchandise sales and shipping and excluded from net sales, net revenue, AOV and ASP.

**Average selling price (ASP)**:
Total eligible net sales divided by total eligible net ordered quantity,
excluding shipping and tax. A quantity-based refund reduces the denominator
even when the customer keeps the item. A goodwill amount refund without refunded
units reduces net sales without reducing quantity. A full amount-only refund
leaves otherwise eligible net units in the denominator at zero net sales; full
monetary refund alone does not exclude those units from ASP. Otherwise eligible
zero-net-unit lines retain their net sales in the numerator.

**Gross ordered quantity**:
Units ordered before cancellations, removals and quantity-based refunds.

**Gross order count**:
The count of source orders before eligibility filters.

**Net order count**:
The count of orders with at least one net ordered merchandise unit within the
selected reporting population. An amount-only refund does not remove an order
from this count when qualifying units remain, even if the order is fully refunded.
The count can differ from the eligible order count used for AOV.

**Net ordered quantity**:
Units remaining after cancellations, removals and quantity-based refunds, counting
each unit reduction once. This is the default reported ordered-unit measure and
the quantity basis for ASP. Physical returns are tracked separately.

**Refunded quantity**:
Units explicitly associated with a line-item refund. A monetary adjustment alone
does not establish refunded units or a physical return.

**Returned quantity**:
Units physically returned, supported by return evidence. A refund does not by
itself establish a physical return.

**Kept quantity**:
Physical units kept by customers. Refunded units can still be kept, so this
quantity can differ from net ordered quantity.

**Maximum data age**:
At any query time, the maximum permitted delay between an in-scope source change
and its visibility in the consumer's queryable dataset, including all intervening
delays. A 30-minute limit means changes more than 30 minutes old must already be
reflected; historical records may remain regardless of their creation date.
This is a dataset or consumer expectation, separate from extraction duration
and scheduling frequency.

**Source coverage cutoff**:
The time through which a dataset is established to reflect in-scope
source changes under its declared completeness contract. It is distinct from the
newest record timestamp or the time the last job finished.
