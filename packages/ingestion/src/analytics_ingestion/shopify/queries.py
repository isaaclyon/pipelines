"""Exact Admin GraphQL documents for the raw order source.

These strings are the source contract. Changing selected fields changes
``PAGED_QUERY_VERSION`` and the row mapping in ``docs/specs``; tests compare both.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from typing import Literal

API_VERSION = "2026-10"

# Order.transactions and Order.refunds are truncating arrays, not connections.
# Bounds come from a profile of a large production store, where no order had
# more than 140 transactions or 9 refunds. Both limits exceed the observed
# maximum plus 25% and were executed in bulk and point reads.
# Transactions are complete only when an EXACT transactionsCount equals the
# returned length; a refund array reaching its limit is always incomplete.
TRANSACTION_LIMIT = 250
REFUND_LIMIT = 50
# First page of each refund child connection in batched hydration reads.
REFUND_CHILD_PAGE = 25
REFUND_CHILD_FOLLOW_PAGE = 250
LINE_ITEM_PAGE = 250
REFUNDS_PER_HYDRATION_REQUEST = 50
VERSION_CHECK_IDS_PER_REQUEST = 250

MONEY = "{ shopMoney { amount currencyCode } presentmentMoney { amount currencyCode } }"
ADDRESS_FIELDS = (
    "firstName lastName company address1 address2 city province provinceCode "
    "zip country countryCodeV2 phone"
)
TAX_FIELDS = "title rate channelLiable priceSet MONEY"
ALLOCATION_FIELDS = "allocatedAmountSet MONEY discountApplication { index }"
DISCOUNT_FIELDS = (
    "__typename index allocationMethod targetSelection targetType "
    "value { __typename ... on MoneyV2 { amount currencyCode } "
    "... on PricingPercentageValue { percentage } } "
    "... on DiscountCodeApplication { code } "
    "... on ManualDiscountApplication { title description } "
    "... on AutomaticDiscountApplication { title } "
    "... on ScriptDiscountApplication { title }"
)
SHIPPING_FIELDS = (
    "id title code source isRemoved originalPriceSet MONEY "
    "discountedPriceSet MONEY currentDiscountedPriceSet MONEY "
    f"discountAllocations {{ {ALLOCATION_FIELDS} }} taxLines {{ {TAX_FIELDS} }}"
)
SALE_FIELDS = (
    "__typename id actionType lineType quantity totalAmount MONEY "
    "totalDiscountAmountAfterTaxes MONEY totalDiscountAmountBeforeTaxes MONEY "
    "totalTaxAmount MONEY "
    f"taxes {{ id amount MONEY taxLine {{ {TAX_FIELDS} }} }} "
    "... on ProductSale { lineItem { id } } "
    "... on GiftCardSale { lineItem { id } } "
    "... on TipSale { lineItem { id } } "
    "... on ShippingLineSale { shippingLine { id } } "
    "... on DutySale { duty { id } } "
    "... on FeeSale { fee { __typename id } } "
    "... on AdditionalFeeSale { additionalFee { id name price MONEY } }"
)
AGREEMENT_FIELDS = (
    "__typename id happenedAt reason "
    "... on RefundAgreement { refund { id } } "
    "... on ReturnAgreement { return { id } }"
)
ORDER_CHILD_PAGE = 5

ORDER_FIELDS = (
    (
        "id name createdAt updatedAt processedAt test sourceName cancelReason "
        "cancelledAt closedAt customer { id } displayFinancialStatus "
        "tags discountCodes email phone taxesIncluded "
        f"billingAddress {{ {ADDRESS_FIELDS} }} shippingAddress {{ {ADDRESS_FIELDS} }} "
        f"taxLines {{ {TAX_FIELDS} }} "
        "originalTotalPriceSet MONEY totalTaxSet MONEY totalShippingPriceSet MONEY "
        "displayFulfillmentStatus currencyCode presentmentCurrencyCode "
        "totalPriceSet MONEY currentTotalPriceSet MONEY subtotalPriceSet MONEY "
        "currentSubtotalPriceSet MONEY totalDiscountsSet MONEY "
        "currentTotalTaxSet MONEY totalRefundedSet MONEY "
        "transactionsCount { count precision } "
        "transactions(first: TRANSACTION_LIMIT) { id parentTransaction { id } "
        "createdAt processedAt kind status gateway test errorCode amountSet MONEY } "
        "refunds(first: REFUND_LIMIT) { id createdAt updatedAt processedAt note "
        "return { id } duties { amountSet MONEY originalDuty { id } } "
        "totalRefundedSet MONEY }"
    )
    .replace("TRANSACTION_LIMIT", str(TRANSACTION_LIMIT))
    .replace("REFUND_LIMIT", str(REFUND_LIMIT))
)

LINE_ITEM_FIELDS = (
    "id sku name quantity currentQuantity unfulfilledQuantity "
    "nonFulfillableQuantity requiresShipping originalUnitPriceSet MONEY "
    "discountedTotalSet MONEY product { id } variant { id } "
    "title variantTitle isGiftCard "
    f"discountAllocations {{ {ALLOCATION_FIELDS} }} taxLines {{ {TAX_FIELDS} }}"
)

REFUND_CHILD_FIELDS: dict[str, str] = {
    "refundLineItems": (
        "id quantity lineItem { id } subtotalSet MONEY totalTaxSet MONEY"
    ),
    "orderAdjustments": "id reason amountSet MONEY taxAmountSet MONEY",
    "refundShippingLines": (
        "id shippingLine { id } subtotalAmountSet MONEY taxAmountSet MONEY"
    ),
    "transactions": "id",
}

PAGE_INFO = "pageInfo { hasNextPage endCursor }"

ORDER_CHILD_FIELDS = {
    "discountApplications": DISCOUNT_FIELDS,
    "shippingLines": SHIPPING_FIELDS,
    "agreements": (
        AGREEMENT_FIELDS
        + f" sales(first: {ORDER_CHILD_PAGE}) {{ nodes {{ {SALE_FIELDS} }} "
        + f"{PAGE_INFO} }}"
    ),
}


def _order_connection(name: str, first: int, after: bool = False) -> str:
    args = f"first: {first}"
    if after:
        args += ", after: $after"
    if name == "shippingLines":
        args += ", includeRemovals: true"
    return (
        f"{name}({args}) {{ nodes {{ {_money(ORDER_CHILD_FIELDS[name])} }} "
        f"{PAGE_INFO} }}"
    )


def order_child_page_query(connection: str) -> str:
    return (
        "query($id: ID!, $after: String) { order(id: $id) { id "
        + _order_connection(connection, 100, after=True)
        + " } }"
    )


def agreement_sales_query() -> str:
    # SalesAgreement is not a Node. Resolve it through the owning order and
    # verify the returned identity; a silently ignored search must never mix sales.
    return (
        "query($id: ID!, $search: String!, $after: String) { order(id: $id) { id "
        "agreements(first: 2, query: $search) { nodes { id "
        f"sales(first: 100, after: $after) {{ nodes {{ {_money(SALE_FIELDS)} }} "
        f"{PAGE_INFO} }}"
        " } } } }"
    )


def _money(text: str) -> str:
    return text.replace("MONEY", MONEY)


WindowField = Literal["updated_at", "created_at"]


def format_instant(value: datetime) -> str:
    """Render an aware instant as UTC with second precision and a Z suffix."""
    if value.tzinfo is None:
        raise ValueError("Source instants must be timezone-aware")
    utc = value.astimezone(UTC)
    if utc.microsecond:
        raise ValueError("Source window bounds must be whole seconds")
    return utc.strftime("%Y-%m-%dT%H:%M:%SZ")


def search_window(field: WindowField, start: datetime, end: datetime) -> str:
    """Quoted half-open search; unquoted ISO colons change Shopify parsing."""
    lower, upper = format_instant(start), format_instant(end)
    if end <= start:
        raise ValueError("Source window end must follow its start")
    return f'{field}:>="{lower}" {field}:<"{upper}"'


ORDERS_COUNT = (
    "query($search: String!, $limit: Int!) { ordersCount(query: $search, "
    "limit: $limit) { count precision } }"
)

SHOP_IDENTITY = (
    "query { shop { id myshopifyDomain } currentAppInstallation { "
    "accessScopes { handle } } }"
)

EARLIEST_ORDER = (
    "query { orders(first: 1, sortKey: CREATED_AT) { nodes { id createdAt } } }"
)

ORDER_VERSIONS = (
    "query($ids: [ID!]!) { nodes(ids: $ids) { ... on Order { id updatedAt } } }"
)


def refund_children_query() -> str:
    """First pages of refund components and payment links for several refunds."""
    parts = " ".join(
        f"{name}(first: {REFUND_CHILD_PAGE}) {{ nodes {{ {_money(fields)} }} "
        f"{PAGE_INFO} }}"
        for name, fields in REFUND_CHILD_FIELDS.items()
    )
    return (
        "query($ids: [ID!]!) { nodes(ids: $ids) { ... on Refund { id "
        + parts
        + " } } }"
    )


def refund_child_page_query(connection: str) -> str:
    """Follow-up page for one refund child connection."""
    fields = REFUND_CHILD_FIELDS[connection]
    return (
        "query($id: ID!, $after: String) { node(id: $id) { ... on Refund { "
        f"{connection}(first: {REFUND_CHILD_FOLLOW_PAGE}, after: $after) "
        f"{{ nodes {{ {_money(fields)} }} {PAGE_INFO} }} }} }} }}"
    )


# Paginated reads for the dlt source (ADR 0024). Measured on 2026-10-14 against
# Shopify Plus (20,000-point bucket, 1,000 points/s): 50 orders with five line
# items and the first page of each commercial connection request 772 points of
# the 1,000-point query limit; 100 orders with ten line items exceed it.
PAGE_SIZE = 50
PAGE_LINE_ITEMS = 5


def orders_page_query() -> str:
    """One page of complete order headers with first child pages inline."""
    children = " ".join(
        _order_connection(name, ORDER_CHILD_PAGE) for name in ORDER_CHILD_FIELDS
    )
    return (
        "query($search: String!, $after: String) { orders(first: "
        + f"{PAGE_SIZE}, after: $after, query: $search, sortKey: UPDATED_AT) "
        + "{ nodes { "
        + _money(ORDER_FIELDS)
        + f" {children} lineItems(first: {PAGE_LINE_ITEMS}) {{ nodes {{ "
        + _money(LINE_ITEM_FIELDS)
        + f" }} {PAGE_INFO} }} }} {PAGE_INFO} }} }}"
    )


def order_lines_page_query() -> str:
    """Follow-up line items for one order beyond the inline first page."""
    return (
        "query($id: ID!, $after: String) { order(id: $id) { id "
        + f"lineItems(first: {LINE_ITEM_PAGE}, after: $after) {{ nodes {{ "
        + _money(LINE_ITEM_FIELDS)
        + f" }} {PAGE_INFO} }} }} }}"
    )


PAGED_QUERY_VERSION = hashlib.sha256(
    "\n".join(
        [
            API_VERSION,
            orders_page_query(),
            order_lines_page_query(),
            refund_children_query(),
            *(refund_child_page_query(name) for name in REFUND_CHILD_FIELDS),
            *(order_child_page_query(name) for name in ORDER_CHILD_FIELDS),
            agreement_sales_query(),
        ]
    ).encode()
).hexdigest()[:16]
