"""Raw order contract: typed parsing of source objects into destination rows.

Core physical tables (dlt nests child lists under the ``orders`` root):

=================================  =========================================
Logical table                      Physical table
=================================  =========================================
orders                             ``orders``
order_lines                        ``orders__lines``
refunds                            ``orders__refunds``
refund_lines                       ``orders__refunds__lines``
refund_adjustments                 ``orders__refunds__adjustments``
refund_shipping_lines              ``orders__refunds__shipping_lines``
order_transactions                 ``orders__transactions``
=================================  =========================================

Each table is a list of ``Field`` entries that declare its columns and parse
its rows, so a column cannot be declared without being filled or filled
without being declared.

Every row carries the shop ID and explicit ancestor IDs; consumers never need
dlt's internal ``_dlt_*`` keys. Amounts are exact ``Decimal`` values stored as
DECIMAL(38, 9); retained source files keep Shopify's original text.
Revision-2 commercial children are declared in ``NESTED_TABLES`` and documented
in the raw ingestion specification alongside these original relations.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal

from analytics_ingestion.shopify.errors import ContractError
from analytics_ingestion.shopify.jsonvalue import (
    JsonObject,
    as_list,
    as_object,
    gid,
    objects,
    optional_instant,
    optional_ref,
    optional_str,
    parse_decimal,
    required_bool,
    required_instant,
    required_int,
    required_str,
    selected_value,
)
from analytics_ingestion.shopify.pipeline import Column, ColumnType, Row
from analytics_ingestion.shopify.queries import REFUND_LIMIT


@dataclass(frozen=True)
class Field:
    """Destination columns and how one source object fills them.

    A table's fields declare its columns and parse its rows, so each column is
    written once. Fields filled by the caller (ancestor IDs, positions, load
    metadata) read nothing.
    """

    columns: tuple[Column, ...]
    read: Callable[[JsonObject], Row]


@dataclass(frozen=True)
class MoneyBag:
    shop_amount: Decimal
    shop_currency: str
    presentment_amount: Decimal
    presentment_currency: str


def money_bag(data: JsonObject, field: str) -> MoneyBag:
    bag = as_object(data.get(field), field)
    shop = as_object(bag.get("shopMoney"), f"{field}.shopMoney")
    presentment = as_object(bag.get("presentmentMoney"), f"{field}.presentmentMoney")
    return MoneyBag(
        parse_decimal(shop.get("amount"), f"{field}.shopMoney.amount"),
        required_str(shop, "currencyCode"),
        parse_decimal(presentment.get("amount"), f"{field}.presentmentMoney.amount"),
        required_str(presentment, "currencyCode"),
    )


def optional_money_bag(data: JsonObject, field: str) -> MoneyBag | None:
    return None if selected_value(data, field) is None else money_bag(data, field)


def _money_values(prefix: str, bag: MoneyBag | None) -> Row:
    return {
        f"{prefix}_shop_amount": None if bag is None else bag.shop_amount,
        f"{prefix}_shop_currency": None if bag is None else bag.shop_currency,
        f"{prefix}_presentment_amount": None if bag is None else bag.presentment_amount,
        f"{prefix}_presentment_currency": (
            None if bag is None else bag.presentment_currency
        ),
    }


def _text(name: str, description: str, nullable: bool = True) -> Column:
    return Column(name, "text", nullable, description=description)


def _money_columns(prefix: str, nullable: bool, description: str) -> list[Column]:
    """A Shopify MoneyBag: ``description`` says what the amount measures."""
    return [
        Column(
            f"{prefix}_shop_amount",
            "decimal",
            nullable,
            description=f"{description} Amount in the shop currency.",
        ),
        _text(
            f"{prefix}_shop_currency",
            f"Shop currency code of {prefix}_shop_amount.",
            nullable,
        ),
        Column(
            f"{prefix}_presentment_amount",
            "decimal",
            nullable,
            description=f"{description} Amount in the currency the customer saw.",
        ),
        _text(
            f"{prefix}_presentment_currency",
            f"Presentment currency code of {prefix}_presentment_amount.",
            nullable,
        ),
    ]


ANCESTOR_DESCRIPTIONS = {
    "shop_id": "Shopify global ID of the shop the row was read from.",
    "order_id": "Shopify global ID of the order.",
    "line_item_id": "Shopify global ID of the order line that owns this row.",
    "refund_id": "Shopify global ID of the refund that owns this row.",
    "shipping_line_id": "Shopify global ID of the shipping line that owns this row.",
    "agreement_id": "Shopify global ID of the sales agreement that owns this row.",
    "sale_id": "Shopify global ID of the sale that owns this row.",
    "transaction_id": "Shopify global ID of the order transaction.",
}


def _ancestors(*names: str) -> list[Column]:
    return [
        _text(name, ANCESTOR_DESCRIPTIONS[name], False)
        for name in ("shop_id", "order_id", *names)
    ]


def _columns(fields: Sequence[Field]) -> list[Column]:
    return [column for field in fields for column in field.columns]


def _read(fields: Sequence[Field], data: JsonObject) -> Row:
    row: Row = {}
    for field in fields:
        row.update(field.read(data))
    return row


def _value(
    name: str,
    data_type: ColumnType,
    nullable: bool,
    description: str,
    read: Callable[[JsonObject], object],
) -> Field:
    column = Column(name, data_type, nullable, description=description)
    return Field((column,), lambda data: {name: read(data)})


def _given(*columns: Column) -> Field:
    return Field(columns, lambda _: {})


def _string(name: str, key: str, description: str, *, required: bool = False) -> Field:
    if required:
        return _value(
            name, "text", False, description, lambda data: required_str(data, key)
        )
    return _value(name, "text", True, description, lambda data: optional_str(data, key))


def _instant(name: str, key: str, description: str, *, required: bool = False) -> Field:
    if required:
        return _value(
            name,
            "timestamp",
            False,
            description,
            lambda data: required_instant(data, key),
        )
    return _value(
        name,
        "timestamp",
        True,
        description,
        lambda data: optional_instant(data, key),
    )


def _boolean(name: str, key: str, description: str, *, required: bool = True) -> Field:
    if required:
        return _value(
            name, "bool", False, description, lambda data: required_bool(data, key)
        )
    return _value(
        name, "bool", True, description, lambda data: _nullable_bool(data, key)
    )


def _integer(name: str, key: str, description: str) -> Field:
    return _value(
        name, "bigint", False, description, lambda data: required_int(data, key)
    )


def _gid(name: str, kind: str, label: str, description: str) -> Field:
    return _value(
        name, "text", False, description, lambda data: gid(data.get("id"), kind, label)
    )


def _reference(name: str, key: str, kind: str, description: str) -> Field:
    return _value(
        name, "text", True, description, lambda data: optional_ref(data, key, kind)
    )


def _money(prefix: str, key: str, description: str, *, required: bool = True) -> Field:
    bag = money_bag if required else optional_money_bag
    return Field(
        tuple(_money_columns(prefix, not required, description)),
        lambda data: _money_values(prefix, bag(data, key)),
    )


def _stored_nullable(field: Field) -> Field:
    return Field(tuple(replace(c, nullable=True) for c in field.columns), field.read)


def _additive(*fields: Field) -> list[Field]:
    """Revision 2 is additive: existing rows have no evidence for these columns.

    Source parsing still requires selected fields; nullable storage is not
    license to turn a missing response field into a legitimate Shopify null.
    """
    return [_stored_nullable(field) for field in fields]


def _when(kinds: set[str], field: Field) -> Field:
    """A field only some ``__typename`` values carry; null for the others."""

    def read(data: JsonObject) -> Row:
        if required_str(data, "__typename") in kinds:
            return field.read(data)
        return {column.name: None for column in field.columns}

    return Field(_stored_nullable(field).columns, read)


METADATA_COLUMNS = [
    _text(
        "_batch_id", "Dagster run ID of the ingestion run that loaded the row.", False
    ),
    Column(
        "_observation_sequence",
        "bigint",
        True,
        description="Ledger-era observation order; null for rows the dlt source loads.",
    ),
    Column(
        "_observed_at",
        "timestamp",
        False,
        description="UTC instant ingestion read the order's source page.",
    ),
    _text(
        "_extraction_kind",
        "How the run selected the order: refresh, lookback or reextract.",
        False,
    ),
    _text(
        "_content_digest",
        "Ledger-era digest of the parsed order; null for rows the dlt source loads.",
    ),
    _text(
        "_query_version",
        "Fingerprint of the GraphQL query documents that read the order.",
        False,
    ),
    Column(
        "_extraction_revision",
        "bigint",
        True,
        description=(
            "Raw contract revision that parsed the order. Columns added by a "
            "later revision are null on rows from an earlier one."
        ),
    ),
    _text(
        "_api_version",
        "Shopify Admin API version that served the order.",
        False,
    ),
]


ADDRESS_NAMES = {
    "firstName": ("first_name", "First name"),
    "lastName": ("last_name", "Last name"),
    "company": ("company", "Company name"),
    "address1": ("address1", "First street line"),
    "address2": ("address2", "Second street line"),
    "city": ("city", "City"),
    "province": ("province", "Province or state name"),
    "provinceCode": ("province_code", "Province or state code"),
    "zip": ("zip", "Postal code"),
    "country": ("country", "Country name"),
    "countryCodeV2": ("country_code", "ISO 3166-1 alpha-2 country code"),
    "phone": ("phone", "Phone number"),
}


def _address(kind: str, key: str) -> Field:
    def read(data: JsonObject) -> Row:
        address = _nullable_object(data, key)
        return {
            f"{kind}_address_{name}": (
                None if address is None else optional_str(address, source)
            )
            for source, (name, _) in ADDRESS_NAMES.items()
        }

    return Field(
        tuple(
            _text(
                f"{kind}_address_{name}",
                f"{label} of the order's {kind} address; null without an address.",
            )
            for name, label in ADDRESS_NAMES.values()
        ),
        read,
    )


ORDER_FIELDS = [
    _string(
        "name", "name", "Order name shown in Shopify, such as #1001.", required=True
    ),
    _instant(
        "created_at",
        "createdAt",
        "UTC instant Shopify created the order.",
        required=True,
    ),
    _instant(
        "updated_at",
        "updatedAt",
        "UTC instant Shopify last changed the order; the merge keeps the newest.",
        required=True,
    ),
    _instant(
        "processed_at",
        "processedAt",
        "UTC instant the order was processed; imports can predate creation.",
        required=True,
    ),
    _boolean("test", "test", "Whether Shopify marks the order as a test order."),
    _string(
        "source_name",
        "sourceName",
        "Channel or app that created the order, such as web, pos or an app ID.",
    ),
    _string(
        "cancel_reason",
        "cancelReason",
        "Shopify cancellation reason code; null unless cancelled.",
    ),
    _instant(
        "cancelled_at",
        "cancelledAt",
        "UTC instant the order was cancelled; null unless cancelled.",
    ),
    _instant(
        "closed_at", "closedAt", "UTC instant the order was closed; null while open."
    ),
    _reference(
        "customer_id",
        "customer",
        "Customer",
        "Shopify global ID of the order's customer; null for guest orders.",
    ),
    _string(
        "display_financial_status",
        "displayFinancialStatus",
        "Shopify financial status label, such as PAID or PARTIALLY_REFUNDED.",
    ),
    _string(
        "display_fulfillment_status",
        "displayFulfillmentStatus",
        "Shopify fulfillment status label, such as FULFILLED or UNFULFILLED.",
        required=True,
    ),
    _string(
        "currency_code",
        "currencyCode",
        "Shop currency code of the order's shop amounts.",
        required=True,
    ),
    _string(
        "presentment_currency_code",
        "presentmentCurrencyCode",
        "Currency the customer saw and paid in.",
        required=True,
    ),
    _money(
        "total_price",
        "totalPriceSet",
        "Original order total including tax and shipping.",
    ),
    _money(
        "current_total_price",
        "currentTotalPriceSet",
        "Order total after edits, refunds and removals.",
    ),
    _money(
        "subtotal_price",
        "subtotalPriceSet",
        "Original line subtotal after discounts, before shipping and tax.",
        required=False,
    ),
    _money(
        "current_subtotal_price",
        "currentSubtotalPriceSet",
        "Line subtotal after edits, refunds and removals.",
    ),
    _money(
        "total_discounts",
        "totalDiscountsSet",
        "Total discounts applied to the original order; null when absent.",
        required=False,
    ),
    _money(
        "current_total_tax",
        "currentTotalTaxSet",
        "Tax after edits, refunds and removals.",
    ),
    _money(
        "total_refunded", "totalRefundedSet", "Total amount refunded to the customer."
    ),
    _given(
        Column(
            "transactions_count",
            "bigint",
            False,
            description="Shopify's exact count of the order's transactions.",
        ),
        _text(
            "transactions_count_precision",
            "Precision of transactions_count; loaded orders are always EXACT.",
            False,
        ),
        *METADATA_COLUMNS,
    ),
    *_additive(
        _string(
            "email", "email", "Contact email on the order. Restricted personal data."
        ),
        _string(
            "phone", "phone", "Contact phone on the order. Restricted personal data."
        ),
        _boolean(
            "taxes_included",
            "taxesIncluded",
            "Whether line prices include tax.",
        ),
        _money(
            "original_total_price",
            "originalTotalPriceSet",
            "Order total at creation, before any edits.",
        ),
        _money("total_tax", "totalTaxSet", "Original total tax on the order."),
        _money(
            "total_shipping_price",
            "totalShippingPriceSet",
            "Original shipping charges after shipping discounts.",
        ),
        _address("billing", "billingAddress"),
        _address("shipping", "shippingAddress"),
    ),
]

LINE_FIELDS = [
    _gid(
        "line_item_id",
        "LineItem",
        "lineItem.id",
        "Shopify global ID of the order line.",
    ),
    _string("sku", "sku", "Variant SKU at purchase; null when the variant has none."),
    _string(
        "name",
        "name",
        "Product and variant name at purchase, as Shopify displays the line.",
        required=True,
    ),
    _integer("quantity", "quantity", "Units originally ordered on the line."),
    _integer(
        "current_quantity",
        "currentQuantity",
        "Units remaining after edits and removals.",
    ),
    _money(
        "original_unit_price",
        "originalUnitPriceSet",
        "Unit price before discounts.",
    ),
    _money(
        "discounted_total",
        "discountedTotalSet",
        "Line total after line-level discounts, before order-level allocations.",
    ),
    _reference(
        "product_id",
        "product",
        "Product",
        "Shopify global ID of the product; null if it was deleted or is custom.",
    ),
    _reference(
        "variant_id",
        "variant",
        "ProductVariant",
        "Shopify global ID of the variant; null if it was deleted or is custom.",
    ),
    *_additive(
        _integer(
            "unfulfilled_quantity",
            "unfulfilledQuantity",
            "Units on the line not yet fulfilled.",
        ),
        _integer(
            "non_fulfillable_quantity",
            "nonFulfillableQuantity",
            "Units on the line that cannot be fulfilled.",
        ),
        _boolean(
            "requires_shipping",
            "requiresShipping",
            "Whether the line requires shipping.",
        ),
        _string("title", "title", "Product title at purchase.", required=True),
        _string("variant_title", "variantTitle", "Variant title at purchase."),
        _boolean("is_gift_card", "isGiftCard", "Whether the line sells a gift card."),
    ),
]

REFUND_FIELDS = [
    _gid("refund_id", "Refund", "refund.id", "Shopify global ID of the refund."),
    _instant("created_at", "createdAt", "UTC instant the refund was created."),
    _instant(
        "updated_at",
        "updatedAt",
        "UTC instant Shopify last changed the refund.",
        required=True,
    ),
    _money(
        "total_refunded", "totalRefundedSet", "Total amount refunded by this refund."
    ),
    _string("note", "note", "Staff note on the refund."),
    _reference(
        "return_id",
        "return",
        "Return",
        "Shopify global ID of the return the refund settles; null when unrelated.",
    ),
    _instant("processed_at", "processedAt", "UTC instant the refund was processed."),
]


def _refund_line_id(data: JsonObject) -> str:
    if data.get("id") is None:
        raise ContractError("Refund line without ID cannot be keyed")
    return gid(data.get("id"), "RefundLineItem", "id")


REFUND_LINE_FIELDS = [
    _value(
        "refund_line_item_id",
        "text",
        False,
        "Shopify global ID of the refund line.",
        _refund_line_id,
    ),
    _value(
        "line_item_id",
        "text",
        False,
        "Shopify global ID of the refunded order line.",
        lambda data: gid(
            as_object(data.get("lineItem"), "lineItem").get("id"),
            "LineItem",
            "refundLineItem.lineItem",
        ),
    ),
    _integer("quantity", "quantity", "Units of the order line refunded."),
    _money("subtotal", "subtotalSet", "Refunded line amount before tax."),
    _money("total_tax", "totalTaxSet", "Tax refunded on the line."),
]

REFUND_ADJUSTMENT_FIELDS = [
    _gid(
        "adjustment_id",
        "OrderAdjustment",
        "adjustment.id",
        "Shopify global ID of the order adjustment.",
    ),
    _money(
        "amount",
        "amountSet",
        "Adjustment amount, signed as Shopify reports it.",
    ),
    _money(
        "tax_amount",
        "taxAmountSet",
        "Tax on the adjustment, signed as Shopify reports it.",
    ),
    _string(
        "reason",
        "reason",
        "Shopify adjustment reason, such as REFUND_DISCREPANCY.",
    ),
]

REFUND_SHIPPING_FIELDS = [
    _gid(
        "refund_shipping_line_id",
        "RefundShippingLine",
        "refundShippingLine.id",
        "Shopify global ID of the refunded shipping line.",
    ),
    _money(
        "subtotal_amount", "subtotalAmountSet", "Shipping amount refunded before tax."
    ),
    _money("tax_amount", "taxAmountSet", "Tax refunded on shipping."),
    _reference(
        "shipping_line_id",
        "shippingLine",
        "ShippingLine",
        "Shopify global ID of the original shipping line.",
    ),
]

POSITION_DESCRIPTION = (
    "Zero-based position in the owning source list; not a durable source ID."
)

REFUND_DUTY_FIELDS = [
    _reference(
        "duty_id",
        "originalDuty",
        "Duty",
        "Shopify global ID of the original duty; null if it is unavailable.",
    ),
    _given(Column("position", "bigint", True, description=POSITION_DESCRIPTION)),
    _money("amount", "amountSet", "Duty amount refunded."),
]

TRANSACTION_FIELDS = [
    _gid(
        "transaction_id",
        "OrderTransaction",
        "transaction.id",
        "Shopify global ID of the order transaction.",
    ),
    _reference(
        "parent_transaction_id",
        "parentTransaction",
        "OrderTransaction",
        "Transaction this one captures, voids or refunds; null for originals.",
    ),
    _instant(
        "created_at",
        "createdAt",
        "UTC instant the transaction was created.",
        required=True,
    ),
    _instant("processed_at", "processedAt", "UTC instant the gateway processed it."),
    _string(
        "kind",
        "kind",
        "Transaction kind, such as AUTHORIZATION, CAPTURE, SALE or REFUND.",
        required=True,
    ),
    _string(
        "status",
        "status",
        "Transaction status, such as SUCCESS, PENDING or FAILURE.",
        required=True,
    ),
    _string("gateway", "gateway", "Payment gateway that handled the transaction."),
    _boolean("test", "test", "Whether the transaction is a test transaction."),
    _string(
        "error_code",
        "errorCode",
        "Gateway error code; null for transactions without an error.",
    ),
    _money("amount", "amountSet", "Transaction amount."),
]

POSITION = _given(Column("position", "bigint", False, description=POSITION_DESCRIPTION))

TAX_FIELDS = [
    POSITION,
    _string("title", "title", "Tax name, such as a state or VAT tax.", required=True),
    _value(
        "rate",
        "decimal",
        True,
        "Tax rate as a fraction, such as 0.06 for 6%.",
        lambda data: _number(data, "rate"),
    ),
    _boolean(
        "channel_liable",
        "channelLiable",
        "Whether the sales channel, not the merchant, remits the tax.",
        required=False,
    ),
    _money("price", "priceSet", "Tax amount."),
]

ALLOCATION_FIELDS = [
    POSITION,
    _value(
        "application_index",
        "bigint",
        False,
        "Index of the order discount application that produced the allocation.",
        lambda data: required_int(
            as_object(data.get("discountApplication"), "discountApplication"),
            "index",
        ),
    ),
    _money(
        "allocated_amount",
        "allocatedAmountSet",
        "Discount amount allocated to the owning line.",
    ),
]

SHIPPING_FIELDS = [
    _gid(
        "shipping_line_id",
        "ShippingLine",
        "shipping line id",
        "Shopify global ID of the shipping line.",
    ),
    _string(
        "title", "title", "Shipping method title shown to the customer.", required=True
    ),
    _string("code", "code", "Shipping rate code."),
    _string("source", "source", "Rate provider, such as shopify or a carrier."),
    _boolean(
        "is_removed",
        "isRemoved",
        "Whether the shipping line was removed after the order was placed.",
    ),
    _money("original_price", "originalPriceSet", "Shipping price before discounts."),
    _money(
        "discounted_price",
        "discountedPriceSet",
        "Shipping price after discounts at order creation.",
    ),
    _money(
        "current_discounted_price",
        "currentDiscountedPriceSet",
        "Shipping price after discounts and later edits.",
    ),
]


def _discount_value(item: JsonObject) -> Row:
    value = as_object(item.get("value"), "discount value")
    value_type = required_str(value, "__typename")
    if value_type not in {"MoneyV2", "PricingPercentageValue"}:
        raise ContractError("Unknown discount value type")
    money = value_type == "MoneyV2"
    return {
        "value_type": value_type,
        "value_amount": parse_decimal(value.get("amount"), "discount value")
        if money
        else None,
        "value_currency": required_str(value, "currencyCode") if money else None,
        "value_percentage": None if money else _number(value, "percentage"),
    }


DISCOUNT_FIELDS = [
    _integer(
        "application_index",
        "index",
        "Order-scoped index that discount allocations refer to.",
    ),
    _string(
        "type",
        "__typename",
        "Discount application type, such as DiscountCodeApplication.",
        required=True,
    ),
    _string(
        "allocation_method",
        "allocationMethod",
        "How the value is spread: ACROSS, EACH or ONE.",
        required=True,
    ),
    _string(
        "target_selection",
        "targetSelection",
        "Which lines qualify: ALL, ENTITLED or EXPLICIT.",
        required=True,
    ),
    _string(
        "target_type",
        "targetType",
        "What the discount targets: LINE_ITEM or SHIPPING_LINE.",
        required=True,
    ),
    _when(
        {"DiscountCodeApplication"},
        _string(
            "code",
            "code",
            "Discount code entered; null for other types.",
            required=True,
        ),
    ),
    _when(
        {
            "ManualDiscountApplication",
            "AutomaticDiscountApplication",
            "ScriptDiscountApplication",
        },
        _string("title", "title", "Discount title; null for discount codes."),
    ),
    _when(
        {"ManualDiscountApplication"},
        _string(
            "description",
            "description",
            "Staff description of a manual discount; null for other types.",
        ),
    ),
    Field(
        (
            _text(
                "value_type",
                "MoneyV2 for a fixed amount, PricingPercentageValue for a percent.",
                False,
            ),
            Column(
                "value_amount",
                "decimal",
                True,
                description="Fixed discount amount; null for percentage discounts.",
            ),
            _text(
                "value_currency",
                "Currency of value_amount; null for percentage discounts.",
            ),
            Column(
                "value_percentage",
                "decimal",
                True,
                description="Percentage, such as 10 for 10%; null for fixed amounts.",
            ),
        ),
        _discount_value,
    ),
]

AGREEMENT_FIELDS = [
    _value(
        "agreement_id",
        "text",
        False,
        "Shopify global ID of the sales agreement.",
        lambda data: _source_id(data),
    ),
    _string(
        "type",
        "__typename",
        "Agreement type, such as OrderAgreement, RefundAgreement or ReturnAgreement.",
        required=True,
    ),
    _instant(
        "happened_at",
        "happenedAt",
        "UTC instant the agreement took effect.",
        required=True,
    ),
    _string(
        "reason",
        "reason",
        "Why the agreement happened, such as ORDER, EDIT_ORDER or REFUND.",
        required=True,
    ),
    _when(
        {"RefundAgreement"},
        _reference(
            "refund_id",
            "refund",
            "Refund",
            "Refund behind a refund agreement; null for other types.",
        ),
    ),
    _when(
        {"ReturnAgreement"},
        _reference(
            "return_id",
            "return",
            "Return",
            "Return behind a return agreement; null for other types.",
        ),
    ),
]


def _fees(item: JsonObject) -> Row:
    kind = required_str(item, "__typename")
    fee = _nullable_object(item, "fee") if kind == "FeeSale" else None
    extra = (
        _nullable_object(item, "additionalFee") if kind == "AdditionalFeeSale" else None
    )
    return {
        "additional_fee_id": None if extra is None else _source_id(extra),
        "fee_id": None if fee is None else _source_id(fee),
        "fee_type": None if fee is None else required_str(fee, "__typename"),
        "additional_fee_name": None if extra is None else required_str(extra, "name"),
        **_money_values(
            "additional_fee_price", None if extra is None else money_bag(extra, "price")
        ),
    }


SALE_FIELDS = [
    _value(
        "sale_id",
        "text",
        False,
        "Shopify global ID of the sale.",
        lambda data: _source_id(data),
    ),
    _string(
        "type",
        "__typename",
        "Sale type, such as ProductSale, ShippingLineSale or DutySale.",
        required=True,
    ),
    _string(
        "action_type",
        "actionType",
        "What the sale records: ORDER, RETURN, UPDATE or UNKNOWN.",
        required=True,
    ),
    _string(
        "line_type",
        "lineType",
        "Kind of line sold, such as PRODUCT, SHIPPING, DUTY or TIP.",
        required=True,
    ),
    _value(
        "quantity",
        "bigint",
        True,
        "Signed units; negative for returns. Null when the sale has no quantity.",
        lambda data: (
            None
            if selected_value(data, "quantity") is None
            else required_int(data, "quantity")
        ),
    ),
    _when(
        {"ProductSale", "GiftCardSale", "TipSale"},
        _reference(
            "line_item_id",
            "lineItem",
            "LineItem",
            "Order line of a product, gift card or tip sale; null for other types.",
        ),
    ),
    _when(
        {"ShippingLineSale"},
        _reference(
            "shipping_line_id",
            "shippingLine",
            "ShippingLine",
            "Shipping line of a shipping sale; null for other types.",
        ),
    ),
    _when(
        {"DutySale"},
        _reference(
            "duty_id", "duty", "Duty", "Duty of a duty sale; null for other types."
        ),
    ),
    Field(
        (
            _text(
                "additional_fee_id",
                "Additional fee of an additional-fee sale; null for other types.",
            ),
            _text("fee_id", "Fee of a fee sale, such as a return fee; null otherwise."),
            _text(
                "fee_type", "Fee type of a fee sale, such as ReturnFee; null otherwise."
            ),
            _text(
                "additional_fee_name",
                "Name of the additional fee; null for other types.",
            ),
            *_money_columns(
                "additional_fee_price",
                True,
                "Additional fee price; null for other types.",
            ),
        ),
        _fees,
    ),
    _money(
        "total_amount", "totalAmount", "Sale total, signed; includes tax and discounts."
    ),
    _money("total_tax_amount", "totalTaxAmount", "Tax on the sale, signed."),
    _money(
        "total_discount_amount_before_taxes",
        "totalDiscountAmountBeforeTaxes",
        "Discounts on the sale before tax, signed.",
    ),
    _money(
        "total_discount_amount_after_taxes",
        "totalDiscountAmountAfterTaxes",
        "Discounts on the sale after tax, signed.",
    ),
]

SALE_TAX_FIELDS = [
    _value(
        "sale_tax_id",
        "text",
        False,
        "Shopify global ID of the sale tax.",
        lambda data: _source_id(data),
    ),
    _money("amount", "amount", "Tax amount charged on the sale, signed."),
]

ORDER_COLUMNS = [*_ancestors(), *_columns(ORDER_FIELDS)]
TAX_COLUMNS = _columns(TAX_FIELDS)
ALLOCATION_COLUMNS = _columns(ALLOCATION_FIELDS)

# Nested path (relative to the orders root) -> columns. Order defines the
# physical table names ``orders__<path joined by __>``.
NESTED_TABLES: dict[tuple[str, ...], list[Column]] = {
    ("lines",): [*_ancestors(), *_columns(LINE_FIELDS)],
    ("refunds",): [*_ancestors(), *_columns(REFUND_FIELDS)],
    ("refunds", "lines"): [*_ancestors("refund_id"), *_columns(REFUND_LINE_FIELDS)],
    ("refunds", "adjustments"): [
        *_ancestors("refund_id"),
        *_columns(REFUND_ADJUSTMENT_FIELDS),
    ],
    ("refunds", "shipping_lines"): [
        *_ancestors("refund_id"),
        *_columns(REFUND_SHIPPING_FIELDS),
    ],
    ("transactions",): [*_ancestors(), *_columns(TRANSACTION_FIELDS)],
    ("tags",): [*_ancestors(), _text("tag", "Order tag as entered in Shopify.", False)],
    ("discount_codes",): [
        *_ancestors(),
        _text("code", "Discount code applied to the order.", False),
    ],
    ("discount_applications",): [*_ancestors(), *_columns(DISCOUNT_FIELDS)],
    ("tax_lines",): [*_ancestors(), *TAX_COLUMNS],
    ("lines", "tax_lines"): [*_ancestors("line_item_id"), *TAX_COLUMNS],
    ("lines", "discount_allocations"): [
        *_ancestors("line_item_id"),
        *ALLOCATION_COLUMNS,
    ],
    ("shipping_lines",): [*_ancestors(), *_columns(SHIPPING_FIELDS)],
    ("shipping_lines", "tax_lines"): [*_ancestors("shipping_line_id"), *TAX_COLUMNS],
    ("shipping_lines", "discount_allocations"): [
        *_ancestors("shipping_line_id"),
        *ALLOCATION_COLUMNS,
    ],
    ("agreements",): [*_ancestors(), *_columns(AGREEMENT_FIELDS)],
    ("agreements", "sales"): [*_ancestors("agreement_id"), *_columns(SALE_FIELDS)],
    ("agreements", "sales", "taxes"): [
        *_ancestors("agreement_id", "sale_id"),
        *_columns(SALE_TAX_FIELDS),
        *TAX_COLUMNS,
    ],
    ("refunds", "transactions"): [*_ancestors("refund_id", "transaction_id")],
    ("refunds", "duties"): [*_ancestors("refund_id"), *_columns(REFUND_DUTY_FIELDS)],
}
ROOT_TABLE = "orders"

# Raw metadata for weekly deletion-candidate confirmation.
SOURCE_STATUS_TABLE = "order_source_status"
SOURCE_STATUS_COLUMNS = [
    _text("shop_id", ANCESTOR_DESCRIPTIONS["shop_id"], False),
    _text("order_id", ANCESTOR_DESCRIPTIONS["order_id"], False),
    _text(
        "status",
        "unavailable after two direct reads missed the order, or available again.",
        False,
    ),
    Column(
        "created_at",
        "timestamp",
        True,
        description="UTC creation instant of the order; sets its reconciliation month.",
    ),
    Column(
        "detected_at",
        "timestamp",
        False,
        description="UTC instant the reconciliation run recorded the status.",
    ),
    _text(
        "evidence_batch_id",
        "Dagster run ID of the reconciliation run that recorded the status.",
        False,
    ),
    _text("detail", "Evidence for the status, such as the direct-read result.", False),
]

# One row per successful run; dbt source freshness reads its covered window.
BATCH_EVIDENCE_TABLE = "ingestion_batches"
BATCH_EVIDENCE_COLUMNS = [
    _text("shop_id", ANCESTOR_DESCRIPTIONS["shop_id"], False),
    _text("batch_id", "Dagster run ID of the ingestion run.", False),
    _text("kind", "Run kind: refresh, lookback or reextract.", False),
    _text(
        "window_field",
        "Order timestamp the window filters: updated_at, or created_at for reextract.",
        False,
    ),
    Column(
        "window_start",
        "timestamp",
        False,
        description="Inclusive UTC start of the covered window.",
    ),
    Column(
        "window_end",
        "timestamp",
        False,
        description="Exclusive UTC end of the covered window; freshness reads it.",
    ),
    _text(
        "status", "Run status; complete, since only successful runs write rows.", False
    ),
    _text(
        "completeness",
        "Window coverage; complete, since only complete windows are recorded.",
        False,
    ),
    Column(
        "observation_sequence",
        "bigint",
        True,
        description="Ledger-era sequence; null for runs of the dlt source.",
    ),
    Column(
        "observed_started_at",
        "timestamp",
        True,
        description="UTC instant extraction started reading the window.",
    ),
    Column(
        "observed_finished_at",
        "timestamp",
        True,
        description="UTC instant extraction finished reading the window.",
    ),
    Column(
        "completed_at",
        "timestamp",
        True,
        description="UTC instant the run recorded this evidence after its load.",
    ),
    Column(
        "orders_observed",
        "bigint",
        True,
        description="Complete orders the run read and loaded.",
    ),
    Column(
        "orders_applied",
        "bigint",
        True,
        description="Ledger-era applied count; null for runs of the dlt source.",
    ),
    Column(
        "exceptions",
        "bigint",
        True,
        description="Orders the run wrote to shopify_order_rejects.",
    ),
    Column(
        "anomalies",
        "bigint",
        True,
        description="Ledger-era anomaly count; null for runs of the dlt source.",
    ),
    _text("load_ids", "Comma-separated dlt load IDs the run committed."),
    _text("api_version", "Shopify Admin API version the run used.", False),
    _text("query_version", "Fingerprint of the GraphQL query documents used.", False),
    Column(
        "extraction_revision",
        "bigint",
        True,
        description="Raw contract revision of the run's parser.",
    ),
]

DATASET_DESCRIPTION = (
    "Raw Shopify orders and their child collections, with run evidence, "
    "rejects and deletion status, as the dlt order source loads them."
)
# Physical table -> meaning. Columns are described where they are declared.
TABLE_DESCRIPTIONS = {
    "orders": "One row per (shop_id, order_id): the latest complete observation.",
    "orders__lines": "One row per order line, replaced with its parent order.",
    "orders__refunds": "One row per refund, replaced with its parent order.",
    "orders__refunds__lines": "One row per refunded order line within a refund.",
    "orders__refunds__adjustments": "One row per order adjustment within a refund.",
    "orders__refunds__shipping_lines": "One row per refunded shipping line.",
    "orders__refunds__transactions": (
        "One row per (refund, transaction): links a refund to its refund "
        "transactions in orders__transactions."
    ),
    "orders__refunds__duties": "One row per refunded duty, by position.",
    "orders__transactions": (
        "One row per order transaction, of every kind and status, replaced "
        "with its parent order."
    ),
    "orders__tags": "One row per order tag; revision 2 and later.",
    "orders__discount_codes": "One row per order discount code; revision 2 and later.",
    "orders__discount_applications": (
        "One row per discount application, keyed by its order-scoped index."
    ),
    "orders__tax_lines": "One row per order-level tax line, by position.",
    "orders__lines__tax_lines": "One row per tax line on an order line, by position.",
    "orders__lines__discount_allocations": (
        "One row per discount allocated to an order line, by position."
    ),
    "orders__shipping_lines": (
        "One row per shipping line, including removed ones; revision 2 and later."
    ),
    "orders__shipping_lines__tax_lines": (
        "One row per tax line on a shipping line, by position."
    ),
    "orders__shipping_lines__discount_allocations": (
        "One row per discount allocated to a shipping line, by position."
    ),
    "orders__agreements": (
        "One row per sales agreement: the order, an edit, a refund or a return."
    ),
    "orders__agreements__sales": "One row per sale recorded by an agreement.",
    "orders__agreements__sales__taxes": "One row per tax charged on a sale.",
    SOURCE_STATUS_TABLE: (
        "One row per order the weekly count reconciliation found missing from, "
        "or back in, Shopify. Raw orders are never deleted."
    ),
    BATCH_EVIDENCE_TABLE: (
        "One row per successful ingestion run, written after its orders load."
    ),
    "shopify_order_rejects": (
        "One row per order version that failed the field contract; open until "
        "a version at least as new loads."
    ),
}


@dataclass(frozen=True)
class RefundChildren:
    """Complete, paginated refund child connections from ordinary GraphQL."""

    lines: list[JsonObject]
    adjustments: list[JsonObject]
    shipping_lines: list[JsonObject]
    transactions: list[JsonObject]


@dataclass(frozen=True)
class OrderException:
    order_id: str
    updated_at: datetime
    reason: str


@dataclass(frozen=True)
class ParsedOrder:
    """A complete order observation ready to load."""

    order_id: str
    created_at: datetime
    updated_at: datetime
    content: Row  # rows without metadata, children sorted by stable ID


def _sorted(rows: list[Row], key: str) -> list[Row]:
    return sorted(rows, key=lambda row: str(row[key]))


def _source_id(data: JsonObject, key: str = "id") -> str:
    """Interfaces can use concrete or interface GID types; validate the GID."""
    value = required_str(data, key)
    parts = value.split("/")
    if len(parts) != 5:
        raise ContractError(f"Expected Shopify ID: {key}")
    return gid(value, parts[3], key)


def _number(data: JsonObject, key: str) -> Decimal | None:
    value = selected_value(data, key)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ContractError(f"Expected numeric value: {key}")
    return parse_decimal(format(Decimal(str(value)), "f"), key)


def _nullable_bool(data: JsonObject, key: str) -> bool | None:
    return None if selected_value(data, key) is None else required_bool(data, key)


def _nullable_object(data: JsonObject, key: str) -> JsonObject | None:
    value = selected_value(data, key)
    return None if value is None else as_object(value, key)


def _child(base: Row, fields: Sequence[Field], data: JsonObject) -> Row:
    return {**base, **_read(fields, data)}


def _ids(row: Row, *names: str) -> Row:
    return {name: row[name] for name in ("shop_id", "order_id", *names)}


def _positioned(
    base: Row, fields: Sequence[Field], items: Sequence[JsonObject]
) -> list[Row]:
    return [
        {**_child(base, fields, item), "position": position}
        for position, item in enumerate(items)
    ]


def _tax_rows(base: Row, value: object) -> list[Row]:
    return _positioned(base, TAX_FIELDS, objects(value, "taxLines"))


def _allocations(base: Row, value: object) -> list[Row]:
    return _positioned(base, ALLOCATION_FIELDS, objects(value, "discountAllocations"))


def _shipping(base: Row, item: JsonObject) -> Row:
    row = _child(base, SHIPPING_FIELDS, item)
    base = _ids(row, "shipping_line_id")
    return {
        **row,
        "tax_lines": _tax_rows(base, item.get("taxLines")),
        "discount_allocations": _allocations(base, item.get("discountAllocations")),
    }


def _sale(base: Row, item: JsonObject) -> Row:
    row = _child(base, SALE_FIELDS, item)
    base = _ids(row, "agreement_id", "sale_id")
    taxes = [
        {
            **_tax_rows(base, [tax.get("taxLine")])[0],
            **_read(SALE_TAX_FIELDS, tax),
            "position": position,
        }
        for position, tax in enumerate(objects(item.get("taxes"), "sale taxes"))
    ]
    return {**row, "taxes": _sorted(taxes, "sale_tax_id")}


def _agreement(base: Row, item: JsonObject) -> Row:
    row = _child(base, AGREEMENT_FIELDS, item)
    base = _ids(row, "agreement_id")
    sales = [_sale(base, sale) for sale in objects(item.get("sales"), "sales")]
    return {**row, "sales": _sorted(sales, "sale_id")}


def _order_children(base: Row, data: JsonObject) -> Row:
    discounts = [
        _child(base, DISCOUNT_FIELDS, item)
        for item in objects(data.get("discountApplications"), "discountApplications")
    ]
    shipping = [
        _shipping(base, item)
        for item in objects(data.get("shippingLines"), "shippingLines")
    ]
    agreements = [
        _agreement(base, item) for item in objects(data.get("agreements"), "agreements")
    ]
    result: Row = {
        "tax_lines": _tax_rows(base, data.get("taxLines")),
        "discount_applications": sorted(
            discounts, key=lambda row: int(str(row["application_index"]))
        ),
        "shipping_lines": _sorted(shipping, "shipping_line_id"),
        "agreements": _sorted(agreements, "agreement_id"),
    }
    for source, target, key in (
        ("tags", "tags", "tag"),
        ("discountCodes", "discount_codes", "code"),
    ):
        strings: list[str] = []
        for value in as_list(data.get(source), source):
            if not isinstance(value, str):
                raise ContractError(f"Expected string: {source}")
            strings.append(value)
        result[target] = [{**base, key: value} for value in sorted(strings)]
    return result


def _line_row(base: Row, data: JsonObject) -> Row:
    row = _child(base, LINE_FIELDS, data)
    base = _ids(row, "line_item_id")
    return {
        **row,
        "tax_lines": _tax_rows(base, data.get("taxLines")),
        "discount_allocations": _allocations(base, data.get("discountAllocations")),
    }


def _refund_row(base: Row, data: JsonObject, children: RefundChildren) -> Row:
    row = _child(base, REFUND_FIELDS, data)
    base = _ids(row, "refund_id")
    lines = [_child(base, REFUND_LINE_FIELDS, item) for item in children.lines]
    adjustments = [
        _child(base, REFUND_ADJUSTMENT_FIELDS, item) for item in children.adjustments
    ]
    shipping = [
        _child(base, REFUND_SHIPPING_FIELDS, item) for item in children.shipping_lines
    ]
    transactions = [
        {
            **base,
            "transaction_id": gid(
                item.get("id"), "OrderTransaction", "refund transaction"
            ),
        }
        for item in children.transactions
    ]
    duties = objects(data.get("duties"), "refund duties")
    return {
        **row,
        "transactions": _sorted(transactions, "transaction_id"),
        "duties": _positioned(base, REFUND_DUTY_FIELDS, duties),
        "lines": _sorted(lines, "refund_line_item_id"),
        "adjustments": _sorted(adjustments, "adjustment_id"),
        "shipping_lines": _sorted(shipping, "refund_shipping_line_id"),
    }


def order_identity(data: JsonObject) -> tuple[str, datetime, datetime]:
    """Control fields; failures here invalidate the whole retained observation."""
    return (
        gid(data.get("id"), "Order", "order.id"),
        required_instant(data, "createdAt"),
        required_instant(data, "updatedAt"),
    )


def parse_order(
    shop_id: str,
    data: JsonObject,
    lines: Sequence[JsonObject],
    refund_children: Mapping[str, RefundChildren],
) -> ParsedOrder | OrderException:
    """Parse one complete order or explain why it cannot replace prior data."""
    order_id, created_at, updated_at = order_identity(data)
    try:
        count = data.get("transactionsCount")
        transactions = objects(data.get("transactions"), "transactions")
        if count is None:
            return OrderException(order_id, updated_at, "transactions_count_missing")
        count_data = as_object(count, "transactionsCount")
        precision = required_str(count_data, "precision")
        expected = required_int(count_data, "count")
        if precision != "EXACT" or expected != len(transactions):
            return OrderException(order_id, updated_at, "transactions_unverified")
        refunds = objects(data.get("refunds"), "refunds")
        if len(refunds) >= REFUND_LIMIT:
            return OrderException(order_id, updated_at, "refunds_at_limit")
        base: Row = {"shop_id": shop_id, "order_id": order_id}
        refund_rows: list[Row] = []
        for refund in refunds:
            refund_id = gid(refund.get("id"), "Refund", "refund.id")
            children = refund_children.get(refund_id)
            if children is None:
                return OrderException(order_id, updated_at, "refund_children_missing")
            refund_rows.append(_refund_row(base, refund, children))
        line_rows = [_line_row(base, line) for line in lines]
        transaction_rows = [
            _child(base, TRANSACTION_FIELDS, item) for item in transactions
        ]
        for rows, key in (
            (line_rows, "line_item_id"),
            (refund_rows, "refund_id"),
            (transaction_rows, "transaction_id"),
        ):
            if len({row[key] for row in rows}) != len(rows):
                return OrderException(order_id, updated_at, f"duplicate_{key}")
        content: Row = {
            **base,
            **_read(ORDER_FIELDS, data),
            "transactions_count": expected,
            "transactions_count_precision": precision,
            "lines": _sorted(line_rows, "line_item_id"),
            "refunds": _sorted(refund_rows, "refund_id"),
            "transactions": _sorted(transaction_rows, "transaction_id"),
            **_order_children(base, data),
        }
    except ContractError as error:
        return OrderException(order_id, updated_at, f"contract: {error}")
    return ParsedOrder(order_id, created_at, updated_at, content)
