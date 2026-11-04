"""Stage commercial evidence from raw-shaped DuckDB fixtures without business rules.

The raw relations copy the physical names and storage types declared by the
ingestion contract in ``packages/ingestion/.../shopify/model.py``. The staged
parents that the relationship tests reference (order lines and refunds) are
built too, so every foreign key runs against real staged rows.
"""

import json
import os
import shutil
import subprocess
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

# Real dbt project nodes these tests execute (ADR 0026).
pytestmark = pytest.mark.dbt("+path:models/staging/shopify_orders")

ROOT = Path(__file__).parents[1]
MONEY = "decimal(38,9)"
MODELS = [
    "stg_shopify_orders__agreements",
    "stg_shopify_orders__agreement_sales",
    "stg_shopify_orders__agreement_sale_taxes",
    "stg_shopify_orders__refund_lines",
    "stg_shopify_orders__refund_adjustments",
    "stg_shopify_orders__refund_shipping_lines",
    "stg_shopify_orders__refund_duties",
    "stg_shopify_orders__shipping_lines",
    "stg_shopify_orders__discount_applications",
    "stg_shopify_orders__line_discount_allocations",
    "stg_shopify_orders__shipping_line_discount_allocations",
    "stg_shopify_orders__line_tax_lines",
    "stg_shopify_orders__shipping_line_tax_lines",
    # Staged parents that the relationship tests reference.
    "stg_shopify_orders__order_lines",
    "stg_shopify_orders__refunds",
]
CURRENCY_TEST = "shopify_commercial_currencies_match"
EXPECTED_COUNTS = {
    "stg_shopify_orders__agreements": 2,
    "stg_shopify_orders__agreement_sales": 4,
    "stg_shopify_orders__agreement_sale_taxes": 2,
    "stg_shopify_orders__refund_lines": 1,
    "stg_shopify_orders__refund_adjustments": 2,
    "stg_shopify_orders__refund_shipping_lines": 2,
    "stg_shopify_orders__refund_duties": 2,
    "stg_shopify_orders__shipping_lines": 2,
    "stg_shopify_orders__discount_applications": 3,
    "stg_shopify_orders__line_discount_allocations": 2,
    "stg_shopify_orders__shipping_line_discount_allocations": 1,
    "stg_shopify_orders__line_tax_lines": 1,
    "stg_shopify_orders__shipping_line_tax_lines": 1,
}
Column = tuple[str, str]
Row = dict[str, object]

# Each (raw table, key column, staged unique test) must reject a duplicate row
# and a null key. The unique test declares the same columns as the model key.
KEYED = [
    # (raw table, raw key column, staged key column, staged model)
    (
        "orders__agreements",
        "agreement_id",
        "agreement_id",
        "stg_shopify_orders__agreements",
    ),
    (
        "orders__agreements__sales",
        "sale_id",
        "sale_id",
        "stg_shopify_orders__agreement_sales",
    ),
    (
        "orders__agreements__sales__taxes",
        "sale_tax_id",
        "sale_tax_id",
        "stg_shopify_orders__agreement_sale_taxes",
    ),
    (
        "orders__refunds__lines",
        "refund_line_item_id",
        "refund_line_item_id",
        "stg_shopify_orders__refund_lines",
    ),
    (
        "orders__refunds__adjustments",
        "adjustment_id",
        "adjustment_id",
        "stg_shopify_orders__refund_adjustments",
    ),
    (
        "orders__refunds__shipping_lines",
        "refund_shipping_line_id",
        "refund_shipping_line_id",
        "stg_shopify_orders__refund_shipping_lines",
    ),
    (
        "orders__refunds__duties",
        "position",
        "duty_position",
        "stg_shopify_orders__refund_duties",
    ),
    (
        "orders__shipping_lines",
        "shipping_line_id",
        "shipping_line_id",
        "stg_shopify_orders__shipping_lines",
    ),
    (
        "orders__discount_applications",
        "application_index",
        "application_index",
        "stg_shopify_orders__discount_applications",
    ),
    (
        "orders__lines__discount_allocations",
        "position",
        "allocation_position",
        "stg_shopify_orders__line_discount_allocations",
    ),
    (
        "orders__shipping_lines__discount_allocations",
        "position",
        "allocation_position",
        "stg_shopify_orders__shipping_line_discount_allocations",
    ),
    (
        "orders__lines__tax_lines",
        "position",
        "tax_line_position",
        "stg_shopify_orders__line_tax_lines",
    ),
    (
        "orders__shipping_lines__tax_lines",
        "position",
        "tax_line_position",
        "stg_shopify_orders__shipping_line_tax_lines",
    ),
]

# Each orphan must fail the named composite relationship test. The shop-2 case
# moves one child to a shop that has no parent, which proves shop scoping.
ORPHANS = [
    (
        "orders__agreements",
        "refund_id",
        "refund-missing",
        "shopify_agreements_refund_exists",
    ),
    (
        "orders__agreements__sales",
        "agreement_id",
        "agr-missing",
        "shopify_agreement_sales_agreement_exists",
    ),
    (
        "orders__agreements__sales",
        "shipping_line_id",
        "ship-missing",
        "shopify_agreement_sales_shipping_line_exists",
    ),
    (
        "orders__agreements__sales__taxes",
        "sale_id",
        "sale-missing",
        "shopify_agreement_sale_taxes_sale_exists",
    ),
    (
        "orders__refunds__lines",
        "refund_id",
        "refund-missing",
        "shopify_refund_lines_refund_exists",
    ),
    (
        "orders__refunds__lines",
        "shop_id",
        "shop-2",
        "shopify_refund_lines_refund_exists",
    ),
    (
        "orders__refunds__adjustments",
        "refund_id",
        "refund-missing",
        "shopify_refund_adjustments_refund_exists",
    ),
    (
        "orders__refunds__shipping_lines",
        "refund_id",
        "refund-missing",
        "shopify_refund_shipping_lines_refund_exists",
    ),
    (
        "orders__refunds__shipping_lines",
        "shipping_line_id",
        "ship-missing",
        "shopify_refund_shipping_lines_shipping_line_exists",
    ),
    (
        "orders__refunds__duties",
        "refund_id",
        "refund-missing",
        "shopify_refund_duties_refund_exists",
    ),
    (
        "orders__lines__discount_allocations",
        "application_index",
        9,
        "shopify_line_discount_allocations_application_exists",
    ),
    (
        "orders__shipping_lines__discount_allocations",
        "shipping_line_id",
        "ship-missing",
        "shopify_shipping_discount_allocations_shipping_line_exists",
    ),
    (
        "orders__shipping_lines__discount_allocations",
        "application_index",
        9,
        "shopify_shipping_discount_allocations_application_exists",
    ),
    (
        "orders__shipping_lines__tax_lines",
        "shipping_line_id",
        "ship-missing",
        "shopify_shipping_tax_lines_shipping_line_exists",
    ),
]


def _money_cols(prefix: str) -> list[Column]:
    return [
        (f"{prefix}_shop_amount", MONEY),
        (f"{prefix}_shop_currency", "varchar"),
        (f"{prefix}_presentment_amount", MONEY),
        (f"{prefix}_presentment_currency", "varchar"),
    ]


def _money(prefix: str, shop: str | None, presentment: str | None) -> Row:
    """A money bag in USD shop currency and CAD presentment currency."""
    return {
        f"{prefix}_shop_amount": None if shop is None else Decimal(shop),
        f"{prefix}_shop_currency": "USD",
        f"{prefix}_presentment_amount": None
        if presentment is None
        else Decimal(presentment),
        f"{prefix}_presentment_currency": "CAD",
    }


def _at(day: int, hour: int) -> datetime:
    """A Pacific-time instant; staging must store its UTC equivalent."""
    return datetime(2026, 7, day, hour, tzinfo=timezone(timedelta(hours=-7)))


SCOPE: Column = ("shop_id", "varchar")
KEY_COLS: list[Column] = [SCOPE, ("order_id", "varchar")]


def _cols(*groups: list[Column]) -> list[Column]:
    return [column for group in groups for column in group]


def _row(**values: object) -> Row:
    return {"shop_id": "shop-1", "order_id": "order-1", **values}


def commercial_raw_tables() -> dict[str, tuple[list[Column], list[Row]]]:
    """Physical raw relations as (columns, rows) for one order in shop-1."""
    tax_common = [
        ("title", "varchar"),
        ("rate", MONEY),
        ("channel_liable", "boolean"),
    ]
    agreement_sale_money = _cols(
        _money_cols("total_amount"),
        _money_cols("total_tax_amount"),
        _money_cols("total_discount_amount_before_taxes"),
        _money_cols("total_discount_amount_after_taxes"),
    )

    def sale_money(total: tuple[str, str], tax: tuple[str, str] = ("0", "0")) -> Row:
        return {
            **_money("total_amount", *total),
            **_money("total_tax_amount", *tax),
            **_money(
                "total_discount_amount_before_taxes", "0.000000000", "0.000000000"
            ),
            **_money("total_discount_amount_after_taxes", "0.000000000", "0.000000000"),
        }

    line_1 = _row(
        line_item_id="line-1",
        sku="SKU-1",
        name="Tee",
        title="Tee",
        variant_title="Blue / M",
        product_id="prod-1",
        variant_id="var-1",
        is_gift_card=False,
        quantity=2,
        current_quantity=1,
        **_money("original_unit_price", "50.000000000", "67.500000000"),
        **_money("discounted_total", "100.000000000", "135.000000000"),
    )
    line_2 = _row(
        line_item_id="line-2",
        sku="SKU-GC",
        name="Gift card",
        title="Gift card",
        variant_title=None,
        product_id=None,
        variant_id=None,
        is_gift_card=True,
        quantity=1,
        current_quantity=1,
        **_money("original_unit_price", "25.000000000", "33.750000000"),
        **_money("discounted_total", "25.000000000", "33.750000000"),
    )
    refund = _row(
        refund_id="refund-1",
        return_id=None,
        note="Private note",
        created_at=_at(2, 0),
        updated_at=_at(3, 10),
        processed_at=_at(2, 9),
        **_money("total_refunded", "30.000000000", "40.500000000"),
    )
    agreement_refund = _row(
        agreement_id="agr-1",
        type="RefundAgreement",
        happened_at=_at(2, 9),
        reason="refund",
        refund_id="refund-1",
        return_id=None,
    )
    agreement_return = _row(
        agreement_id="agr-2",
        type="ReturnAgreement",
        happened_at=_at(2, 10),
        reason="return",
        refund_id=None,
        return_id="return-1",
    )
    sale_product = _row(
        agreement_id="agr-1",
        sale_id="sale-1",
        type="ProductSale",
        action_type=" return ",
        line_type="product",
        quantity=-1,
        line_item_id="line-1",
        shipping_line_id=None,
        duty_id=None,
        fee_id=None,
        fee_type=None,
        additional_fee_id=None,
        additional_fee_name=None,
        **_money("additional_fee_price", None, None),
        **sale_money(("50.000000000", "67.500000000"), ("4.125000000", "5.568750000")),
    )
    sale_shipping = _row(
        agreement_id="agr-1",
        sale_id="sale-2",
        type="ShippingLineSale",
        action_type="REMOVAL",
        line_type="SHIPPING",
        quantity=None,
        line_item_id=None,
        shipping_line_id="ship-1",
        duty_id=None,
        fee_id=None,
        fee_type=None,
        additional_fee_id=None,
        additional_fee_name=None,
        **_money("additional_fee_price", None, None),
        **sale_money(("10.000000000", "13.500000000")),
    )
    sale_fee = _row(
        agreement_id="agr-2",
        sale_id="sale-3",
        type="AdditionalFeeSale",
        action_type="ADDITION",
        line_type="ADDITIONAL_FEE",
        quantity=None,
        line_item_id=None,
        shipping_line_id=None,
        duty_id=None,
        fee_id=None,
        fee_type=None,
        additional_fee_id="af-1",
        additional_fee_name="Restocking",
        **_money("additional_fee_price", "15.000000000", "20.250000000"),
        **sale_money(("15.000000000", "20.250000000")),
    )
    sale_source_fee = _row(
        agreement_id="agr-2",
        sale_id="sale-4",
        type="FeeSale",
        action_type="ADDITION",
        line_type="FEE",
        quantity=None,
        line_item_id=None,
        shipping_line_id=None,
        duty_id=None,
        fee_id="fee-1",
        fee_type="ReturnFee",
        additional_fee_id=None,
        additional_fee_name=None,
        **_money("additional_fee_price", None, None),
        **sale_money(("5.000000000", "6.750000000")),
    )
    sale_tax = _row(
        agreement_id="agr-1",
        sale_id="sale-1",
        sale_tax_id="stax-1",
        position=0,
        title="CA Sales Tax",
        rate=Decimal("0.082500000"),
        channel_liable=False,
        **_money("amount", "4.125000000", "5.568750000"),
        **_money("price", "4.125000000", "5.568750000"),
    )
    sale_tax_2 = _row(
        agreement_id="agr-1",
        sale_id="sale-1",
        sale_tax_id="stax-2",
        position=1,
        title="Stamp",
        rate=None,
        channel_liable=None,
        **_money("amount", "0.000000001", "0.000000001"),
        **_money("price", "0.000000001", "0.000000001"),
    )
    refund_line = _row(
        refund_id="refund-1",
        refund_line_item_id="rli-1",
        line_item_id="line-1",
        quantity=1,
        **_money("subtotal", "50.000000000", "67.500000000"),
        **_money("total_tax", "4.125000000", "5.568750000"),
    )
    adjustment_fee = _row(
        refund_id="refund-1",
        adjustment_id="adj-1",
        reason="return handling fee",
        **_money("amount", "15.000000000", "20.250000000"),
        **_money("tax_amount", "0.000000000", "0.000000000"),
    )
    adjustment_negative = _row(
        refund_id="refund-1",
        adjustment_id="adj-2",
        reason=None,
        **_money("amount", "-4.000000000", "-5.400000000"),
        **_money("tax_amount", "0.000000000", "0.000000000"),
    )
    shipping_refund = _row(
        refund_id="refund-1",
        refund_shipping_line_id="rsl-1",
        shipping_line_id="ship-1",
        **_money("subtotal_amount", "10.000000000", "13.500000000"),
        **_money("tax_amount", "0.000000000", "0.000000000"),
    )
    shipping_refund_unlinked = _row(
        refund_id="refund-1",
        refund_shipping_line_id="rsl-2",
        shipping_line_id=None,
        **_money("subtotal_amount", "0.000000000", "0.000000000"),
        **_money("tax_amount", "0.000000000", "0.000000000"),
    )
    duty_refund = _row(
        refund_id="refund-1",
        position=0,
        duty_id="duty-1",
        **_money("amount", "3.000000000", "4.050000000"),
    )
    duty_refund_unlinked = _row(
        refund_id="refund-1",
        position=1,
        duty_id=None,
        **_money("amount", "1.000000000", "1.350000000"),
    )
    discount_code = _row(
        application_index=0,
        type="DiscountCodeApplication",
        allocation_method="ACROSS",
        target_selection="ALL",
        target_type="LINE_ITEM",
        code="SAVE10",
        title=None,
        description=None,
        value_type="MoneyV2",
        value_amount=Decimal("10.000000000"),
        value_currency="USD",
        value_percentage=None,
    )
    discount_free_shipping = _row(
        application_index=1,
        type="AutomaticDiscountApplication",
        allocation_method="EACH",
        target_selection="ENTITLED",
        target_type=" shipping_line ",
        code=None,
        title="Free shipping",
        description=None,
        value_type="PricingPercentageValue",
        value_amount=None,
        value_currency=None,
        value_percentage=Decimal("100.000000000"),
    )
    discount_manual = _row(
        application_index=2,
        type="ManualDiscountApplication",
        allocation_method="ONE",
        target_selection="EXPLICIT",
        target_type="LINE_ITEM",
        code=None,
        title="Goodwill",
        description="Manual adjustment",
        value_type="MoneyV2",
        value_amount=Decimal("3.333333333"),
        value_currency="USD",
        value_percentage=None,
    )
    line_alloc_discount = _row(
        line_item_id="line-1",
        position=0,
        application_index=0,
        **_money("allocated_amount", "6.666666667", "9.000000000"),
    )
    line_alloc_manual = _row(
        line_item_id="line-1",
        position=1,
        application_index=2,
        **_money("allocated_amount", "3.333333333", "4.500000000"),
    )
    shipping_alloc = _row(
        shipping_line_id="ship-1",
        position=0,
        application_index=1,
        **_money("allocated_amount", "10.000000000", "13.500000000"),
    )
    shipping_1 = _row(
        shipping_line_id="ship-1",
        title="Standard",
        code="STANDARD",
        source="shopify",
        is_removed=False,
        **_money("original_price", "10.000000000", "13.500000000"),
        **_money("discounted_price", "0.000000000", "0.000000000"),
        **_money("current_discounted_price", "0.000000000", "0.000000000"),
    )
    shipping_2 = _row(
        shipping_line_id="ship-2",
        title="Express",
        code=None,
        source=None,
        is_removed=True,
        **_money("original_price", "20.000000000", "27.000000000"),
        **_money("discounted_price", "20.000000000", "27.000000000"),
        **_money("current_discounted_price", "0.000000000", "0.000000000"),
    )
    line_tax = _row(
        line_item_id="line-1",
        position=0,
        title="CA Sales Tax",
        rate=Decimal("0.082500000"),
        channel_liable=None,
        **_money("price", "8.250000000", "11.137500000"),
    )
    shipping_tax = _row(
        shipping_line_id="ship-1",
        position=0,
        title="Shipping tax",
        rate=None,
        channel_liable=True,
        **_money("price", "0.123456789", "0.166666662"),
    )

    shop_key = KEY_COLS
    return {
        "orders": (
            [
                *shop_key,
                ("currency_code", "varchar"),
                ("presentment_currency_code", "varchar"),
            ],
            [
                {
                    "shop_id": "shop-1",
                    "order_id": "order-1",
                    "currency_code": "USD",
                    "presentment_currency_code": "CAD",
                }
            ],
        ),
        "orders__lines": (
            _cols(
                KEY_COLS,
                [
                    ("line_item_id", "varchar"),
                    ("sku", "varchar"),
                    ("name", "varchar"),
                    ("title", "varchar"),
                    ("variant_title", "varchar"),
                    ("product_id", "varchar"),
                    ("variant_id", "varchar"),
                    ("is_gift_card", "boolean"),
                    ("quantity", "decimal(19, 0)"),
                    ("current_quantity", "decimal(19, 0)"),
                ],
                _money_cols("original_unit_price"),
                _money_cols("discounted_total"),
            ),
            [line_1, line_2],
        ),
        "orders__refunds": (
            _cols(
                KEY_COLS,
                [
                    ("refund_id", "varchar"),
                    ("return_id", "varchar"),
                    ("created_at", "timestamptz"),
                    ("updated_at", "timestamptz"),
                    ("processed_at", "timestamptz"),
                    ("note", "varchar"),
                ],
                _money_cols("total_refunded"),
            ),
            [refund],
        ),
        "orders__agreements": (
            _cols(
                KEY_COLS,
                [
                    ("agreement_id", "varchar"),
                    ("type", "varchar"),
                    ("happened_at", "timestamptz"),
                    ("reason", "varchar"),
                    ("refund_id", "varchar"),
                    ("return_id", "varchar"),
                ],
            ),
            [agreement_refund, agreement_return],
        ),
        "orders__agreements__sales": (
            _cols(
                KEY_COLS,
                [
                    ("agreement_id", "varchar"),
                    ("sale_id", "varchar"),
                    ("type", "varchar"),
                    ("action_type", "varchar"),
                    ("line_type", "varchar"),
                    ("quantity", "decimal(19, 0)"),
                    ("line_item_id", "varchar"),
                    ("shipping_line_id", "varchar"),
                    ("duty_id", "varchar"),
                    ("fee_id", "varchar"),
                    ("fee_type", "varchar"),
                    ("additional_fee_id", "varchar"),
                    ("additional_fee_name", "varchar"),
                ],
                _money_cols("additional_fee_price"),
                agreement_sale_money,
            ),
            [sale_product, sale_shipping, sale_fee, sale_source_fee],
        ),
        "orders__agreements__sales__taxes": (
            _cols(
                KEY_COLS,
                [
                    ("agreement_id", "varchar"),
                    ("sale_id", "varchar"),
                    ("sale_tax_id", "varchar"),
                    ("position", "decimal(19, 0)"),
                ],
                tax_common,
                _money_cols("amount"),
                _money_cols("price"),
            ),
            [sale_tax, sale_tax_2],
        ),
        "orders__refunds__lines": (
            _cols(
                KEY_COLS,
                [
                    ("refund_id", "varchar"),
                    ("refund_line_item_id", "varchar"),
                    ("line_item_id", "varchar"),
                    ("quantity", "decimal(19, 0)"),
                ],
                _money_cols("subtotal"),
                _money_cols("total_tax"),
            ),
            [refund_line],
        ),
        "orders__refunds__adjustments": (
            _cols(
                KEY_COLS,
                [
                    ("refund_id", "varchar"),
                    ("adjustment_id", "varchar"),
                    ("reason", "varchar"),
                ],
                _money_cols("amount"),
                _money_cols("tax_amount"),
            ),
            [adjustment_fee, adjustment_negative],
        ),
        "orders__refunds__shipping_lines": (
            _cols(
                KEY_COLS,
                [
                    ("refund_id", "varchar"),
                    ("refund_shipping_line_id", "varchar"),
                    ("shipping_line_id", "varchar"),
                ],
                _money_cols("subtotal_amount"),
                _money_cols("tax_amount"),
            ),
            [shipping_refund, shipping_refund_unlinked],
        ),
        "orders__refunds__duties": (
            _cols(
                KEY_COLS,
                [
                    ("refund_id", "varchar"),
                    ("position", "decimal(19, 0)"),
                    ("duty_id", "varchar"),
                ],
                _money_cols("amount"),
            ),
            [duty_refund, duty_refund_unlinked],
        ),
        "orders__discount_applications": (
            _cols(
                KEY_COLS,
                [
                    ("application_index", "decimal(19, 0)"),
                    ("type", "varchar"),
                    ("allocation_method", "varchar"),
                    ("target_selection", "varchar"),
                    ("target_type", "varchar"),
                    ("code", "varchar"),
                    ("title", "varchar"),
                    ("description", "varchar"),
                    ("value_type", "varchar"),
                    ("value_amount", MONEY),
                    ("value_currency", "varchar"),
                    ("value_percentage", MONEY),
                ],
            ),
            [discount_code, discount_free_shipping, discount_manual],
        ),
        "orders__lines__discount_allocations": (
            _cols(
                KEY_COLS,
                [
                    ("line_item_id", "varchar"),
                    ("position", "decimal(19, 0)"),
                    ("application_index", "decimal(19, 0)"),
                ],
                _money_cols("allocated_amount"),
            ),
            [line_alloc_discount, line_alloc_manual],
        ),
        "orders__shipping_lines__discount_allocations": (
            _cols(
                KEY_COLS,
                [
                    ("shipping_line_id", "varchar"),
                    ("position", "decimal(19, 0)"),
                    ("application_index", "decimal(19, 0)"),
                ],
                _money_cols("allocated_amount"),
            ),
            [shipping_alloc],
        ),
        "orders__lines__tax_lines": (
            _cols(
                KEY_COLS,
                [("line_item_id", "varchar"), ("position", "decimal(19, 0)")],
                tax_common,
                _money_cols("price"),
            ),
            [line_tax],
        ),
        "orders__shipping_lines__tax_lines": (
            _cols(
                KEY_COLS,
                [("shipping_line_id", "varchar"), ("position", "decimal(19, 0)")],
                tax_common,
                _money_cols("price"),
            ),
            [shipping_tax],
        ),
        "orders__shipping_lines": (
            _cols(
                KEY_COLS,
                [
                    ("shipping_line_id", "varchar"),
                    ("title", "varchar"),
                    ("code", "varchar"),
                    ("source", "varchar"),
                    ("is_removed", "boolean"),
                ],
                _money_cols("original_price"),
                _money_cols("discounted_price"),
                _money_cols("current_discounted_price"),
            ),
            [shipping_1, shipping_2],
        ),
    }


def _create(
    db: duckdb.DuckDBPyConnection, table: str, columns: list[Column], rows: list[Row]
) -> None:
    ddl = ", ".join(f"{name} {kind}" for name, kind in columns)
    _ = db.execute(f"create table raw.{table} ({ddl})")
    names = [name for name, _ in columns]
    values = [tuple(row.get(name) for name in names) for row in rows]
    if values:
        placeholders = ", ".join("?" for _ in names)
        db.executemany(f"insert into raw.{table} values ({placeholders})", values)


def _raw(path: Path) -> None:
    with duckdb.connect(str(path)) as db:
        _ = db.execute("create schema raw")
        for table, (columns, rows) in commercial_raw_tables().items():
            _create(db, table, columns, rows)


def _dbt(command: str, path: Path, target: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "dbt",
            command,
            "--select",
            *MODELS,
            # Named tests always run. The commercial currency test reads raw
            # orders through a source, so cautious selection would skip it.
            CURRENCY_TEST,
            "tag:shopify_commercial_cross_model",
            # Run a test only when all of its parents are selected. Otherwise
            # an existing order-level test runs against this fixture's partial
            # raw orders relation.
            "--indirect-selection",
            "cautious",
            "--quiet",
            "--warn-error-options",
            '{"error": ["NoNodesForSelectionCriteria"]}',
            "--project-dir",
            str(ROOT / "dbt"),
            "--profiles-dir",
            str(ROOT / "dbt"),
            "--target-path",
            str(target),
            "--log-path",
            str(target / "logs"),
        ],
        env=os.environ
        | {
            "DBT_TARGET": "local",
            "DBT_NAMESPACE": "commercial",
            "DUCKDB_PATH": str(path),
            "SHOPIFY_ORDERS_DATASET": "raw",
        },
        timeout=180,
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.fixture(scope="module")
def built(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """One staged fixture; staging models are views, so copies follow raw edits."""
    raw = tmp_path_factory.mktemp("commercial") / "warehouse.duckdb"
    _raw(raw)
    result = _dbt("build", raw, raw.parent / "target")
    assert result.returncode == 0, _output(result)
    return raw


def _copy(built: Path, tmp_path: Path) -> Path:
    # Same file name: the views name the database after it.
    raw = tmp_path / built.name
    _ = shutil.copyfile(built, raw)
    return raw


def _failed_tests(raw: Path, target: Path) -> set[str]:
    """Run every selected test against the edited raw data; return the failures."""
    result = _dbt("test", raw, target)
    results = json.loads((target / "run_results.json").read_text())  # pyright: ignore[reportAny] # dbt artifact read in a test.
    failed = {
        str(item["unique_id"]).split(".")[2]  # pyright: ignore[reportAny]
        for item in results["results"]  # pyright: ignore[reportAny]
        if item["status"] in ("fail", "error")
    }
    assert (result.returncode != 0) == bool(failed), _output(result)
    return failed


def _output(result: subprocess.CompletedProcess[str]) -> str:
    return result.stdout + result.stderr


def test_staging_preserves_commercial_evidence(built: Path) -> None:
    raw = built
    with duckdb.connect(str(raw), read_only=True) as db:
        for model, count in EXPECTED_COUNTS.items():
            assert db.execute(
                f"select count(*) from commercial_staging.{model}"
            ).fetchone() == (count,), model

        # dlt stores Snowflake integers as NUMBER(19,0); staging must expose
        # the declared bigint rather than leak the raw numeric precision.
        assert db.execute("""
            select data_type from information_schema.columns
            where table_schema = 'commercial_staging'
                and table_name = 'stg_shopify_orders__agreement_sales'
                and column_name = 'signed_quantity'
        """).fetchone() == ("BIGINT",)

        assert (
            db.execute("""
            select table_name, column_name from information_schema.columns
            where table_schema = 'commercial_staging'
                and data_type = 'DECIMAL(19,0)'
        """).fetchall()
            == []
        )

        # Actions and line types are trimmed and upper-cased.
        # Unknown values stay visible.
        assert db.execute("""
            select sale_id, sale_type, action_type, line_type, signed_quantity,
                line_item_id, shipping_line_id,
                total_amount_shop_amount, total_amount_shop_currency_code,
                total_amount_presentment_amount, total_amount_presentment_currency_code
            from commercial_staging.stg_shopify_orders__agreement_sales order by sale_id
        """).fetchall() == [
            (
                "sale-1",
                "ProductSale",
                "RETURN",
                "PRODUCT",
                -1,
                "line-1",
                None,
                Decimal("50"),
                "USD",
                Decimal("67.5"),
                "CAD",
            ),
            (
                "sale-2",
                "ShippingLineSale",
                "REMOVAL",
                "SHIPPING",
                None,
                None,
                "ship-1",
                Decimal("10"),
                "USD",
                Decimal("13.5"),
                "CAD",
            ),
            (
                "sale-3",
                "AdditionalFeeSale",
                "ADDITION",
                "ADDITIONAL_FEE",
                None,
                None,
                None,
                Decimal("15"),
                "USD",
                Decimal("20.25"),
                "CAD",
            ),
            (
                "sale-4",
                "FeeSale",
                "ADDITION",
                "FEE",
                None,
                None,
                None,
                Decimal("5"),
                "USD",
                Decimal("6.75"),
                "CAD",
            ),
        ]
        assert db.execute("""
            select sale_id, additional_fee_id, additional_fee_name,
                additional_fee_price_shop_amount, fee_id, fee_type
            from commercial_staging.stg_shopify_orders__agreement_sales
            where sale_id in ('sale-3', 'sale-4') order by sale_id
        """).fetchall() == [
            ("sale-3", "af-1", "Restocking", Decimal("15"), None, None),
            ("sale-4", None, None, None, "fee-1", "ReturnFee"),
        ]

        # Source instants are stored as the UTC equivalent of the source time.
        assert db.execute("""
            select agreement_id, agreement_type, happened_at,
                reason, refund_id, return_id
            from commercial_staging.stg_shopify_orders__agreements order by agreement_id
        """).fetchall() == [
            (
                "agr-1",
                "RefundAgreement",
                datetime(2026, 7, 2, 16),
                "refund",
                "refund-1",
                None,
            ),
            (
                "agr-2",
                "ReturnAgreement",
                datetime(2026, 7, 2, 17),
                "return",
                None,
                "return-1",
            ),
        ]

        # Sale taxes keep the tax amount and the tax-line price separately.
        assert db.execute("""
            select sale_tax_id, sale_tax_position, tax_title,
                tax_rate, is_channel_liable,
                sale_tax_shop_amount, tax_price_shop_amount
            from commercial_staging.stg_shopify_orders__agreement_sale_taxes
            order by sale_tax_id
        """).fetchall() == [
            (
                "stax-1",
                0,
                "CA Sales Tax",
                Decimal("0.0825"),
                False,
                Decimal("4.125"),
                Decimal("4.125"),
            ),
            (
                "stax-2",
                1,
                "Stamp",
                None,
                None,
                Decimal("0.000000001"),
                Decimal("0.000000001"),
            ),
        ]

        # Signed adjustments and unlinked optional references keep source values.
        assert db.execute("""
            select adjustment_id, adjustment_reason, adjustment_shop_amount,
                adjustment_presentment_amount
            from commercial_staging.stg_shopify_orders__refund_adjustments
            order by adjustment_id
        """).fetchall() == [
            ("adj-1", "return handling fee", Decimal("15"), Decimal("20.25")),
            ("adj-2", None, Decimal("-4"), Decimal("-5.4")),
        ]
        assert db.execute("""
            select refund_shipping_line_id, shipping_line_id, subtotal_shop_amount
            from commercial_staging.stg_shopify_orders__refund_shipping_lines
            order by refund_shipping_line_id
        """).fetchall() == [
            ("rsl-1", "ship-1", Decimal("10")),
            ("rsl-2", None, Decimal("0")),
        ]
        assert db.execute("""
            select duty_position, duty_id, refund_shop_amount
            from commercial_staging.stg_shopify_orders__refund_duties
            order by duty_position
        """).fetchall() == [
            (0, "duty-1", Decimal("3")),
            (1, None, Decimal("1")),
        ]

        # Removed shipping lines are retained and keep their source prices.
        assert db.execute("""
            select shipping_line_id, shipping_title, shipping_code, shipping_source,
                is_removed, discounted_price_shop_amount
            from commercial_staging.stg_shopify_orders__shipping_lines
            order by shipping_line_id
        """).fetchall() == [
            ("ship-1", "Standard", "STANDARD", "shopify", False, Decimal("0")),
            ("ship-2", "Express", None, None, True, Decimal("20")),
        ]

        # Discount values keep their own currency or percentage, not both.
        assert db.execute("""
            select application_index, discount_application_type,
                target_type, discount_code,
                discount_value_type, discount_value_amount,
                    discount_value_currency_code,
                discount_value_percentage
            from commercial_staging.stg_shopify_orders__discount_applications
            order by application_index
        """).fetchall() == [
            (
                0,
                "DiscountCodeApplication",
                "LINE_ITEM",
                "SAVE10",
                "MoneyV2",
                Decimal("10"),
                "USD",
                None,
            ),
            (
                1,
                "AutomaticDiscountApplication",
                "SHIPPING_LINE",
                None,
                "PricingPercentageValue",
                None,
                None,
                Decimal("100"),
            ),
            (
                2,
                "ManualDiscountApplication",
                "LINE_ITEM",
                None,
                "MoneyV2",
                Decimal("3.333333333"),
                "USD",
                None,
            ),
        ]

        # Exact nine-place decimals survive staging for each allocation and tax line.
        assert db.execute("""
            select line_item_id, allocation_position, application_index,
                allocated_amount_shop_amount, allocated_amount_presentment_amount
            from commercial_staging.stg_shopify_orders__line_discount_allocations
            order by allocation_position
        """).fetchall() == [
            ("line-1", 0, 0, Decimal("6.666666667"), Decimal("9")),
            ("line-1", 1, 2, Decimal("3.333333333"), Decimal("4.5")),
        ]
        assert db.execute("""
            select shipping_line_id, tax_title, tax_rate, is_channel_liable,
                tax_price_shop_amount
            from commercial_staging.stg_shopify_orders__shipping_line_tax_lines
        """).fetchall() == [
            ("ship-1", "Shipping tax", None, True, Decimal("0.123456789"))
        ]


def _orphan_waves() -> list[list[tuple[str, str, object, str]]]:
    """Group orphans so that each relationship test has one defect per run."""
    waves: list[list[tuple[str, str, object, str]]] = []
    for orphan in ORPHANS:
        wave = next(
            (w for w in waves if orphan[3] not in {item[3] for item in w}), None
        )
        if wave is None:
            waves.append([orphan])
        else:
            wave.append(orphan)
    return waves


def test_staging_rejects_duplicate_keys(built: Path, tmp_path: Path) -> None:
    # Every keyed table gets a duplicate; each unique test must fail, and only
    # those tests may fail.
    raw = _copy(built, tmp_path)
    with duckdb.connect(str(raw)) as db:
        for table, _, _, _ in KEYED:
            _ = db.execute(f"insert into raw.{table} select * from raw.{table} limit 1")
    assert _failed_tests(raw, tmp_path / "target") == {
        f"shopify_staged_{model.removeprefix('stg_shopify_orders__')}_unique_key"
        for _, _, _, model in KEYED
    }


def test_staging_rejects_null_keys(built: Path, tmp_path: Path) -> None:
    raw = _copy(built, tmp_path)
    with duckdb.connect(str(raw)) as db:
        for table, raw_key, _, _ in KEYED:
            _ = db.execute(
                f"update raw.{table} set {raw_key} = null "
                f"where rowid = (select min(rowid) from raw.{table})"
            )
    failed = _failed_tests(raw, tmp_path / "target")
    expected = {f"not_null_{model}_{staged_key}" for _, _, staged_key, model in KEYED}
    assert expected <= failed
    # A null parent key also orphans its children; nothing else may fail.
    assert {name for name in failed - expected if not name.endswith("_exists")} == set()


@pytest.mark.parametrize("wave", _orphan_waves())
def test_relationships_reject_orphans(
    built: Path, tmp_path: Path, wave: list[tuple[str, str, object, str]]
) -> None:
    raw = _copy(built, tmp_path)
    with duckdb.connect(str(raw)) as db:
        for table, column, value, _ in wave:
            _ = db.execute(
                f"update raw.{table} set {column} = ? "
                f"where rowid = (select min(rowid) from raw.{table})",
                [value],
            )
    # Moving a child to another shop also orphans it from other parents.
    assert {test for *_, test in wave} <= _failed_tests(raw, tmp_path / "target")


def test_currency_mismatch_fails(built: Path, tmp_path: Path) -> None:
    raw = _copy(built, tmp_path)
    with duckdb.connect(str(raw)) as db:
        _ = db.execute(
            "update raw.orders__refunds__adjustments "
            "set amount_presentment_currency = 'EUR' "
            "where adjustment_id = 'adj-1'"
        )
    assert _failed_tests(raw, tmp_path / "target") == {CURRENCY_TEST}


def test_empty_sources_build_without_rows(built: Path, tmp_path: Path) -> None:
    raw = _copy(built, tmp_path)
    with duckdb.connect(str(raw)) as db:
        for table in commercial_raw_tables():
            _ = db.execute(f"delete from raw.{table}")
    assert _failed_tests(raw, tmp_path / "target") == set()
    with duckdb.connect(str(raw), read_only=True) as db:
        for model in EXPECTED_COUNTS:
            assert db.execute(
                f"select count(*) from commercial_staging.{model}"
            ).fetchone() == (0,), model


def test_second_shop_is_scoped_separately(built: Path, tmp_path: Path) -> None:
    raw = _copy(built, tmp_path)
    with duckdb.connect(str(raw)) as db:
        for table in commercial_raw_tables():
            _ = db.execute(
                f"insert into raw.{table} "
                f"select * replace ('shop-2' as shop_id) from raw.{table}"
            )
    assert _failed_tests(raw, tmp_path / "target") == set()
    with duckdb.connect(str(raw), read_only=True) as db:
        for model, count in EXPECTED_COUNTS.items():
            assert db.execute(
                f"select count(*) from commercial_staging.{model}"
                " where shop_id = 'shop-2'"
            ).fetchone() == (count,), model
