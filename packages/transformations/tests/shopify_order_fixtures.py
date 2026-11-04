"""Source-shaped synthetic Shopify order fixtures; no live order data."""

from collections.abc import Sequence
from decimal import Decimal
from pathlib import Path

import duckdb

from tests.shopify_commercial_fixtures import create_empty_commercial_tables

ROOT = Path(__file__).parents[1]
MONEY = (
    "total_price",
    "current_total_price",
    "original_total_price",
    "subtotal_price",
    "current_subtotal_price",
    "total_discounts",
    "total_tax",
    "current_total_tax",
    "total_shipping_price",
    "total_refunded",
)
ADDRESS = (
    "first_name",
    "last_name",
    "company",
    "address1",
    "address2",
    "city",
    "province",
    "province_code",
    "zip",
    "country",
    "country_code",
    "phone",
)
ORDER_COLUMNS: dict[str, str] = {
    "shop_id": "varchar",
    "order_id": "varchar",
    "name": "varchar",
    "customer_id": "varchar",
    "created_at": "timestamptz",
    "processed_at": "timestamptz",
    "updated_at": "timestamptz",
    "cancelled_at": "timestamptz",
    "closed_at": "timestamptz",
    "cancel_reason": "varchar",
    "test": "boolean",
    "source_name": "varchar",
    "display_financial_status": "varchar",
    "display_fulfillment_status": "varchar",
    "taxes_included": "boolean",
    "currency_code": "varchar",
    "presentment_currency_code": "varchar",
    **{
        f"{name}_{part}": kind
        for name in MONEY
        for part, kind in (
            ("shop_amount", "decimal(38, 9)"),
            ("shop_currency", "varchar"),
            ("presentment_amount", "decimal(38, 9)"),
            ("presentment_currency", "varchar"),
        )
    },
    "email": "varchar",
    "phone": "varchar",
    **{
        f"{kind}_address_{part}": "varchar"
        for kind in ("billing", "shipping")
        for part in ADDRESS
    },
    "_observed_at": "timestamptz",
    "_query_version": "varchar",
    "transactions_count": "decimal(19, 0)",
    "transactions_count_precision": "varchar",
    # dlt stores integer load metadata as NUMBER(19,0) on Snowflake.
    "_extraction_revision": "decimal(19, 0)",
}
TAG_COLUMNS: dict[str, str] = {
    "shop_id": "varchar",
    "order_id": "varchar",
    "tag": "varchar",
}
CODE_COLUMNS: dict[str, str] = {
    "shop_id": "varchar",
    "order_id": "varchar",
    "code": "varchar",
}
LINE_COLUMNS: dict[str, str] = {
    "shop_id": "varchar",
    "order_id": "varchar",
    "line_item_id": "varchar",
    "sku": "varchar",
    "name": "varchar",
    "title": "varchar",
    "variant_title": "varchar",
    "product_id": "varchar",
    "variant_id": "varchar",
    "is_gift_card": "boolean",
    # Match dlt Snowflake integer storage, rather than pre-normalizing the raw
    # fixture to dbt's logical BIGINT (NUMBER(38,0) on Snowflake).
    "quantity": "decimal(19, 0)",
    "current_quantity": "decimal(19, 0)",
    "original_unit_price_shop_amount": "decimal(38, 9)",
    "original_unit_price_shop_currency": "varchar",
    "original_unit_price_presentment_amount": "decimal(38, 9)",
    "original_unit_price_presentment_currency": "varchar",
    "discounted_total_shop_amount": "decimal(38, 9)",
    "discounted_total_shop_currency": "varchar",
    "discounted_total_presentment_amount": "decimal(38, 9)",
    "discounted_total_presentment_currency": "varchar",
}


def order_row(order_id: str, processed_at: str, **values: object) -> dict[str, object]:
    row: dict[str, object] = {
        "shop_id": "shop-1",
        "order_id": order_id,
        "name": f"#{order_id}",
        "created_at": processed_at,
        "processed_at": processed_at,
        "updated_at": processed_at,
        "_observed_at": processed_at,
        "_extraction_revision": 3,
        "test": False,
        "display_fulfillment_status": "UNFULFILLED",
        "currency_code": "USD",
        "presentment_currency_code": "CAD",
        "email": f"{order_id}@example.com",
        "shipping_address_country_code": "US",
        "shipping_address_province_code": "UT",
    }
    for name in MONEY:
        row |= {
            f"{name}_shop_amount": Decimal("10"),
            f"{name}_shop_currency": "USD",
            f"{name}_presentment_amount": Decimal("13.7"),
            f"{name}_presentment_currency": "CAD",
        }
    return row | values


def line_row(
    order_id: str, line_item_id: str, quantity: int, price: str
) -> dict[str, object]:
    return {
        "shop_id": "shop-1",
        "order_id": order_id,
        "line_item_id": line_item_id,
        "name": "Blanket",
        "quantity": quantity,
        "current_quantity": quantity,
        "original_unit_price_shop_amount": Decimal(price),
        "original_unit_price_shop_currency": "USD",
        "original_unit_price_presentment_amount": Decimal(price),
        "original_unit_price_presentment_currency": "CAD",
        "discounted_total_shop_amount": Decimal(price) * quantity,
        "discounted_total_shop_currency": "USD",
        "discounted_total_presentment_amount": Decimal(price) * quantity,
        "discounted_total_presentment_currency": "CAD",
    }


def tag_row(order_id: str, tag: str) -> dict[str, object]:
    return {"shop_id": "shop-1", "order_id": order_id, "tag": tag}


def code_row(order_id: str, code: str) -> dict[str, object]:
    return {"shop_id": "shop-1", "order_id": order_id, "code": code}


RAW_TABLES = {
    "orders": ORDER_COLUMNS,
    "orders__lines": LINE_COLUMNS,
    "orders__tags": TAG_COLUMNS,
    "orders__discount_codes": CODE_COLUMNS,
}


def write_raw(
    path: Path,
    orders: list[dict[str, object]],
    lines: list[dict[str, object]],
    tags: Sequence[dict[str, object]] = (),
    codes: Sequence[dict[str, object]] = (),
) -> None:
    path.unlink(missing_ok=True)
    with duckdb.connect(str(path)) as db:
        _ = db.execute("create schema raw")
        for table, columns in RAW_TABLES.items():
            _ = db.execute(
                f"create table raw.{table} ("
                + ", ".join(f'"{name}" {kind}' for name, kind in columns.items())
                + ")"
            )
        fill_raw(db, orders, lines, tags, codes)
        create_empty_commercial_tables(db)
        _ = db.execute("checkpoint")


def fill_raw(
    db: duckdb.DuckDBPyConnection,
    orders: Sequence[dict[str, object]],
    lines: Sequence[dict[str, object]],
    tags: Sequence[dict[str, object]],
    codes: Sequence[dict[str, object]],
) -> None:
    for (table, columns), rows in zip(
        RAW_TABLES.items(), (orders, lines, tags, codes), strict=True
    ):
        # Keep the commercial orders that add_financial_raw adds.
        _ = db.execute(f"delete from raw.{table} where order_id not like 'money-%'")
        for row in rows:
            _ = db.execute(
                f"insert into raw.{table} values ("
                + ", ".join("?" for _ in columns)
                + ")",
                [row.get(name) for name in columns],
            )


GOOD_ORDERS = [
    # 07:30 UTC on 1 July is 00:30 Pacific: the order date is 1 July.
    order_row(
        "1001",
        "2026-07-01 07:30:00+00",
        customer_id="customer-1",
        source_name="web",
        display_financial_status="PAID",
        shipping_address_city="Salt Lake City",
    ),
    # 06:30 UTC on 1 July is 23:30 Pacific on 30 June.
    order_row(
        "1002",
        "2026-07-01 06:30:00+00",
        cancelled_at="2026-07-02 00:00:00+00",
        test=True,
    ),
    # Extraction revision 1 never selected tags, codes or email.
    order_row("1003", "2026-07-01 12:00:00+00", _extraction_revision=None),
    # No processing time: order_date falls back to created_at (00:00 Pacific),
    # and the warning-only fallback test does not block publication.
    order_row("1004", "2026-07-02 07:00:00+00") | {"processed_at": None},
]
GOOD_TAGS = [
    # Staging trims and lowercases tags before the classification rules.
    tag_row("1001", " Broadcast "),
    tag_row("1002", "Vera-Bulk"),
]
GOOD_CODES = [code_row("1004", " Free-Replacement ")]
GOOD_LINES = [
    line_row("1001", "1", 2, "10.005")
    | {
        "sku": "BLANKET-1",
        "title": "Blanket",
        "variant_title": "Blue",
        "product_id": "product-1",
        "variant_id": "variant-1",
        "is_gift_card": False,
        "current_quantity": 1,
        "original_unit_price_presentment_amount": Decimal("13.700000001"),
        "discounted_total_shop_amount": Decimal("18.01"),
        "discounted_total_presentment_amount": Decimal("25.02"),
    },
    line_row("1001", "2", 1, "99.99")
    | {
        "sku": "BLANKET-1",
        "title": "Blanket",
        "variant_title": "Blue",
        "product_id": "product-1",
        "variant_id": "variant-1",
        "is_gift_card": False,
    },
    line_row("1002", "3", 3, "0.333333333")
    | {
        "is_gift_card": True,
        "current_quantity": 0,
    },
]


def add_financial_raw(path: Path) -> None:
    """Add two resolved USD orders with a 200/100 shipping allocation basis."""
    with duckdb.connect(str(path)) as db:

        def insert(table: str, row: dict[str, object]) -> None:
            _ = db.execute(
                f"insert into raw.{table} ({', '.join(row)}) values "
                f"({', '.join('?' for _ in row)})",
                list(row.values()),
            )

        for order_id in ("money-1", "money-2"):
            identity: dict[str, object] = {"shop_id": "shop-1", "order_id": order_id}
            order = order_row(
                order_id,
                "2026-07-01 12:00:00+00",
                presentment_currency_code="USD",
                _query_version="verified-fixture",
                transactions_count=1,
                transactions_count_precision="EXACT",
            )
            for prefix in MONEY:
                amount = Decimal("0")
                if prefix in (
                    "total_price",
                    "current_total_price",
                    "original_total_price",
                ):
                    amount = Decimal("310")
                elif prefix in ("subtotal_price", "current_subtotal_price"):
                    amount = Decimal("300")
                elif prefix == "total_shipping_price":
                    amount = Decimal("10")
                for currency in ("shop", "presentment"):
                    order[f"{prefix}_{currency}_amount"] = amount
                    order[f"{prefix}_{currency}_currency"] = "USD"
            insert("orders", order)
            insert(
                "orders__agreements",
                identity
                | {
                    "agreement_id": order_id + "-agreement",
                    "type": "OrderAgreement",
                    "reason": "ORDER",
                    "happened_at": "2026-07-01 12:00:00+00",
                },
            )
            insert(
                "orders__shipping_lines",
                identity | {"shipping_line_id": order_id + "-shipping"},
            )
            for suffix, quantity, amount in [
                ("a", 2, "200"),
                ("b", 1, "100"),
                ("shipping", 1, "10"),
            ]:
                shipping = suffix == "shipping"
                line_id = order_id + "-" + suffix
                if not shipping:
                    line = line_row(order_id, line_id, quantity, "100") | {
                        "is_gift_card": False
                    }
                    for key in list(line):
                        if key.endswith("_currency"):
                            line[key] = "USD"
                    insert("orders__lines", line)
                sale = identity | {
                    "agreement_id": order_id + "-agreement",
                    "sale_id": line_id + "-sale",
                    "type": "ShippingLineSale" if shipping else "ProductSale",
                    "action_type": "ORDER",
                    "line_type": "SHIPPING" if shipping else "PRODUCT",
                    "quantity": quantity,
                    "line_item_id": None if shipping else line_id,
                    "shipping_line_id": line_id if shipping else None,
                }
                for prefix in (
                    "total_amount",
                    "total_tax_amount",
                    "total_discount_amount_before_taxes",
                    "total_discount_amount_after_taxes",
                ):
                    for currency in ("shop", "presentment"):
                        sale[f"{prefix}_{currency}_amount"] = Decimal(
                            amount if prefix == "total_amount" else "0"
                        )
                        sale[f"{prefix}_{currency}_currency"] = "USD"
                insert("orders__agreements__sales", sale)
            insert(
                "orders__transactions",
                identity
                | {
                    "transaction_id": order_id + "-payment",
                    "created_at": "2026-07-01 12:00:00+00",
                    "kind": "SALE",
                    "status": "SUCCESS",
                    "gateway": "manual",
                    "test": False,
                    "amount_shop_amount": Decimal("310"),
                    "amount_shop_currency": "USD",
                    "amount_presentment_amount": Decimal("310"),
                    "amount_presentment_currency": "USD",
                },
            )
