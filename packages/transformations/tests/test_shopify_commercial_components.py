"""Verify commercial components on typed DuckDB fixtures.

Expected amounts follow the approved Shopify order-mart specification.
Upstream SQL runs directly; finalized fee proof has its own dbt unit tests.
"""

import re
from collections.abc import Callable
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import cast

import duckdb
import pytest
import yaml

from analytics_transformations.mart_contracts import mapping, sequence, text

type Row = dict[str, object]
type Rows = Callable[[Dataset], dict[str, Row]]

ROOT = Path(__file__).parents[1]
MODELS = ROOT / "dbt" / "models" / "intermediate" / "shopify_orders"
# Runs intermediate model SQL directly over fixture tables (ADR 0026).
pytestmark = pytest.mark.dbt("path:models/intermediate")
SCHEMA_FILE = MODELS / "_shopify_orders__commercial_components.yml"
REF = re.compile(r"\{\{\s*ref\('([^']+)'\)\s*\}\}")
MONEY = "decimal(38, 9)"
SHOP = "s1"

# Staged relations the models read. Only the columns the models use are declared.
FIXTURE_SCHEMAS: dict[str, dict[str, str]] = {
    "int_shopify_refunds__handling_fees": {
        "shop_id": "varchar",
        "order_id": "varchar",
        "refund_id": "varchar",
        "handling_fee_status": "varchar",
        "handling_fee_shop_amount": MONEY,
    },
    "stg_shopify_orders__orders": {
        "shop_id": "varchar",
        "order_id": "varchar",
        "current_total_price_shop_amount": MONEY,
        "original_total_price_shop_amount": MONEY,
        "source_observed_at": "timestamp",
        "source_query_version": "varchar",
        "extraction_revision": "bigint",
        "transactions_count": "bigint",
        "transactions_count_precision": "varchar",
        "is_test": "boolean",
    },
    "stg_shopify_orders__agreements": {
        "shop_id": "varchar",
        "order_id": "varchar",
        "agreement_id": "varchar",
        "agreement_type": "varchar",
        "happened_at": "timestamp",
        "reason": "varchar",
        "refund_id": "varchar",
        "return_id": "varchar",
    },
    "stg_shopify_orders__agreement_sales": {
        "shop_id": "varchar",
        "order_id": "varchar",
        "agreement_id": "varchar",
        "sale_id": "varchar",
        "sale_type": "varchar",
        "action_type": "varchar",
        "line_type": "varchar",
        "signed_quantity": "bigint",
        "line_item_id": "varchar",
        "shipping_line_id": "varchar",
        "duty_id": "varchar",
        "fee_id": "varchar",
        "fee_type": "varchar",
        "additional_fee_id": "varchar",
        "total_amount_shop_amount": MONEY,
        "total_amount_shop_currency_code": "varchar",
        "total_tax_amount_shop_amount": MONEY,
        "total_tax_amount_shop_currency_code": "varchar",
        "total_discount_amount_before_taxes_shop_amount": MONEY,
        "total_discount_amount_before_taxes_shop_currency_code": "varchar",
    },
    "stg_shopify_orders__order_lines": {
        "shop_id": "varchar",
        "order_id": "varchar",
        "line_item_id": "varchar",
        "is_gift_card": "boolean",
    },
    "stg_shopify_orders__refunds": {
        "created_at": "timestamp",
        "return_id": "varchar",
        "shop_id": "varchar",
        "order_id": "varchar",
        "refund_id": "varchar",
        "shop_currency_code": "varchar",
        "total_refunded_shop_amount": MONEY,
    },
    "stg_shopify_orders__refund_transactions": {
        "shop_id": "varchar",
        "order_id": "varchar",
        "refund_id": "varchar",
        "transaction_id": "varchar",
    },
    "stg_shopify_orders__refund_adjustments": {
        "shop_id": "varchar",
        "order_id": "varchar",
        "refund_id": "varchar",
        "adjustment_id": "varchar",
        "adjustment_reason": "varchar",
        "adjustment_shop_amount": MONEY,
        "adjustment_shop_currency_code": "varchar",
        "adjustment_tax_shop_amount": MONEY,
        "adjustment_tax_shop_currency_code": "varchar",
    },
    "stg_shopify_orders__transactions": {
        "shop_id": "varchar",
        "order_id": "varchar",
        "transaction_id": "varchar",
        "parent_transaction_id": "varchar",
        "created_at": "timestamp",
        "processed_at": "timestamp",
        "gateway": "varchar",
        "kind": "varchar",
        "status": "varchar",
        "shop_currency_code": "varchar",
        "transaction_shop_amount": MONEY,
    },
}


class Warehouse:
    """A DuckDB database that holds fixtures and builds models from their SQL."""

    def __init__(self, fixtures: dict[str, list[Row]]) -> None:
        self.con = duckdb.connect(":memory:")
        for name, schema in FIXTURE_SCHEMAS.items():
            columns = ", ".join(f"{column} {kind}" for column, kind in schema.items())
            _ = self.con.execute(f"create table {name} ({columns})")
            placeholders = ", ".join("?" for _ in schema)
            for row in fixtures.get(name, []):
                unknown = set(row) - set(schema)
                assert not unknown, f"{name} has undeclared columns {sorted(unknown)}"
                _ = self.con.execute(
                    f"insert into {name} values ({placeholders})",
                    [row.get(column) for column in schema],
                )

    def _exists(self, name: str) -> bool:
        found = self.con.execute(
            "select count(*) from information_schema.tables where table_name = ?",
            [name],
        ).fetchone()
        return bool(found and found[0])

    def build(self, name: str) -> None:
        """Create a model table, building its upstream intermediate models first."""
        if self._exists(name):
            return
        sql_path = MODELS / f"{name}.sql"
        assert sql_path.exists(), f"no fixture or model named {name}"
        source = sql_path.read_text()
        for match in REF.finditer(source):
            self.build(match.group(1))
        _ = self.con.execute(
            f"create table {name} as {REF.sub(lambda m: m.group(1), source)}"
        )

    def rows(self, name: str) -> list[Row]:
        self.build(name)
        cursor = self.con.execute(f"select * from {name}")
        assert cursor.description is not None
        columns = [str(column[0]) for column in cursor.description]
        values = cast(list[tuple[object, ...]], cursor.fetchall())
        return [dict(zip(columns, row, strict=True)) for row in values]


class Dataset:
    """Collects fixture rows, one list per staged relation."""

    def __init__(self) -> None:
        self.tables: dict[str, list[Row]] = {name: [] for name in FIXTURE_SCHEMAS}

    def add(self, table: str, **row: object) -> None:
        self.tables[table].append(row)


def shell(ds: Dataset, order_id: str, *, current_total: str = "0") -> None:
    """Add the order header, two sales agreements and three order lines."""
    ds.add(
        "stg_shopify_orders__orders",
        shop_id=SHOP,
        order_id=order_id,
        current_total_price_shop_amount=Decimal(current_total),
    )
    ds.add(
        "stg_shopify_orders__agreements",
        shop_id=SHOP,
        order_id=order_id,
        agreement_id=f"{order_id}-order",
        agreement_type="ORDER",
        happened_at=datetime(2026, 10, 1, 12),
        reason="ORDER",
        refund_id=None,
    )
    ds.add(
        "stg_shopify_orders__agreements",
        shop_id=SHOP,
        order_id=order_id,
        agreement_id=f"{order_id}-return",
        agreement_type="RETURN",
        happened_at=datetime(2026, 10, 5, 12),
        reason="RETURN",
        refund_id=f"{order_id}-refund",
    )
    for line_item_id, gift in (
        ("line-a", False),
        ("line-b", False),
        ("line-gift", True),
    ):
        ds.add(
            "stg_shopify_orders__order_lines",
            shop_id=SHOP,
            order_id=order_id,
            line_item_id=f"{order_id}-{line_item_id}",
            is_gift_card=gift,
        )


def sell(
    ds: Dataset,
    order_id: str,
    sale_id: str,
    *,
    sale_type: str = "ProductSale",
    action: str = "ORDER",
    line: str | None = "line-a",
    total: str | None = "0",
    tax: str | None = "0",
    discount: str | None = None,
    currency: str = "USD",
    fee_type: str | None = None,
    shipping_line_id: str | None = None,
) -> None:
    """Add one agreement sale. Return sales point at the return agreement."""
    agreement = "order" if action in ("ORDER", "UPDATE") else "return"
    ds.add(
        "stg_shopify_orders__agreement_sales",
        shop_id=SHOP,
        order_id=order_id,
        agreement_id=f"{order_id}-{agreement}",
        sale_id=sale_id,
        sale_type=sale_type,
        action_type=action,
        line_type="PRODUCT",
        signed_quantity=1 if sale_type == "ProductSale" else None,
        line_item_id=f"{order_id}-{line}" if line else None,
        shipping_line_id=shipping_line_id,
        fee_type=fee_type,
        total_amount_shop_amount=Decimal(total) if total is not None else None,
        total_amount_shop_currency_code=currency,
        total_tax_amount_shop_amount=Decimal(tax) if tax is not None else None,
        total_tax_amount_shop_currency_code=currency,
        total_discount_amount_before_taxes_shop_amount=(
            Decimal(discount) if discount is not None else None
        ),
        total_discount_amount_before_taxes_shop_currency_code=currency,
    )


def transaction(
    ds: Dataset,
    order_id: str,
    transaction_id: str,
    kind: str,
    status: str,
    amount: str,
    *,
    parent: str | None = None,
    currency: str = "USD",
    created_at: datetime = datetime(2026, 10, 1, 12),
    gateway: str = "manual",
) -> None:
    ds.add(
        "stg_shopify_orders__transactions",
        shop_id=SHOP,
        order_id=order_id,
        transaction_id=transaction_id,
        parent_transaction_id=parent,
        created_at=created_at,
        gateway=gateway,
        kind=kind,
        status=status,
        shop_currency_code=currency,
        transaction_shop_amount=Decimal(amount),
    )


def complete(ds: Dataset, order_id: str = "o1") -> None:
    """A fully reconciled order.

    Order: merchandise line a (100.00 gross, 10.00 discount, 7.20 tax), line b (50.00,
    4.00 tax), shipping (10.00, 0.80 tax). Return: line a is fully refunded (97.20
    total, 90.00 net). Net merchandise is 50.00. Net shipping is 10.00.
    """
    shell(ds, order_id, current_total="64.80")
    sell(
        ds,
        order_id,
        "a-sale",
        line="line-a",
        total="97.20",
        tax="7.20",
        discount="10.00",
    )
    sell(
        ds,
        order_id,
        "b-sale",
        line="line-b",
        total="54.00",
        tax="4.00",
        discount="0.00",
    )
    sell(
        ds,
        order_id,
        "ship-sale",
        sale_type="ShippingLineSale",
        line=None,
        total="10.80",
        tax="0.80",
        shipping_line_id=f"{order_id}-ship",
    )
    sell(
        ds,
        order_id,
        "a-return",
        action="RETURN",
        line="line-a",
        total="-97.20",
        tax="-7.20",
        discount="0.00",
    )
    ds.add(
        "stg_shopify_orders__refunds",
        shop_id=SHOP,
        order_id=order_id,
        refund_id=f"{order_id}-refund",
        shop_currency_code="USD",
        total_refunded_shop_amount=Decimal("97.20"),
    )
    ds.add(
        "stg_shopify_orders__refund_transactions",
        shop_id=SHOP,
        order_id=order_id,
        refund_id=f"{order_id}-refund",
        transaction_id=f"{order_id}-t-refund",
    )
    transaction(ds, order_id, f"{order_id}-t-sale", "SALE", "SUCCESS", "162.00")
    transaction(
        ds,
        order_id,
        f"{order_id}-t-refund",
        "REFUND",
        "SUCCESS",
        "97.20",
        parent=f"{order_id}-t-sale",
    )


def build_rows(ds: Dataset, model: str) -> list[Row]:
    return Warehouse(ds.tables).rows(model)


@pytest.fixture
def ledger_rows() -> Callable[[Dataset], dict[str, Row]]:
    def run(ds: Dataset) -> dict[str, Row]:
        return {
            text(row["sale_id"]): row
            for row in build_rows(ds, "int_shopify_sales__ledger")
        }

    return run


@pytest.fixture
def order_rows() -> Callable[[Dataset], dict[str, Row]]:
    def run(ds: Dataset) -> dict[str, Row]:
        return {
            text(row["order_id"]): row
            for row in build_rows(ds, "int_shopify_orders__commercial_components")
        }

    return run


def dec(value: str) -> Decimal:
    return Decimal(value)


# ---------------------------------------------------------------- sales ledger


def test_ledger_components_follow_sign_and_discount_rules(ledger_rows: Rows) -> None:
    ds = Dataset()
    complete(ds)
    rows = ledger_rows(ds)

    product = rows["a-sale"]
    assert product["resolution_status"] == "resolved"
    assert product["sale_net_shop_amount"] == dec("90.00")
    assert product["gross_merchandise_shop_amount"] == dec("100.00")
    assert product["merchandise_discount_shop_amount"] == dec("-10.00")
    assert product["merchandise_refund_shop_amount"] == dec("0")

    refund = rows["a-return"]
    assert refund["sale_net_shop_amount"] == dec("-90.00")
    assert refund["gross_merchandise_shop_amount"] == dec("0")
    assert refund["merchandise_refund_shop_amount"] == dec("-90.00")

    shipping = rows["ship-sale"]
    assert shipping["shipping_shop_amount"] == dec("10.00")
    assert shipping["sale_tax_shop_amount"] == dec("0.80")


@pytest.mark.parametrize(
    ("sale_type", "fee_type", "kind"),
    [
        ("FeeSale", "ReturnShippingFee", "return_shipping_fee"),
        ("FeeSale", "RESTOCKING_FEE", "unfinalized_fee"),
        ("DutySale", None, "duty"),
        ("AdditionalFeeSale", None, "additional_fee"),
    ],
)
def test_fee_and_duty_sales_land_in_their_own_component(
    ledger_rows: Rows, sale_type: str, fee_type: str | None, kind: str
) -> None:
    ds = Dataset()
    shell(ds, "o1")
    sell(
        ds,
        "o1",
        "x",
        sale_type=sale_type,
        action="RETURN",
        line=None,
        total="-15.00" if sale_type == "DutySale" else "15.00",
        tax="0.00",
        fee_type=fee_type,
    )
    row = ledger_rows(ds)["x"]
    assert row["sale_kind"] == kind
    assert row["resolution_status"] == "resolved"
    component = {
        "return_shipping_fee": "return_shipping_fee_shop_amount",
        "unfinalized_fee": "unfinalized_fee_shop_amount",
        "duty": "duty_shop_amount",
        "additional_fee": "additional_fee_shop_amount",
    }[kind]
    assert row[component] == dec("-15.00" if sale_type == "DutySale" else "15.00")


SALE_FAILURE_CASES: list[tuple[Callable[[Dataset], None], str]] = [
    (
        lambda ds: sell(ds, "o1", "x", sale_type="MysteryKind", total="1", tax="0"),
        "unresolved_sale_type",
    ),
    (
        lambda ds: sell(
            ds, "o1", "x", total="10", tax="0", discount="0", currency="CAD"
        ),
        "unresolved_currency",
    ),
    (
        lambda ds: sell(ds, "o1", "x", total="10", tax="0", discount=None),
        "unresolved_missing_money",
    ),
    (
        lambda ds: sell(ds, "o1", "x", total="-10", tax="2", discount="0"),
        "unresolved_sign_conflict",
    ),
    (
        lambda ds: sell(
            ds, "o1", "x", line="line-missing", total="10", tax="0", discount="0"
        ),
        "unresolved_missing_order_line",
    ),
]


@pytest.mark.parametrize(
    ("setup", "status"),
    SALE_FAILURE_CASES,
)
def test_unresolved_sale_keeps_source_fields_and_null_components(
    ledger_rows: Rows, setup: Callable[[Dataset], None], status: str
) -> None:
    ds = Dataset()
    shell(ds, "o1")
    setup(ds)
    row = ledger_rows(ds)["x"]
    assert row["resolution_status"] == status
    assert row["is_resolved"] is False
    assert row["sale_net_shop_amount"] is None
    assert row["gross_merchandise_shop_amount"] is None
    assert row["merchandise_discount_shop_amount"] is None
    assert row["shipping_shop_amount"] is None


def test_resolved_components_sum_to_sale_net(ledger_rows: Rows) -> None:
    ds = Dataset()
    complete(ds)
    sell(
        ds,
        "o1",
        "gift-sale",
        line="line-gift",
        total="50.00",
        tax="0.00",
        discount="0.00",
    )
    components = (
        "gross_merchandise_shop_amount",
        "merchandise_discount_shop_amount",
        "merchandise_refund_shop_amount",
        "gift_card_liability_shop_amount",
        "shipping_shop_amount",
        "shipping_refund_shop_amount",
        "duty_shop_amount",
        "return_shipping_fee_shop_amount",
        "unfinalized_fee_shop_amount",
        "additional_fee_shop_amount",
    )
    for row in ledger_rows(ds).values():
        assert row["resolution_status"] == "resolved", row["sale_id"]
        total = sum((cast(Decimal, row[name]) for name in components), Decimal(0))
        assert total == row["sale_net_shop_amount"], row["sale_id"]


def test_gift_card_sale_is_liability_not_merchandise(ledger_rows: Rows) -> None:
    ds = Dataset()
    shell(ds, "o1")
    sell(ds, "o1", "gift", line="line-gift", total="50.00", tax="0.00", discount="0.00")
    row = ledger_rows(ds)["gift"]
    assert row["sale_kind"] == "gift_card"
    assert row["gift_card_liability_shop_amount"] == dec("50.00")
    assert row["gross_merchandise_shop_amount"] == dec("0")


# ----------------------------------------------------------- order components


def test_complete_order_reconciles(order_rows: Rows) -> None:
    ds = Dataset()
    complete(ds)
    row = order_rows(ds)["o1"]
    assert row["commercial_resolution_status"] == "complete"
    assert row["is_commercial_resolved"] is True
    assert row["gross_merchandise_shop_amount"] == dec("150.00")
    assert row["merchandise_discount_shop_amount"] == dec("-10.00")
    assert row["merchandise_refund_shop_amount"] == dec("-90.00")
    assert row["net_merchandise_shop_amount"] == dec("50.00")
    assert row["net_shipping_shop_amount"] == dec("10.00")
    assert row["net_revenue_shop_amount"] == dec("60.00")
    assert row["tax_shop_amount"] == dec("4.80")
    assert row["ledger_header_difference_shop_amount"] == dec("0.00")
    assert row["refund_agreement_difference_shop_amount"] == dec("0.00")
    assert row["successful_collected_shop_amount"] == dec("162.00")
    assert row["successful_refund_object_payment_shop_amount"] == dec("97.20")


def test_return_shipping_fee_is_reported_outside_net_revenue(order_rows: Rows) -> None:
    ds = Dataset()
    complete(ds)
    sell(
        ds,
        "o1",
        "rfee",
        sale_type="FeeSale",
        action="RETURN",
        line=None,
        total="8.00",
        tax="0.00",
        fee_type="ReturnShippingFee",
    )
    ds.tables["stg_shopify_orders__orders"][0]["current_total_price_shop_amount"] = dec(
        "72.80"
    )
    ds.tables["stg_shopify_orders__refunds"][0]["total_refunded_shop_amount"] = dec(
        "89.20"
    )
    for tx in ds.tables["stg_shopify_orders__transactions"]:
        if tx["kind"] == "REFUND":
            tx["transaction_shop_amount"] = dec("89.20")
    row = order_rows(ds)["o1"]
    assert row["commercial_resolution_status"] == "complete"
    assert row["return_shipping_fee_shop_amount"] == dec("8.00")
    assert row["net_revenue_shop_amount"] == dec("60.00")


def test_known_zero_shipping_is_zero_not_null(order_rows: Rows) -> None:
    ds = Dataset()
    shell(ds, "o1", current_total="54.00")
    sell(ds, "o1", "b-sale", line="line-b", total="54.00", tax="4.00", discount="0.00")
    transaction(ds, "o1", "t1", "SALE", "SUCCESS", "54.00")
    row = order_rows(ds)["o1"]
    assert row["commercial_resolution_status"] == "complete"
    assert row["shipping_shop_amount"] == dec("0")
    assert row["net_shipping_shop_amount"] == dec("0")
    assert row["net_revenue_shop_amount"] == dec("50.00")


def test_gift_card_order_excludes_liability_from_merchandise(order_rows: Rows) -> None:
    ds = Dataset()
    shell(ds, "o1", current_total="50.00")
    sell(ds, "o1", "gift", line="line-gift", total="50.00", tax="0.00", discount="0.00")
    transaction(ds, "o1", "t1", "SALE", "SUCCESS", "50.00")
    row = order_rows(ds)["o1"]
    assert row["commercial_resolution_status"] == "complete"
    assert row["gift_card_liability_shop_amount"] == dec("50.00")
    assert row["gross_merchandise_shop_amount"] == dec("0")
    assert row["net_merchandise_shop_amount"] == dec("0")


ORDER_FAILURE_CASES: list[tuple[Callable[[Dataset], None], str]] = [
    (
        lambda ds: sell(
            ds,
            "o1",
            "fee",
            sale_type="FeeSale",
            action="RETURN",
            line=None,
            total="15.00",
            tax="0.00",
            fee_type="RESTOCKING_FEE",
        ),
        "unfinalized_fee",
    ),
    (
        lambda ds: ds.add(
            "stg_shopify_orders__refunds",
            shop_id=SHOP,
            order_id="o1",
            refund_id="extra",
            shop_currency_code="USD",
            total_refunded_shop_amount=Decimal("5"),
        ),
        "unresolved_refund",
    ),
    (
        lambda ds: ds.add(
            "stg_shopify_orders__refund_adjustments",
            shop_id=SHOP,
            order_id="o1",
            refund_id="o1-refund",
            adjustment_id="adj-1",
            adjustment_reason="GOODWILL",
            adjustment_shop_amount=Decimal("5.00"),
            adjustment_shop_currency_code="USD",
            adjustment_tax_shop_amount=Decimal("0.00"),
            adjustment_tax_shop_currency_code="USD",
        ),
        "unresolved_adjustment",
    ),
    (
        lambda ds: transaction(
            ds, "o1", "t-bad", "REFUND", "SUCCESS", "3.00", parent="o1-t-sale"
        ),
        "unresolved_payment_refund",
    ),
    (
        lambda ds: transaction(
            ds, "o1", "t-cad", "SALE", "SUCCESS", "3.00", currency="CAD"
        ),
        "unresolved_payment_currency",
    ),
    (
        lambda ds: ds.tables["stg_shopify_orders__agreements"].clear(),
        "no_agreement_evidence",
    ),
    (
        lambda ds: sell(
            ds, "o1", "mystery", sale_type="MysteryKind", total="1", tax="0"
        ),
        "unresolved_sales",
    ),
]


@pytest.mark.parametrize(
    ("mutate", "status"),
    ORDER_FAILURE_CASES,
)
def test_blocking_evidence_sets_status_and_nulls_net(
    order_rows: Rows, mutate: Callable[[Dataset], None], status: str
) -> None:
    ds = Dataset()
    complete(ds)
    mutate(ds)
    row = order_rows(ds)["o1"]
    assert row["commercial_resolution_status"] == status
    assert row["is_commercial_resolved"] is False
    assert row["net_merchandise_shop_amount"] is None
    assert row["net_shipping_shop_amount"] is None
    assert row["net_revenue_shop_amount"] is None


def test_refund_without_agreement_blocks_completion(order_rows: Rows) -> None:
    ds = Dataset()
    complete(ds)
    ds.add(
        "stg_shopify_orders__refunds",
        shop_id=SHOP,
        order_id="o1",
        refund_id="extra",
        shop_currency_code="USD",
        total_refunded_shop_amount=Decimal("5.00"),
    )
    row = order_rows(ds)["o1"]
    assert row["commercial_resolution_status"] == "unresolved_refund"
    assert row["refund_without_agreement_count"] == 1


def test_adjustment_is_visible_and_not_a_merchandise_refund(order_rows: Rows) -> None:
    ds = Dataset()
    complete(ds)
    ds.add(
        "stg_shopify_orders__refund_adjustments",
        shop_id=SHOP,
        order_id="o1",
        refund_id="o1-refund",
        adjustment_id="adj-1",
        adjustment_reason="GOODWILL",
        adjustment_shop_amount=Decimal("5.00"),
        adjustment_shop_currency_code="USD",
        adjustment_tax_shop_amount=Decimal("0.00"),
        adjustment_tax_shop_currency_code="USD",
    )
    row = order_rows(ds)["o1"]
    assert row["commercial_resolution_status"] == "unresolved_adjustment"
    assert row["unresolved_adjustment_shop_amount"] == dec("5.00")
    assert row["merchandise_refund_shop_amount"] == dec("-90.00")


def test_failed_attempt_reversal_is_excluded_from_refunds(order_rows: Rows) -> None:
    ds = Dataset()
    complete(ds)
    failed_attempt_reversal(ds)
    row = order_rows(ds)["o1"]
    assert row["commercial_resolution_status"] == "complete"
    assert row["failed_attempt_reversal_shop_amount"] == dec("20.00")
    assert row["unresolved_refund_payment_shop_amount"] == dec("0")
    assert row["refund_object_shop_amount"] == dec("97.20")
    assert row["net_merchandise_shop_amount"] == dec("50.00")


def test_order_without_sales_keeps_components_null(order_rows: Rows) -> None:
    ds = Dataset()
    shell(ds, "o1")
    row = order_rows(ds)["o1"]
    assert row["sale_count"] == 0
    assert row["commercial_resolution_status"] == "no_sale_evidence"
    assert row["gross_merchandise_shop_amount"] is None
    assert row["tax_shop_amount"] is None


def test_orders_are_isolated(order_rows: Rows) -> None:
    ds = Dataset()
    complete(ds, "o1")
    complete(ds, "o2")
    ds.add(
        "stg_shopify_orders__refund_adjustments",
        shop_id=SHOP,
        order_id="o2",
        refund_id="o2-refund",
        adjustment_id="adj-1",
        adjustment_reason="GOODWILL",
        adjustment_shop_amount=Decimal("5.00"),
        adjustment_shop_currency_code="USD",
        adjustment_tax_shop_amount=Decimal("0.00"),
        adjustment_tax_shop_currency_code="USD",
    )
    rows = order_rows(ds)
    assert rows["o1"]["commercial_resolution_status"] == "complete"
    assert rows["o2"]["commercial_resolution_status"] == "unresolved_adjustment"
    assert rows["o1"]["net_revenue_shop_amount"] == dec("60.00")


# ------------------------------------------------------------ line rollup


def test_line_rollup_assigns_direct_line_amounts() -> None:
    ds = Dataset()
    complete(ds)
    rows = {
        text(row["line_item_id"]): row
        for row in build_rows(ds, "int_shopify_order_lines__commercial_components")
    }
    line_a = rows["o1-line-a"]
    assert line_a["line_commercial_status"] == "resolved"
    assert line_a["gross_merchandise_shop_amount"] == dec("100.00")
    assert line_a["merchandise_refund_shop_amount"] == dec("-90.00")
    assert line_a["net_merchandise_shop_amount"] == dec("0.00")
    assert rows["o1-line-b"]["net_merchandise_shop_amount"] == dec("50.00")


def test_line_without_sales_has_no_sale_evidence() -> None:
    ds = Dataset()
    shell(ds, "o1")
    rows = {
        text(row["line_item_id"]): row
        for row in build_rows(ds, "int_shopify_order_lines__commercial_components")
    }
    assert rows["o1-line-a"]["line_commercial_status"] == "no_sale_evidence"
    assert rows["o1-line-a"]["net_merchandise_shop_amount"] is None


# ------------------------------------------------------- payment reconciliation


def test_payment_reconciliation_separates_refund_classes() -> None:
    ds = Dataset()
    complete(ds)
    failed_attempt_reversal(ds)
    rows = build_rows(ds, "int_shopify_orders__payment_reconciliation")
    assert len(rows) == 1
    row = rows[0]
    assert row["failed_payment_attempt_count"] == 1
    assert row["collected_shop_amount"] == dec("182.00")
    assert row["refund_object_payment_shop_amount"] == dec("97.20")
    assert row["failed_attempt_reversal_shop_amount"] == dec("20.00")
    assert row["unresolved_refund_payment_count"] == 0


def test_non_usd_payment_blocks_amounts() -> None:
    ds = Dataset()
    shell(ds, "o1")
    transaction(ds, "o1", "t-cad", "SALE", "SUCCESS", "10.00", currency="CAD")
    row = build_rows(ds, "int_shopify_orders__payment_reconciliation")[0]
    assert row["payment_currency_unresolved_count"] == 1
    assert row["collected_shop_amount"] is None


# ---------------------------------------------------------------- YAML contract


def _sql_columns(model: str) -> dict[str, str]:
    ds = Dataset()
    complete(ds)
    warehouse = Warehouse(ds.tables)
    warehouse.build(model)
    described = cast(
        list[tuple[object, ...]], warehouse.con.execute(f"describe {model}").fetchall()
    )
    return {text(row[0]): text(row[1]) for row in described}


def _normal(kind: str) -> str:
    return kind.lower().replace(" ", "")


@pytest.mark.parametrize(
    "model",
    [
        "int_shopify_sales__ledger",
        "int_shopify_order_lines__commercial_components",
        "int_shopify_orders__payment_reconciliation",
        "int_shopify_transactions__payment_flows",
        "int_shopify_orders__payment_history",
        "int_shopify_orders__commercial_components",
        "int_shopify_refunds__commercial_reconciliation",
    ],
)
def test_declared_columns_and_types_match_sql(model: str) -> None:
    payload = mapping(cast(object, yaml.safe_load(SCHEMA_FILE.read_text())))
    models = [mapping(entry) for entry in sequence(payload["models"])]
    definition = next(entry for entry in models if entry["name"] == model)
    declared = [mapping(column) for column in sequence(definition["columns"])]
    actual = _sql_columns(model)
    names = [text(column["name"]) for column in declared]
    assert len(names) == len(set(names)), model
    assert sorted(names) == sorted(actual), model
    for column in declared:
        name = text(column["name"])
        assert _normal(actual[name]) == _normal(text(column["data_type"])), name


def test_actual_gift_card_sale_type_is_not_merchandise(ledger_rows: Rows) -> None:
    ds = Dataset()
    shell(ds, "o1")
    sell(
        ds,
        "o1",
        "gift",
        sale_type="GiftCardSale",
        line="line-gift",
        total="50",
        tax="0",
    )
    row = ledger_rows(ds)["gift"]
    assert row["resolution_status"] == "resolved"
    assert row["gift_card_liability_shop_amount"] == dec("50")
    assert row["gross_merchandise_shop_amount"] == dec("0")


def test_missing_gift_card_flag_is_not_regular_merchandise(ledger_rows: Rows) -> None:
    ds = Dataset()
    shell(ds, "o1")
    ds.tables["stg_shopify_orders__order_lines"][0]["is_gift_card"] = None
    sell(ds, "o1", "sale", total="50", tax="0", discount="0")
    row = ledger_rows(ds)["sale"]
    assert row["resolution_status"] == "unresolved_gift_card_evidence"
    assert row["gross_merchandise_shop_amount"] is None


def test_fee_type_must_match_the_selected_shopify_object(ledger_rows: Rows) -> None:
    ds = Dataset()
    shell(ds, "o1")
    sell(
        ds,
        "o1",
        "fee",
        sale_type="FeeSale",
        line=None,
        total="15",
        tax="0",
        fee_type="ReturnShippingFee",
    )
    sell(
        ds,
        "o1",
        "unknown-fee",
        sale_type="FeeSale",
        line=None,
        total="15",
        tax="0",
        fee_type="UnverifiedReturnShippingFee",
    )
    rows = ledger_rows(ds)
    assert rows["fee"]["sale_kind"] == "return_shipping_fee"
    assert rows["unknown-fee"]["sale_kind"] == "unfinalized_fee"


def test_finalized_handling_fee_explains_refund_gap_without_becoming_revenue(
    order_rows: Rows,
) -> None:
    ds = Dataset()
    complete(ds)
    ds.tables["stg_shopify_orders__refunds"][0]["total_refunded_shop_amount"] = dec(
        "82.20"
    )
    for tx in ds.tables["stg_shopify_orders__transactions"]:
        if tx["kind"] == "REFUND":
            tx["transaction_shop_amount"] = dec("82.20")
    ds.add(
        "stg_shopify_orders__refund_adjustments",
        shop_id=SHOP,
        order_id="o1",
        refund_id="o1-refund",
        adjustment_id="fee",
        adjustment_reason="REFUND_DISCREPANCY",
        adjustment_shop_amount=dec("15"),
        adjustment_shop_currency_code="USD",
        adjustment_tax_shop_amount=dec("0"),
        adjustment_tax_shop_currency_code="USD",
    )
    ds.add(
        "int_shopify_refunds__handling_fees",
        shop_id=SHOP,
        order_id="o1",
        refund_id="o1-refund",
        handling_fee_status="matched_loop_handling_fee",
        handling_fee_shop_amount=dec("15"),
    )
    ds.tables["stg_shopify_orders__agreements"][1]["reason"] = "REFUND"
    sell(
        ds,
        "o1",
        "fee-adjustment-sale",
        sale_type="AdjustmentSale",
        action="RETURN",
        line=None,
        total="15",
        tax="0",
    )
    row = order_rows(ds)["o1"]
    assert row["commercial_resolution_status"] == "complete"
    assert row["handling_fee_shop_amount"] == dec("15")
    assert row["unresolved_adjustment_shop_amount"] == dec("0")
    assert row["refund_agreement_difference_shop_amount"] == dec("0")
    assert row["net_merchandise_shop_amount"] == dec("50")
    assert row["net_revenue_shop_amount"] == dec("60")


def failed_attempt_reversal(ds: Dataset) -> None:
    start = datetime(2026, 10, 1, 11, 50)
    transaction(
        ds,
        "o1",
        "attempt-parent",
        "SALE",
        "SUCCESS",
        "20",
        created_at=start,
        gateway="gift_card",
    )
    transaction(
        ds,
        "o1",
        "attempt-failure",
        "SALE",
        "FAILURE",
        "142",
        created_at=start,
        gateway="shopify_payments",
    )
    transaction(
        ds,
        "o1",
        "attempt-reversal",
        "REFUND",
        "SUCCESS",
        "20",
        parent="attempt-parent",
        gateway="gift_card",
        created_at=datetime(2026, 10, 1, 11, 50, 2),
    )


@pytest.mark.parametrize(
    "defect",
    ["partial", "historical_failure", "duplicate", "after_purchase", "missing_time"],
)
def test_unproven_reversal_remains_unresolved(order_rows: Rows, defect: str) -> None:
    ds = Dataset()
    complete(ds)
    failed_attempt_reversal(ds)
    rows = ds.tables["stg_shopify_orders__transactions"]
    reversal = next(row for row in rows if row["transaction_id"] == "attempt-reversal")
    failed = next(row for row in rows if row["transaction_id"] == "attempt-failure")
    if defect == "partial":
        reversal["transaction_shop_amount"] = dec("10")
    elif defect == "historical_failure":
        failed["created_at"] = datetime(2026, 9, 1)
    elif defect == "duplicate":
        ds.add(
            "stg_shopify_orders__transactions",
            **(reversal | {"transaction_id": "duplicate"}),
        )
    elif defect == "after_purchase":
        reversal["created_at"] = datetime(2026, 10, 2)
    else:
        reversal["created_at"] = None
    row = order_rows(ds)["o1"]
    assert row["commercial_resolution_status"] == "unresolved_payment_refund"
    assert row["failed_attempt_reversal_shop_amount"] == dec("0")
    assert row["net_revenue_shop_amount"] is None


@pytest.mark.parametrize(
    "defect",
    [
        "header",
        "refund",
        "missing_header",
        "no_payments",
        "unknown_kind",
        "unknown_status",
        "missing_payment_amount",
    ],
)
def test_incomplete_evidence_cannot_report_complete(
    order_rows: Rows, defect: str
) -> None:
    ds = Dataset()
    complete(ds)
    if defect == "header":
        ds.tables["stg_shopify_orders__orders"][0][
            "current_total_price_shop_amount"
        ] = dec("70")
    elif defect == "missing_header":
        ds.tables["stg_shopify_orders__orders"][0][
            "current_total_price_shop_amount"
        ] = None
    elif defect == "refund":
        ds.tables["stg_shopify_orders__refunds"][0]["total_refunded_shop_amount"] = dec(
            "107.20"
        )
    elif defect == "no_payments":
        ds.tables["stg_shopify_orders__transactions"].clear()
    elif defect == "missing_payment_amount":
        ds.tables["stg_shopify_orders__transactions"][0]["transaction_shop_amount"] = (
            None
        )
    else:
        transaction(
            ds,
            "o1",
            "unexplained",
            "CHANGE" if defect == "unknown_kind" else "SALE",
            "SUCCESS" if defect == "unknown_kind" else "UNKNOWN",
            "10",
        )
    row = order_rows(ds)["o1"]
    assert not row["is_commercial_resolved"]
    assert row["net_revenue_shop_amount"] is None


@pytest.mark.parametrize(
    "action,total,discount",
    [("RETURN", "10", "0"), ("ORDER", "-10", "0"), ("ORDER", "100", "-10")],
)
def test_invalid_commercial_signs_remain_unknown(
    ledger_rows: Rows, action: str, total: str, discount: str
) -> None:
    ds = Dataset()
    shell(ds, "o1")
    sell(ds, "o1", "bad-sign", action=action, total=total, tax="0", discount=discount)
    row = ledger_rows(ds)["bad-sign"]
    assert row["resolution_status"] == "unresolved_sign_conflict"
    assert row["gross_merchandise_shop_amount"] is None


def test_order_edit_refund_links_uniquely_without_double_subtraction(
    order_rows: Rows,
) -> None:
    ds = Dataset()
    shell(ds, "o1", current_total="146.23")
    sell(ds, "o1", "original", total="292.46", tax="17.46", discount="0")
    ds.add(
        "stg_shopify_orders__agreements",
        shop_id=SHOP,
        order_id="o1",
        agreement_id="price-edit",
        agreement_type="OrderEditAgreement",
        reason="ORDER_EDIT",
        happened_at=datetime(2026, 10, 5, 16, 58, 19),
    )
    sell(
        ds,
        "o1",
        "remove-old-price",
        action="UPDATE",
        total="-292.46",
        tax="-17.46",
        discount="0",
    )
    sell(
        ds,
        "o1",
        "new-price",
        action="ORDER",
        total="146.23",
        tax="8.73",
        discount="137.5",
    )
    for sale in ds.tables["stg_shopify_orders__agreement_sales"][1:]:
        sale["agreement_id"] = "price-edit"
    ds.add(
        "stg_shopify_orders__refunds",
        shop_id=SHOP,
        order_id="o1",
        refund_id="edit-refund",
        shop_currency_code="USD",
        total_refunded_shop_amount=dec("146.23"),
        created_at=datetime(2026, 10, 5, 16, 58, 33),
    )
    ds.add(
        "stg_shopify_orders__refund_transactions",
        shop_id=SHOP,
        order_id="o1",
        refund_id="edit-refund",
        transaction_id="edit-payment-refund",
    )
    transaction(ds, "o1", "initial-charge", "SALE", "SUCCESS", "292.46")
    transaction(
        ds,
        "o1",
        "edit-payment-refund",
        "REFUND",
        "SUCCESS",
        "146.23",
        parent="initial-charge",
    )
    row = order_rows(ds)["o1"]
    assert row["commercial_resolution_status"] == "complete"
    assert row["merchandise_refund_shop_amount"] == dec("0")
    assert row["net_merchandise_shop_amount"] == dec("137.5")
    assert row["refund_agreement_difference_shop_amount"] == dec("0")
    assert row["failed_attempt_reversal_shop_amount"] == dec("0")


def test_refund_payment_difference_is_not_complete(order_rows: Rows) -> None:
    ds = Dataset()
    complete(ds)
    for tx in ds.tables["stg_shopify_orders__transactions"]:
        if tx["kind"] == "REFUND":
            tx["transaction_shop_amount"] = dec("96.20")
    row = order_rows(ds)["o1"]
    assert row["commercial_resolution_status"] == "unresolved_refund_payment_difference"
    assert row["net_revenue_shop_amount"] is None


def test_direct_return_identity_resolves_refund_without_refund_agreement(
    order_rows: Rows,
) -> None:
    ds = Dataset()
    complete(ds)
    agreement = ds.tables["stg_shopify_orders__agreements"][1]
    agreement["refund_id"] = None
    agreement["return_id"] = "return-1"
    ds.tables["stg_shopify_orders__refunds"][0]["return_id"] = "return-1"
    row = order_rows(ds)["o1"]
    assert row["commercial_resolution_status"] == "complete"
    assert row["refund_agreement_difference_shop_amount"] == dec("0")


@pytest.mark.parametrize("mismatch", ["components", "payments"])
def test_refund_differences_cannot_cancel_between_refunds(
    order_rows: Rows, mismatch: str
) -> None:
    ds = Dataset()
    complete(ds)
    ds.tables["stg_shopify_orders__orders"][0]["current_total_price_shop_amount"] = dec(
        "44.80"
    )
    ds.tables["stg_shopify_orders__refunds"][0]["total_refunded_shop_amount"] = dec(
        "107.20"
    )
    for tx in ds.tables["stg_shopify_orders__transactions"]:
        if tx["kind"] == "REFUND":
            tx["transaction_shop_amount"] = dec("107.20")
    ds.add(
        "stg_shopify_orders__agreements",
        shop_id=SHOP,
        order_id="o1",
        agreement_id="second-return",
        reason="REFUND",
        refund_id="second-refund",
    )
    sell(ds, "o1", "second-return-sale", action="RETURN", total="-20", tax="0")
    ds.tables["stg_shopify_orders__agreement_sales"][-1]["agreement_id"] = (
        "second-return"
    )
    ds.add(
        "stg_shopify_orders__refunds",
        shop_id=SHOP,
        order_id="o1",
        refund_id="second-refund",
        shop_currency_code="USD",
        total_refunded_shop_amount=dec("10"),
    )
    ds.add(
        "stg_shopify_orders__refund_transactions",
        shop_id=SHOP,
        order_id="o1",
        refund_id="second-refund",
        transaction_id="second-refund-payment",
    )
    transaction(ds, "o1", "second-refund-payment", "REFUND", "SUCCESS", "10")
    if mismatch == "payments":
        ds.tables["stg_shopify_orders__refunds"][0]["total_refunded_shop_amount"] = dec(
            "97.20"
        )
        ds.tables["stg_shopify_orders__refunds"][1]["total_refunded_shop_amount"] = dec(
            "20"
        )
    row = order_rows(ds)["o1"]
    assert row["refund_agreement_difference_shop_amount"] == dec("0")
    assert row["commercial_resolution_status"] == (
        "unresolved_refund_difference"
        if mismatch == "components"
        else "unresolved_refund_payment_difference"
    )
    assert row["net_revenue_shop_amount"] is None


def test_payment_attempt_count_retains_declared_integer_type_and_values() -> None:
    ds = Dataset()
    transaction(ds, "o1", "failed", "SALE", "FAILURE", "10")
    transaction(ds, "o1", "errored", "AUTHORIZATION", "ERROR", "10")
    transaction(ds, "o1", "success", "CAPTURE", "SUCCESS", "10")
    transaction(ds, "o1", "pending", "SALE", "PENDING", "10")
    warehouse = Warehouse(ds.tables)
    try:
        warehouse.build("int_shopify_transactions__payment_flows")
        actual = warehouse.con.execute(
            "select transaction_id, failed_payment_attempt_count, "
            "typeof(failed_payment_attempt_count) "
            "from int_shopify_transactions__payment_flows order by transaction_id"
        ).fetchall()
        assert actual == [
            ("errored", 1, "INTEGER"),
            ("failed", 1, "INTEGER"),
            ("pending", 0, "INTEGER"),
            ("success", 0, "INTEGER"),
        ]
    finally:
        warehouse.con.close()


def test_payment_flows_preserve_identity_and_both_reversal_movements() -> None:
    ds = Dataset()
    complete(ds)
    failed_attempt_reversal(ds)
    warehouse = Warehouse(ds.tables)
    try:
        rows = {
            str(row["transaction_id"]): row
            for row in warehouse.rows("int_shopify_transactions__payment_flows")
        }
        reversal = rows["attempt-reversal"]
        assert reversal["payment_flow_class"] == "failed_attempt_reversal"
        parent = rows[str(reversal["parent_transaction_id"])]
        assert parent["payment_flow_class"] == "collection"
        assert parent["is_failed_attempt_collection"] is True
        assert reversal["is_failed_attempt_collection"] is False
        assert parent["occurred_at"] is not None
        assert all(row["shop_id"] == SHOP for row in rows.values())
    finally:
        warehouse.con.close()


@pytest.mark.parametrize("linked_refund", [False, True])
def test_payment_flow_pair_exclusion_respects_refund_objects(
    linked_refund: bool,
) -> None:
    ds = Dataset()
    complete(ds)
    failed_attempt_reversal(ds)
    if linked_refund:
        ds.add(
            "stg_shopify_orders__refund_transactions",
            shop_id=SHOP,
            order_id="o1",
            refund_id="customer-refund",
            transaction_id="attempt-reversal",
        )
    warehouse = Warehouse(ds.tables)
    try:
        rows = {
            str(row["transaction_id"]): row
            for row in warehouse.rows("int_shopify_transactions__payment_flows")
        }
        reversal = rows["attempt-reversal"]
        parent = rows[str(reversal["parent_transaction_id"])]
        assert reversal["payment_flow_class"] == (
            "refund_object" if linked_refund else "failed_attempt_reversal"
        )
        assert parent["is_failed_attempt_collection"] is not linked_refund
    finally:
        warehouse.con.close()


def test_payment_flow_reversal_does_not_exclude_another_orders_collection() -> None:
    ds = Dataset()
    complete(ds)
    failed_attempt_reversal(ds)
    reversal = next(
        row
        for row in ds.tables["stg_shopify_orders__transactions"]
        if row["transaction_id"] == "attempt-reversal"
    )
    parent_id = str(reversal["parent_transaction_id"])
    transaction(ds, "other-order", parent_id, "SALE", "SUCCESS", "20")
    warehouse = Warehouse(ds.tables)
    try:
        rows = warehouse.rows("int_shopify_transactions__payment_flows")
        other = next(row for row in rows if row["order_id"] == "other-order")
        assert other["payment_flow_class"] == "collection"
        assert other["is_failed_attempt_collection"] is False
        assert len(rows) == len(ds.tables["stg_shopify_orders__transactions"])
    finally:
        warehouse.con.close()
