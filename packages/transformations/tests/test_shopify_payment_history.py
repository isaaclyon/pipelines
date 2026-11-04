"""Payment chronology uses reconciled obligations, never financial status."""

import os
from datetime import datetime
from decimal import Decimal

import pytest
import snowflake.connector

from tests.test_shopify_commercial_components import (
    FIXTURE_SCHEMAS,
    MODELS,
    REF,
    Dataset,
    Row,
    Warehouse,
    complete,
    sell,
)


def history(
    payments: list[tuple[str, str, str, int]],
    changes: list[tuple[str, str, int]],
    *,
    original: str = "100",
    current: str = "100",
    complete: bool = True,
    live: bool = False,
) -> Row:
    ds = Dataset()
    ds.add(
        "stg_shopify_orders__orders",
        shop_id="s1",
        order_id="o1",
        original_total_price_shop_amount=Decimal(original),
        current_total_price_shop_amount=Decimal(current),
        source_observed_at=datetime(2026, 10, 10),
        source_query_version="verified-fixture",
        extraction_revision=3,
        transactions_count=len(payments),
        transactions_count_precision="EXACT" if complete else None,
    )
    for index, (reason, amount, hour) in enumerate(changes):
        agreement = str(index)
        ds.add(
            "stg_shopify_orders__agreements",
            shop_id="s1",
            order_id="o1",
            agreement_id=agreement,
            reason=reason,
            happened_at=datetime(2026, 10, 1, hour),
            refund_id=agreement if reason == "REFUND" else None,
        )
        ds.add(
            "stg_shopify_orders__agreement_sales",
            shop_id="s1",
            order_id="o1",
            agreement_id=agreement,
            sale_id=agreement,
            total_amount_shop_amount=Decimal(amount),
            total_amount_shop_currency_code="USD",
        )
    for index, (kind, status, amount, hour) in enumerate(payments):
        tx = str(index)
        ds.add(
            "stg_shopify_orders__transactions",
            shop_id="s1",
            order_id="o1",
            transaction_id=tx,
            kind=kind,
            status=status,
            transaction_shop_amount=Decimal(amount),
            shop_currency_code="USD",
            created_at=datetime(2026, 10, 1, hour),
        )
        if kind == "REFUND":
            refund = next(
                str(i) for i, change in enumerate(changes) if change[0] == "REFUND"
            )
            ds.add(
                "stg_shopify_orders__refund_transactions",
                shop_id="s1",
                order_id="o1",
                refund_id=refund,
                transaction_id=tx,
            )
    if live:
        return snowflake_history(ds)
    warehouse = Warehouse(ds.tables)
    try:
        return warehouse.rows("int_shopify_orders__payment_history")[0]
    finally:
        warehouse.con.close()


@pytest.mark.parametrize(
    ("payments", "expected"),
    [
        ([], False),
        ([("AUTHORIZATION", "SUCCESS", "100", 1)], False),
        ([("SALE", "PENDING", "100", 1)], False),
        ([("SALE", "FAILURE", "100", 1)], False),
        ([("CAPTURE", "SUCCESS", "50", 1)], False),
        ([("SALE", "SUCCESS", "100", 1)], True),
        ([("CAPTURE", "SUCCESS", "50", 1), ("CAPTURE", "SUCCESS", "50", 2)], True),
    ],
)
def test_full_payment_requires_collection(
    payments: list[tuple[str, str, str, int]], expected: bool
) -> None:
    row = history(payments, [("ORDER", "100", 1)])
    assert row["has_fully_paid_current_obligation"] is expected


def test_unknown_observation_is_not_verified_empty_history() -> None:
    row = history([], [("ORDER", "100", 1)], complete=False)
    assert row["is_payment_observation_complete"] is False
    assert row["has_fully_paid_current_obligation"] is None


@pytest.mark.parametrize("additional", [False, True])
def test_edit_requires_new_balance(additional: bool) -> None:
    payments = [("SALE", "SUCCESS", "100", 1)]
    if additional:
        payments.append(("CAPTURE", "SUCCESS", "20", 3))
    row = history(
        payments, [("ORDER", "100", 1), ("ORDER_EDIT", "20", 2)], current="120"
    )
    assert row["has_fully_paid_current_obligation"] is additional


def test_refund_preserves_prior_full_payment() -> None:
    row = history(
        [("SALE", "SUCCESS", "100", 1), ("REFUND", "SUCCESS", "100", 3)],
        [("ORDER", "100", 1), ("REFUND", "-100", 2)],
        current="0",
    )
    assert row["has_fully_paid_current_obligation"] is True
    assert row["is_fully_refunded"] is True


def test_cash_refund_does_not_manufacture_full_payment_between_events() -> None:
    row = history(
        [("SALE", "SUCCESS", "50", 1), ("REFUND", "SUCCESS", "50", 3)],
        [("ORDER", "100", 1), ("REFUND", "-50", 2)],
        current="50",
    )
    assert row["has_fully_paid_current_obligation"] is False


@pytest.mark.parametrize("status", ["PENDING", "AWAITING_RESPONSE"])
def test_unsettled_refund_is_not_a_verified_noncash_credit(status: str) -> None:
    row = history(
        [("SALE", "SUCCESS", "50", 1), ("REFUND", status, "50", 3)],
        [("ORDER", "100", 1), ("REFUND", "-50", 2)],
        current="50",
    )
    assert row["is_payment_observation_complete"] is True
    assert row["is_payment_obligation_verified"] is True
    assert row["is_payment_chronology_resolved"] is False
    assert row["has_fully_paid_current_obligation"] is None
    assert row["net_customer_payment_shop_amount"] is None
    assert row["is_partially_refunded"] is None
    assert row["is_fully_refunded"] is None


def test_verified_credit_reduces_remaining_obligation() -> None:
    row = history(
        [("SALE", "SUCCESS", "50", 1)],
        [("ORDER", "100", 1), ("REFUND", "-50", 2)],
        current="50",
    )
    assert row["has_fully_paid_current_obligation"] is True


def test_edit_after_refund_requires_new_collection() -> None:
    row = history(
        [("SALE", "SUCCESS", "100", 1), ("REFUND", "SUCCESS", "20", 2)],
        [("ORDER", "100", 1), ("REFUND", "-20", 2), ("ORDER_EDIT", "20", 3)],
        current="100",
    )
    assert row["has_fully_paid_current_obligation"] is False


def test_header_mismatch_does_not_prove_an_obligation() -> None:
    row = history([("SALE", "SUCCESS", "100", 1)], [("ORDER", "100", 1)], current="80")
    assert row["is_payment_obligation_verified"] is False
    assert row["has_fully_paid_current_obligation"] is None


@pytest.mark.parametrize(
    "live",
    [
        False,
        pytest.param(
            True,
            marks=pytest.mark.skipif(
                os.getenv("ORDER_EVIDENCE_LIVE_ACCEPTANCE") != "development",
                reason="explicit development Snowflake acceptance only",
            ),
        ),
    ],
)
@pytest.mark.parametrize(
    ("ledger_fee", "collected", "expected"),
    [(False, "162", True), (True, "162", True), (True, "150", False)],
)
def test_handling_fee_preserves_revenue_and_fee_inclusive_payment_obligation(
    ledger_fee: bool, collected: str, expected: bool, live: bool
) -> None:
    ds = Dataset()
    complete(ds)
    ds.tables["stg_shopify_orders__orders"][0].update(
        original_total_price_shop_amount=Decimal("162"),
        source_observed_at=datetime(2026, 10, 10),
        source_query_version="fixture-query",
        extraction_revision=3,
        transactions_count=2,
        transactions_count_precision="EXACT",
        is_test=False,
    )
    ds.tables["stg_shopify_orders__agreements"][1]["reason"] = "REFUND"
    ds.tables["stg_shopify_orders__refunds"][0]["total_refunded_shop_amount"] = Decimal(
        "82.20"
    )
    for tx in ds.tables["stg_shopify_orders__transactions"]:
        tx["transaction_shop_amount"] = Decimal(
            "82.20" if tx["kind"] == "REFUND" else collected
        )
    ds.add(
        "stg_shopify_orders__refund_adjustments",
        shop_id="s1",
        order_id="o1",
        refund_id="o1-refund",
        adjustment_id="fee",
        adjustment_reason="REFUND_DISCREPANCY",
        adjustment_shop_amount=Decimal("15"),
        adjustment_shop_currency_code="USD",
        adjustment_tax_shop_amount=Decimal("0"),
        adjustment_tax_shop_currency_code="USD",
    )
    ds.add(
        "int_shopify_refunds__handling_fees",
        shop_id="s1",
        order_id="o1",
        refund_id="o1-refund",
        handling_fee_status="matched_loop_handling_fee",
        handling_fee_shop_amount=Decimal("15"),
    )
    if ledger_fee:
        sell(
            ds,
            "o1",
            "fee-sale",
            sale_type="AdjustmentSale",
            action="RETURN",
            line=None,
            total="15",
            tax="0",
        )
    warehouse = Warehouse(ds.tables)
    try:
        commercial = warehouse.rows("int_shopify_orders__commercial_components")[0]
        payment = warehouse.rows("int_shopify_orders__payment_history")[0]
        if live:
            assert snowflake_history(ds) == payment
        eligibility = warehouse.rows("int_shopify_orders__revenue_eligibility")[0]
        assert commercial["commercial_resolution_status"] == "complete"
        assert commercial["net_revenue_shop_amount"] == Decimal("60")
        assert payment["is_payment_obligation_verified"] is True
        assert payment["has_fully_paid_current_obligation"] is expected
        assert eligibility["is_revenue_eligible"] is expected
    finally:
        warehouse.con.close()


def snowflake_history(ds: Dataset) -> Row:
    """Execute the maintained SQL over bound fixtures; no schemas or data writes."""
    assert os.environ["ANALYTICS_ENVIRONMENT"] == "development"
    assert os.environ["SNOWFLAKE_DATABASE"]
    assert os.environ["SNOWFLAKE_WAREHOUSE"]
    ctes: list[str] = []
    parameters: list[object] = []
    for name, schema in FIXTURE_SCHEMAS.items():
        selects: list[str] = []
        for row in ds.tables[name]:
            selects.append(
                "select "
                + ", ".join(
                    f"cast(%s as {kind}) as {column}" for column, kind in schema.items()
                )
            )
            parameters.extend(row.get(column) for column in schema)
        if not selects:
            selects.append(
                "select "
                + ", ".join(
                    f"cast(null as {kind}) as {column}"
                    for column, kind in schema.items()
                )
                + " where false"
            )
        ctes.append(f"{name} as ({' union all '.join(selects)})")
    for name in (
        "int_shopify_sales__ledger",
        "int_shopify_transactions__payment_flows",
        "int_shopify_orders__payment_history",
    ):
        sql = REF.sub(lambda match: match[1], (MODELS / f"{name}.sql").read_text())
        ctes.append(f"{name} as ({sql})")
    query = (
        "with " + ", ".join(ctes) + " select * from int_shopify_orders__payment_history"
    )
    with (
        snowflake.connector.connect(  # pyright: ignore[reportUnknownMemberType]
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            role=os.environ["SNOWFLAKE_ROLE"],
            warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
            database=os.environ["SNOWFLAKE_DATABASE"],
            private_key_file=os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
            login_timeout=20,
            network_timeout=60,
            session_parameters={"TIMEZONE": "UTC", "STATEMENT_TIMEOUT_IN_SECONDS": 60},
        ) as connection,
        connection.cursor() as cursor,
    ):
        _ = cursor.execute(query, tuple(parameters))
        assert cursor.description is not None
        columns = [str(column.name).lower() for column in cursor.description]
        row: tuple[object, ...] | dict[str, object] | None = cursor.fetchone()
        assert isinstance(row, tuple)
        return dict(zip(columns, row, strict=True))


@pytest.mark.skipif(
    os.getenv("ORDER_EVIDENCE_LIVE_ACCEPTANCE") != "development",
    reason="explicit development Snowflake acceptance only",
)
@pytest.mark.parametrize(
    ("payments", "changes", "current"),
    [
        ([("SALE", "SUCCESS", "100", 1)], [("ORDER", "100", 1)], "100"),
        (
            [("SALE", "SUCCESS", "100", 1)],
            [("ORDER", "100", 1), ("ORDER_EDIT", "20", 2)],
            "120",
        ),
        (
            [("SALE", "SUCCESS", "100", 1), ("CAPTURE", "SUCCESS", "20", 3)],
            [("ORDER", "100", 1), ("ORDER_EDIT", "20", 2)],
            "120",
        ),
        (
            [("SALE", "SUCCESS", "100", 1), ("REFUND", "SUCCESS", "100", 3)],
            [("ORDER", "100", 1), ("REFUND", "-100", 2)],
            "0",
        ),
        (
            [("SALE", "SUCCESS", "50", 1), ("REFUND", "SUCCESS", "50", 3)],
            [("ORDER", "100", 1), ("REFUND", "-50", 2)],
            "50",
        ),
        (
            [("SALE", "SUCCESS", "50", 1)],
            [("ORDER", "100", 1), ("REFUND", "-50", 2)],
            "50",
        ),
        (
            [("SALE", "SUCCESS", "50", 1), ("REFUND", "PENDING", "50", 3)],
            [("ORDER", "100", 1), ("REFUND", "-50", 2)],
            "50",
        ),
        (
            [("SALE", "SUCCESS", "50", 1), ("REFUND", "AWAITING_RESPONSE", "50", 3)],
            [("ORDER", "100", 1), ("REFUND", "-50", 2)],
            "50",
        ),
    ],
)
def test_snowflake_payment_history_matches_duckdb(
    payments: list[tuple[str, str, str, int]],
    changes: list[tuple[str, str, int]],
    current: str,
) -> None:
    assert history(payments, changes, current=current, live=True) == history(
        payments, changes, current=current
    )
