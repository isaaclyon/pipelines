"""Revenue exclusions and unresolved requirements remain independently visible."""

from pathlib import Path

import duckdb
import pytest

from tests.test_shopify_commercial_components import REF

MODEL = (
    Path(__file__).parents[1]
    / "dbt/models/intermediate/shopify_orders"
    / "int_shopify_orders__revenue_eligibility.sql"
)


@pytest.mark.parametrize(
    ("test", "paid", "commercial", "non_gift", "unknown_gift", "expected"),
    [
        (False, True, True, 1, 0, True),
        (True, True, True, 1, 0, False),
        (None, True, True, 1, 0, None),
        (False, False, True, 1, 0, False),
        (False, None, True, 1, 0, None),
        (False, True, False, 1, 0, None),
        (False, True, True, 0, 0, False),
        (False, True, True, 0, 1, None),
        (True, None, False, 0, 0, False),
    ],
)
def test_revenue_eligibility(
    test: bool | None,
    paid: bool | None,
    commercial: bool,
    non_gift: int,
    unknown_gift: int,
    expected: bool | None,
) -> None:
    with duckdb.connect() as db:
        _ = db.execute(
            "create table stg_shopify_orders__orders as "
            "select 's1' shop_id, 'o1' order_id, ?::boolean is_test",
            [test],
        )
        _ = db.execute(
            "create table int_shopify_orders__payment_history as "
            "select 's1' shop_id, 'o1' order_id, "
            "?::boolean has_fully_paid_current_obligation, "
            "?::boolean is_payment_observation_complete, "
            "?::boolean is_payment_obligation_verified, "
            "?::boolean is_payment_chronology_resolved",
            [paid, paid is not None, paid is not None, paid is not None],
        )
        _ = db.execute(
            "create table int_shopify_orders__commercial_components as "
            "select 's1' shop_id, 'o1' order_id, ?::boolean is_commercial_resolved, "
            "?::bigint non_gift_card_line_count, "
            "?::bigint unknown_gift_card_line_count",
            [commercial, non_gift, unknown_gift],
        )
        _ = db.execute(
            "create table stg_shopify_orders__order_lines as "
            "select 's1' shop_id, 'o1' order_id, true is_gift_card"
        )
        sql = REF.sub(lambda match: match[1], MODEL.read_text())
        _ = db.execute(f"create table eligibility as {sql}")
        row: tuple[object, ...] | None = db.execute(
            "select is_revenue_eligible, revenue_exclusion_reasons, "
            "revenue_unresolved_requirements from eligibility"
        ).fetchone()
    assert row is not None
    assert row[0] is expected
    exclusions = str(row[1]).split("|")
    unresolved = str(row[2]).split("|")
    assert ("test_order" in exclusions) is (test is True)
    assert ("payment_not_fully_collected" in exclusions) is (paid is False)
    assert ("test_status_unknown" in unresolved) is (test is None)
    assert ("payment_evidence_incomplete" in unresolved) is (paid is None)
    assert ("payment_obligation_unknown" in unresolved) is (paid is None)
    assert ("payment_chronology_unresolved" in unresolved) is (paid is None)
    assert ("commercial_value_unknown" in unresolved) is (
        not commercial or unknown_gift > 0
    )
