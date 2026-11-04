"""AOV and ASP have separate, evidence-aware populations, not revenue filters."""

from pathlib import Path

import duckdb
import pytest

from tests.test_shopify_commercial_components import REF

MODELS = Path(__file__).parents[1] / "dbt/models/intermediate/shopify_orders"


def build(db: duckdb.DuckDBPyConnection, model: str) -> None:
    sql = REF.sub(lambda match: match[1], (MODELS / f"{model}.sql").read_text())
    _ = db.execute(f"create table {model} as {sql}")


@pytest.mark.parametrize(
    (
        "revenue",
        "cancelled",
        "gifted",
        "replacement",
        "exchange",
        "refunded",
        "value",
        "aov",
        "asp",
    ),
    [
        (True, False, False, False, False, False, "100", True, True),
        (False, False, False, False, False, False, "100", False, False),
        (None, False, False, False, False, False, "100", None, None),
        (True, True, False, False, False, False, "100", False, False),
        (True, False, True, False, False, False, "100", False, False),
        (True, False, False, True, False, False, "100", False, False),
        (True, False, False, False, True, False, "100", False, False),
        (True, False, None, False, False, False, "100", None, None),
        (True, False, False, None, False, False, "100", None, None),
        (True, False, False, False, None, False, "100", None, None),
        (True, False, False, False, False, True, "0", False, True),
        (True, False, False, False, False, False, "0", False, True),
        (True, False, False, False, False, False, "-5", False, True),
        (True, False, False, False, False, None, "100", None, True),
        (True, False, False, False, False, False, None, None, True),
        (None, True, None, True, True, None, None, False, False),
        (True, False, False, False, False, False, "178.75", True, True),
    ],
)
def test_order_average_populations(
    revenue: bool | None,
    cancelled: bool,
    gifted: bool | None,
    replacement: bool | None,
    exchange: bool | None,
    refunded: bool | None,
    value: str | None,
    aov: bool | None,
    asp: bool | None,
) -> None:
    with duckdb.connect() as db:
        _ = db.execute(
            "create table stg_shopify_orders__orders as select 's1' shop_id, "
            "'o1' order_id, case when ? then timestamp '2026-01-01' end cancelled_at",
            [cancelled],
        )
        _ = db.execute(
            "create table int_shopify_orders__revenue_eligibility as select "
            "'s1' shop_id, 'o1' order_id, ?::boolean is_revenue_eligible, "
            "?::varchar revenue_exclusion_reasons, "
            "?::varchar revenue_unresolved_requirements",
            [
                revenue,
                "test_order|" if revenue is False else "",
                "payment_evidence_incomplete|" if revenue is None else "",
            ],
        )
        _ = db.execute(
            "create table int_shopify_orders__classifications as select "
            "'s1' shop_id, 'o1' order_id, ?::boolean is_gifted, "
            "?::boolean is_replacement, ?::boolean is_exchange",
            [gifted, replacement, exchange],
        )
        _ = db.execute(
            "create table int_shopify_orders__payment_history as select "
            "'s1' shop_id, 'o1' order_id, ?::boolean is_fully_refunded",
            [refunded],
        )
        _ = db.execute(
            "create table int_shopify_orders__commercial_components as select "
            "'s1' shop_id, 'o1' order_id, ?::decimal(38,9) net_revenue_shop_amount",
            [value],
        )
        # Same order ID in another shop must not supply classification or proof.
        for table in (
            "int_shopify_orders__revenue_eligibility",
            "int_shopify_orders__classifications",
            "int_shopify_orders__payment_history",
            "int_shopify_orders__commercial_components",
        ):
            _ = db.execute(
                f"insert into {table} select * replace ('s2' as shop_id) from {table}"
            )
        build(db, "int_shopify_orders__average_eligibility")
        assert db.execute(
            "select count(*) from int_shopify_orders__average_eligibility"
        ).fetchone() == (1,)
        row: tuple[object, ...] | None = db.execute(
            "select is_aov_eligible, is_asp_order_eligible, aov_exclusion_reasons, "
            "aov_unresolved_requirements, asp_order_exclusion_reasons, "
            "asp_order_unresolved_requirements "
            "from int_shopify_orders__average_eligibility"
        ).fetchone()
    assert row is not None
    assert row[:2] == (aov, asp)
    for reasons in (str(row[2]), str(row[4])):
        assert ("cancelled_order|" in reasons) is cancelled
        assert ("gifted_order|" in reasons) is (gifted is True)
        assert ("replacement_order|" in reasons) is (replacement is True)
        assert ("exchange_order|" in reasons) is (exchange is True)
        assert ("test_order|" in reasons) is (revenue is False)
    for reasons in (str(row[3]), str(row[5])):
        assert ("payment_evidence_incomplete|" in reasons) is (revenue is None)
        assert ("gifting_status_unknown|" in reasons) is (gifted is None)
        assert ("replacement_status_unknown|" in reasons) is (replacement is None)
        assert ("exchange_status_unknown|" in reasons) is (exchange is None)
    assert ("fully_refunded|" in str(row[2])) is (refunded is True)
    assert ("refund_status_unknown|" in str(row[3])) is (refunded is None)


@pytest.mark.parametrize(
    ("parent", "gift_card", "quantity", "gross", "discount", "refund", "expected"),
    [
        (True, False, 1, "100", "0", "0", True),
        (True, False, 1, "100", "0", "-100", True),
        (True, False, 0, "100", "0", "-80", True),
        (True, False, 1, "357.50", "0", "-178.75", True),
        (True, True, 1, "0", "0", "0", False),
        (True, None, 1, "100", "0", "0", None),
        (True, False, None, "100", "0", "0", None),
        (True, False, 1, None, "0", "0", None),
        (True, False, 1, "100", None, "0", None),
        (True, False, 1, "100", "0", None, None),
        (False, False, None, None, None, None, False),
        (None, False, 1, "100", "0", "0", None),
        (None, True, None, None, None, None, False),
    ],
)
def test_line_asp_population(
    parent: bool | None,
    gift_card: bool | None,
    quantity: int | None,
    gross: str | None,
    discount: str | None,
    refund: str | None,
    expected: bool | None,
) -> None:
    with duckdb.connect() as db:
        _ = db.execute(
            "create table stg_shopify_orders__order_lines as select 's1' shop_id, "
            "'o1' order_id, 'l1' line_item_id, ?::boolean is_gift_card",
            [gift_card],
        )
        _ = db.execute(
            "create table int_shopify_orders__average_eligibility as select "
            "'s1' shop_id, 'o1' order_id, ?::boolean is_asp_order_eligible, "
            "?::varchar asp_order_exclusion_reasons, "
            "?::varchar asp_order_unresolved_requirements",
            [
                parent,
                "cancelled_order|" if parent is False else "",
                "payment_evidence_incomplete|" if parent is None else "",
            ],
        )
        _ = db.execute(
            "create table int_shopify_order_lines__net_units as select "
            "'s1' shop_id, 'o1' order_id, 'l1' line_item_id, "
            "?::bigint net_ordered_quantity",
            [quantity],
        )
        _ = db.execute(
            "create table int_shopify_order_lines__commercial_components as select "
            "'s1' shop_id, 'o1' order_id, 'l1' line_item_id, "
            "?::decimal(38,9) gross_merchandise_shop_amount",
            [gross],
        )
        _ = db.execute(
            "create table int_shopify_order_lines__allocations as select "
            "'s1' shop_id, 'o1' order_id, 'l1' line_item_id, "
            "?::decimal(38,9) merchandise_discount_shop_amount, "
            "?::decimal(38,9) merchandise_refund_shop_amount",
            [discount, refund],
        )
        # Repeated line IDs in other orders/shops must not multiply this line.
        for table in (
            "int_shopify_order_lines__net_units",
            "int_shopify_order_lines__commercial_components",
            "int_shopify_order_lines__allocations",
        ):
            _ = db.execute(
                f"insert into {table} select * replace ('s2' as shop_id) from {table}"
            )
            _ = db.execute(
                f"insert into {table} select * replace ('o2' as order_id) from {table}"
            )
        build(db, "int_shopify_order_lines__asp_eligibility")
        assert db.execute(
            "select count(*) from int_shopify_order_lines__asp_eligibility"
        ).fetchone() == (1,)
        row: tuple[object, ...] | None = db.execute(
            "select is_asp_eligible, asp_exclusion_reasons, "
            "asp_unresolved_requirements "
            "from int_shopify_order_lines__asp_eligibility"
        ).fetchone()
    assert row is not None
    assert row[0] is expected
    assert ("gift_card_item|" in str(row[1])) is (gift_card is True)
    assert ("net_quantity_unknown|" in str(row[2])) is (quantity is None)
    assert ("line_commercial_value_unknown|" in str(row[2])) is (
        gross is None or discount is None or refund is None
    )
