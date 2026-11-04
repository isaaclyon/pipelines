"""Check the published Shopify order marts and the checks that guard them.

Most tests read the shared project build or change a copy of it and run one
check. Calculation rules live in dbt unit tests beside each model.
"""

import json
import os
import subprocess
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

from tests.project_build import GEOGRAPHY_PROBES, NAMESPACE, ProjectBuild
from tests.shopify_order_fixtures import (
    GOOD_CODES,
    GOOD_LINES,
    GOOD_ORDERS,
    GOOD_TAGS,
    ROOT,
    fill_raw,
    line_row,
    order_row,
    write_raw,
)

# Real dbt project nodes these tests read or execute (ADR 0026).
pytestmark = pytest.mark.dbt("+path:models/marts/shopify_orders")
# The shared build's resolved commercial orders, and the orders that test the
# other rules.
COMMERCIAL_ORDERS = "shopify_order_id like 'money-%'"
SCENARIO_ORDERS = f"not ({COMMERCIAL_ORDERS})"


def _failed_tests(target: Path) -> set[str]:
    results = json.loads((target / "run_results.json").read_text())  # pyright: ignore[reportAny] # dbt artifact read in a test.
    return {
        str(result["unique_id"]).split(".")[2]  # pyright: ignore[reportAny]
        for result in results["results"]  # pyright: ignore[reportAny]
        if result["status"] in ("fail", "error")
    }


def test_availability_is_published_with_facts_without_inventing_source_coverage(
    project_build: ProjectBuild,
) -> None:
    published, generation = project_build.database, project_build.generation
    with duckdb.connect(str(published), read_only=True) as db:
        rows = db.execute(
            "select publication_id, shopify_shop_id, shopify_coverage_started_at, "
            "shopify_coverage_ended_at, loop_snapshot_ended_at, "
            "is_loop_snapshot_complete "
            f"from {NAMESPACE}_marts.fct_data_availability"
        ).fetchall()
    assert rows == [(generation, "shop-1", None, None, None, False)]


def test_marts_publish_one_row_per_order_with_exact_rollups(
    project_build: ProjectBuild,
) -> None:
    published = project_build.database
    with duckdb.connect(str(published), read_only=True) as db:
        rows = db.execute(
            "select shopify_order_id, cast(order_date as varchar), order_date_source,"
            " is_cancelled, line_count,"
            " gross_ordered_quantity, original_line_value_shop_amount"
            f" from {NAMESPACE}_marts.fct_orders where {SCENARIO_ORDERS}"
            " order by shopify_order_id"
        ).fetchall()
        contacts = db.execute(
            "select shopify_order_id, order_email, shipping_address_province_code"
            f" from {NAMESPACE}_marts.dim_order_contacts where {SCENARIO_ORDERS}"
            " order by shopify_order_id"
        ).fetchall()
    assert rows == [
        ("1001", "2026-07-01", "processed_at", False, 2, 3, Decimal("120.000000000")),
        ("1002", "2026-06-30", "processed_at", True, 1, 3, Decimal("0.999999999")),
        ("1003", "2026-07-01", "processed_at", False, 0, 0, Decimal("0E-9")),
        ("1004", "2026-07-02", "created_at", False, 0, 0, Decimal("0E-9")),
    ]
    assert contacts == [
        ("1001", "1001@example.com", "UT"),
        ("1002", "1002@example.com", "UT"),
        ("1003", "1003@example.com", "UT"),
        ("1004", "1004@example.com", "UT"),
    ]


def test_line_mart_preserves_source_detail_and_parent_context(
    project_build: ProjectBuild,
) -> None:
    published = project_build.database
    with duckdb.connect(str(published), read_only=True) as db:
        rows = db.execute(
            "select shopify_shop_id, shopify_order_id, shopify_line_item_id,"
            " sku, line_name, title, variant_title, shopify_product_id,"
            " shopify_variant_id, is_gift_card_item, gross_ordered_quantity,"
            " current_quantity, original_unit_price_shop_amount,"
            " original_unit_price_presentment_amount, discounted_total_shop_amount,"
            " discounted_total_presentment_amount, original_line_value_shop_amount,"
            " original_line_value_presentment_amount, shop_currency_code,"
            " presentment_currency_code"
            f" from {NAMESPACE}_marts.fct_order_lines where {SCENARIO_ORDERS}"
            " order by shopify_shop_id, shopify_order_id, shopify_line_item_id"
        ).fetchall()
        context = db.execute(
            "select shopify_customer_id, cast(order_date as varchar),"
            " order_date_source, is_test, is_cancelled, source_name,"
            " financial_status, fulfillment_status, shipping_address_city,"
            " shipping_address_province_code, shipping_address_country_code"
            f" from {NAMESPACE}_marts.fct_order_lines where {SCENARIO_ORDERS}"
            " order by shopify_order_id, shopify_line_item_id"
        ).fetchall()
        column_rows: list[tuple[object, ...]] = db.execute(
            "select column_name from information_schema.columns "
            "where table_name = 'fct_order_lines' "
            f"and table_schema = '{NAMESPACE}_marts'"
        ).fetchall()
        columns = {str(row[0]) for row in column_rows}
    assert rows == [
        (
            "shop-1",
            "1001",
            "1",
            "BLANKET-1",
            "Blanket",
            "Blanket",
            "Blue",
            "product-1",
            "variant-1",
            False,
            2,
            1,
            Decimal("10.005"),
            Decimal("13.700000001"),
            Decimal("18.01"),
            Decimal("25.02"),
            Decimal("20.01"),
            Decimal("27.400000002"),
            "USD",
            "CAD",
        ),
        (
            "shop-1",
            "1001",
            "2",
            "BLANKET-1",
            "Blanket",
            "Blanket",
            "Blue",
            "product-1",
            "variant-1",
            False,
            1,
            1,
            Decimal("99.99"),
            Decimal("99.99"),
            Decimal("99.99"),
            Decimal("99.99"),
            Decimal("99.99"),
            Decimal("99.99"),
            "USD",
            "CAD",
        ),
        (
            "shop-1",
            "1002",
            "3",
            None,
            "Blanket",
            None,
            None,
            None,
            None,
            True,
            3,
            0,
            Decimal("0.333333333"),
            Decimal("0.333333333"),
            Decimal("0.999999999"),
            Decimal("0.999999999"),
            Decimal("0.999999999"),
            Decimal("0.999999999"),
            "USD",
            "CAD",
        ),
    ]
    assert context == [
        (
            "customer-1",
            "2026-07-01",
            "processed_at",
            False,
            False,
            "web",
            "PAID",
            "UNFULFILLED",
            "Salt Lake City",
            "UT",
            "US",
        ),
        (
            "customer-1",
            "2026-07-01",
            "processed_at",
            False,
            False,
            "web",
            "PAID",
            "UNFULFILLED",
            "Salt Lake City",
            "UT",
            "US",
        ),
        (
            None,
            "2026-06-30",
            "processed_at",
            True,
            True,
            None,
            None,
            "UNFULFILLED",
            GEOGRAPHY_PROBES["shipping_address_city"],
            "UT",
            "US",
        ),
    ]
    assert not columns & {
        "order_email",
        "order_phone",
        "shipping_address_first_name",
        "shipping_address_address1",
        "billing_address_address1",
        "total_price_shop_amount",
        "net_sales_shop_amount",
    }


@pytest.mark.parametrize(
    "column",
    [
        "gross_ordered_quantity",
        "original_line_value_shop_amount",
        "original_line_value_presentment_amount",
    ],
)
def test_line_reconciliation_rejects_cross_order_drift(
    project_build: ProjectBuild,
    tmp_path: Path,
    column: str,
) -> None:
    """Equal and opposite errors preserve global totals but must fail per order."""
    candidate = project_build.copy(tmp_path)
    with duckdb.connect(str(candidate)) as db:
        _ = db.execute(
            f"update {NAMESPACE}_marts.fct_order_lines set {column} = {column} + "
            "case when shopify_order_id = '1001' then 1 else -1 end "
            "where shopify_line_item_id in ('1', '3')"
        )
    target = tmp_path / "target"
    assert _failing_candidate_tests(
        project_build, candidate, target, "fct_order_lines_reconcile_orders"
    ) == {"fct_order_lines_reconcile_orders"}


def _failing_candidate_tests(
    project_build: ProjectBuild, candidate: Path, target: Path, *selection: str
) -> set[str]:
    """Run data tests against a modified copy of the shared build."""
    result = subprocess.run(
        [
            "dbt",
            "test",
            "--project-dir",
            str(ROOT / "dbt"),
            "--profiles-dir",
            str(ROOT / "dbt"),
            "--target-path",
            str(target),
            "--select",
            *selection,
        ],
        env=project_build.env(candidate),
        capture_output=True,
        check=False,
        timeout=60,
    )
    return _failed_tests(target) if result.returncode else set()


def test_fct_orders_publishes_channel_and_characteristic_flags(
    project_build: ProjectBuild,
) -> None:
    published = project_build.database
    with duckdb.connect(str(published), read_only=True) as db:
        rows = db.execute(
            "select shopify_order_id, has_classification_evidence,"
            " has_bulk_evidence, is_d2c, is_commercial, is_retail, is_specialty,"
            " is_bulk, is_replacement, is_broadcast, is_employee_order"
            f" from {NAMESPACE}_marts.fct_orders where {SCENARIO_ORDERS}"
            " order by shopify_order_id"
        ).fetchall()
    assert rows == [
        # " Broadcast " normalizes to a Broadcast tag; no commercial match.
        ("1001", True, False, True, False, False, False, False, False, True, False),
        # "Vera-Bulk" contains bulk. Test orders keep their channel.
        ("1002", True, True, False, True, False, False, True, False, False, False),
        # Revision 1: tags, codes and email were never selected.
        ("1003", False, None, None, None, False, None, None, None, None, None),
        # " Free-Replacement " normalizes to a replacement discount code.
        ("1004", True, False, True, False, False, False, False, True, False, False),
    ]


@pytest.mark.parametrize(
    "tampering",
    [
        pytest.param(
            "is_d2c = true where shopify_order_id = '1002'", id="d2c-and-bulk"
        ),
        pytest.param(
            "is_bulk = true, is_commercial = false where shopify_order_id = '1001'",
            id="subchannel-without-commercial",
        ),
        pytest.param(
            "is_specialty = true where shopify_order_id = '1002'",
            id="two-subchannels",
        ),
        pytest.param(
            "is_d2c = true, is_commercial = false where shopify_order_id = '1003'",
            id="d2c-from-missing-evidence",
        ),
        pytest.param(
            "is_replacement = false where shopify_order_id = '1003'",
            id="verdict-without-evidence",
        ),
        pytest.param(
            "is_gifted = true where shopify_order_id = '1001'",
            id="gifting-without-tag-or-employee",
        ),
        pytest.param(
            "is_gifted = false where shopify_order_id = '1003'",
            id="gifting-without-evidence",
        ),
    ],
)
def test_channel_consistency_rejects_incorrect_classification(
    project_build: ProjectBuild,
    tmp_path: Path,
    tampering: str,
) -> None:
    candidate = project_build.copy(tmp_path)
    with duckdb.connect(str(candidate)) as db:
        _ = db.execute(f"update {NAMESPACE}_marts.fct_orders set {tampering}")
    assert _failing_candidate_tests(
        project_build, candidate, tmp_path / "target", "fct_orders_channels_consistent"
    ) == {"fct_orders_channels_consistent"}


def test_staged_quantities_preserve_nulls_zero_and_large_integers(
    tmp_path: Path,
) -> None:
    raw = tmp_path / "warehouse.duckdb"
    large = 2**31 + 7
    write_raw(
        raw,
        [],
        [
            line_row("1001", "1", large, "0") | {"current_quantity": 0},
            line_row("1001", "2", 0, "0")
            | {"quantity": None, "current_quantity": large},
            line_row("1001", "3", 0, "0") | {"current_quantity": None},
        ],
    )
    _ = subprocess.run(
        [
            "dbt",
            "run",
            "--project-dir",
            str(ROOT / "dbt"),
            "--profiles-dir",
            str(ROOT / "dbt"),
            "--target-path",
            str(tmp_path / "target"),
            "--select",
            "stg_shopify_orders__order_lines",
        ],
        env=dict(
            os.environ,
            ANALYTICS_ENVIRONMENT="ci",
            DBT_TARGET="local",
            DBT_NAMESPACE=NAMESPACE,
            DUCKDB_PATH=str(raw),
            SHOPIFY_ORDERS_DATASET="raw",
        ),
        check=True,
        timeout=120,
    )
    with duckdb.connect(str(raw), read_only=True) as db:
        result = db.execute(
            "select order_line_quantity, current_quantity"
            f" from {NAMESPACE}_staging.stg_shopify_orders__order_lines"
            " order by line_item_id"
        )
        assert [str(column[1]) for column in result.description] == ["BIGINT", "BIGINT"]
        assert result.fetchall() == [(large, 0), (None, large), (0, None)]


CURRENCIES = "shopify_orders_currencies_match"


@pytest.mark.parametrize(
    ("orders", "lines", "failing_test"),
    [
        pytest.param(
            GOOD_ORDERS,
            [*GOOD_LINES, GOOD_LINES[0] | {"order_id": "missing-order"}],
            CURRENCIES,
            id="orphan-line",
        ),
        pytest.param(
            GOOD_ORDERS,
            [*GOOD_LINES, GOOD_LINES[0]],
            "shopify_staged_lines_unique_line",
            id="duplicate-line",
        ),
        pytest.param(
            [*GOOD_ORDERS, GOOD_ORDERS[0]],
            GOOD_LINES,
            "shopify_staged_orders_unique_order",
            id="duplicate-order",
        ),
        pytest.param(
            [
                *GOOD_ORDERS[:2],
                order_row("1003", "2026-07-01 12:00:00+00", currency_code="EUR"),
            ],
            GOOD_LINES,
            CURRENCIES,
            id="non-usd-shop-currency",
        ),
        pytest.param(
            GOOD_ORDERS,
            [
                *GOOD_LINES[:2],
                line_row("1002", "3", 3, "0.333333333")
                | {"original_unit_price_presentment_currency": "USD"},
            ],
            CURRENCIES,
            id="line-currency-differs-from-order",
        ),
        pytest.param(
            [*GOOD_ORDERS[:3], GOOD_ORDERS[3] | {"currency_code": None}],
            GOOD_LINES,
            CURRENCIES,
            id="missing-shop-currency",
        ),
        pytest.param(
            [*GOOD_ORDERS[:3], GOOD_ORDERS[3] | {"total_tax_shop_currency": None}],
            GOOD_LINES,
            CURRENCIES,
            id="missing-amount-currency",
        ),
        pytest.param(
            GOOD_ORDERS,
            [
                *GOOD_LINES[:2],
                GOOD_LINES[2] | {"discounted_total_presentment_currency": "USD"},
            ],
            CURRENCIES,
            id="line-discounted-currency-differs",
        ),
    ],
)
def test_bad_raw_data_fails_model_checks(
    project_build: ProjectBuild,
    tmp_path: Path,
    orders: list[dict[str, object]],
    lines: list[dict[str, object]],
    failing_test: str,
) -> None:
    _assert_rejected(
        project_build,
        tmp_path,
        (orders, lines, GOOD_TAGS, GOOD_CODES),
        failing_test,
    )


@pytest.mark.parametrize(
    ("tags", "codes", "failing_test"),
    [
        pytest.param(
            [*GOOD_TAGS, GOOD_TAGS[0]],
            GOOD_CODES,
            "shopify_staged_order_tags_unique_tag",
            id="duplicate-tag",
        ),
        pytest.param(
            GOOD_TAGS,
            [*GOOD_CODES, GOOD_CODES[0]],
            "shopify_staged_order_discount_codes_unique_code",
            id="duplicate-discount-code",
        ),
    ],
)
def test_bad_classification_inputs_fail_model_checks(
    project_build: ProjectBuild,
    tmp_path: Path,
    tags: list[dict[str, object]],
    codes: list[dict[str, object]],
    failing_test: str,
) -> None:
    _assert_rejected(
        project_build,
        tmp_path,
        (GOOD_ORDERS, GOOD_LINES, tags, codes),
        failing_test,
    )


# Staged models read from the replaced raw tables.
STAGED = ("orders", "order_lines", "order_tags", "order_discount_codes")


def _assert_rejected(
    project_build: ProjectBuild,
    tmp_path: Path,
    raw_rows: tuple[
        list[dict[str, object]],
        list[dict[str, object]],
        list[dict[str, object]],
        list[dict[str, object]],
    ],
    failing_test: str,
) -> None:
    """Only the expected check rejects the raw rows.

    Staging models are views, so a copy of the built fixture reads the replaced
    raw rows. Publication runs these checks before anything downstream.
    """
    candidate = project_build.copy(tmp_path)
    with duckdb.connect(str(candidate)) as db:
        fill_raw(db, *raw_rows)
    assert _failing_candidate_tests(
        project_build,
        candidate,
        tmp_path / "target",
        "source:shopify_orders",
        *(f"stg_shopify_orders__{name}" for name in STAGED),
        CURRENCIES,
        "--indirect-selection",
        "cautious",
    ) == {failing_test}


def test_order_facts_carry_their_publication_generation(
    project_build: ProjectBuild,
) -> None:
    published, generation = project_build.database, project_build.generation
    with duckdb.connect(str(published), read_only=True) as db:
        assert db.execute(
            f"select distinct publication_id from {NAMESPACE}_marts.fct_orders"
        ).fetchall() == [(generation,)]


def test_reviewed_contract_uses_source_qualified_identities() -> None:
    contracts = (ROOT / "dbt" / "mart-contracts.json").read_text()
    assert '"shopify_order_id"' in contracts
    assert '"shopify_shop_id"' in contracts
    assert '"shopify_customer_id"' in contracts
    assert '"taxes_included"' not in contracts


def test_net_units_publish_without_recounting_refunds(
    project_build: ProjectBuild,
) -> None:
    published = project_build.database
    with duckdb.connect(str(published), read_only=True) as db:
        assert db.execute(
            "select shopify_order_id, net_ordered_quantity"
            f" from {NAMESPACE}_marts.fct_orders where {SCENARIO_ORDERS}"
            " order by shopify_order_id"
        ).fetchall() == [("1001", 2), ("1002", 0), ("1003", 0), ("1004", 0)]
        assert db.execute(
            "select shopify_line_item_id, net_ordered_quantity, net_quantity_status"
            f" from {NAMESPACE}_marts.fct_order_lines where {SCENARIO_ORDERS}"
            " order by shopify_line_item_id"
        ).fetchall() == [
            ("1", 1, "current_quantity"),
            ("2", 1, "current_quantity"),
            ("3", 0, "current_quantity"),
        ]


def test_missing_commercial_evidence_publishes_unknown_not_zero(
    project_build: ProjectBuild,
) -> None:
    published = project_build.database
    with duckdb.connect(str(published), read_only=True) as db:
        rows = db.execute(
            "select commercial_resolution_status, net_revenue_shop_amount, "
            "net_merchandise_shop_amount "
            f"from {NAMESPACE}_marts.fct_orders where {SCENARIO_ORDERS}"
        ).fetchall()
    assert len(rows) == len(GOOD_ORDERS)
    assert all(row == ("no_agreement_evidence", None, None) for row in rows)


def test_commercial_amounts_and_allocations_build_marts(
    project_build: ProjectBuild,
) -> None:
    published = project_build.database
    with duckdb.connect(str(published), read_only=True) as db:
        assert db.execute(
            f"select is_revenue_eligible, has_fully_paid_current_obligation, "
            "is_aov_eligible "
            f"from {NAMESPACE}_marts.fct_orders where {COMMERCIAL_ORDERS} "
            "order by shopify_order_id"
        ).fetchall() == [(True, True, True), (True, True, True)]
        assert (
            db.execute(
                "select commercial_resolution_status, allocation_status, "
                "net_revenue_shop_amount "
                f"from {NAMESPACE}_marts.fct_orders where {COMMERCIAL_ORDERS} "
                "order by shopify_order_id"
            ).fetchall()
            == [("complete", "complete", Decimal("310"))] * 2
        )
        assert (
            db.execute(
                "select net_merchandise_shop_amount, shipping_shop_amount, "
                "shipping_allocation_method, is_asp_eligible, is_aov_eligible "
                f"from {NAMESPACE}_marts.fct_order_lines where {COMMERCIAL_ORDERS} "
                "order by shopify_order_id, shopify_line_item_id"
            ).fetchall()
            == [
                (Decimal("200"), Decimal("6.67"), "value_weight", True, True),
                (Decimal("100"), Decimal("3.33"), "value_weight", True, True),
            ]
            * 2
        )


@pytest.mark.parametrize(
    ("model", "corruption", "failing_test"),
    [
        (
            "fct_order_lines",
            "shipping_shop_amount = shipping_shop_amount + 0.01",
            "shopify_published_commercial_reconcile",
        ),
        (
            "fct_order_lines",
            "net_ordered_quantity = net_ordered_quantity + 1",
            "shopify_published_net_units_reconcile",
        ),
        (
            "fct_orders",
            "net_revenue_shop_amount = net_revenue_shop_amount + 1",
            "shopify_published_commercial_reconcile",
        ),
        (
            "fct_orders",
            "is_revenue_eligible = false",
            "shopify_published_revenue_eligibility",
        ),
        (
            "fct_orders",
            "is_aov_eligible = false",
            "shopify_published_average_eligibility",
        ),
        (
            "fct_order_lines",
            "is_asp_eligible = false",
            "shopify_published_average_eligibility",
        ),
        (
            "fct_order_lines",
            "aov_exclusion_reasons = 'wrong_parent|'",
            "shopify_published_average_eligibility",
        ),
        ("fct_orders", "is_gifted = true", "fct_orders_channels_consistent"),
    ],
    ids=[
        "line-allocation",
        "line-net-units",
        "order-net-revenue",
        "order-eligibility",
        "order-aov",
        "line-asp",
        "line-parent-reasons",
        "order-gifting",
    ],
)
def test_bad_mart_candidate_fails_model_checks(
    project_build: ProjectBuild,
    tmp_path: Path,
    model: str,
    corruption: str,
    failing_test: str,
) -> None:
    """A mart that drifts from its inputs fails the check that guards it."""
    candidate = project_build.copy(tmp_path)
    with duckdb.connect(str(candidate)) as db:
        _ = db.execute(
            f"update {NAMESPACE}_marts.{model} set {corruption} "
            f"where {COMMERCIAL_ORDERS}"
        )
    target = tmp_path / "target"
    assert _failing_candidate_tests(project_build, candidate, target, failing_test) == {
        failing_test
    }


def test_published_revenue_eligibility_preserves_unknown_and_excluded(
    project_build: ProjectBuild,
) -> None:
    published = project_build.database
    with duckdb.connect(str(published), read_only=True) as db:
        rows: list[tuple[object, ...]] = db.execute(
            "select shopify_order_id, is_revenue_eligible, "
            "revenue_exclusion_reasons, revenue_unresolved_requirements "
            f"from {NAMESPACE}_marts.fct_orders where {SCENARIO_ORDERS} "
            "order by shopify_order_id"
        ).fetchall()
    assert len(rows) == 4
    for order_id, eligible, exclusions, unresolved in rows:
        assert isinstance(exclusions, str)
        assert isinstance(unresolved, str)
        assert eligible is (False if order_id == "1002" else None)
        assert ("test_order|" in exclusions) is (order_id == "1002")
        assert "payment_evidence_incomplete|" in unresolved
        assert "payment_obligation_unknown|" in unresolved
        assert "commercial_value_unknown|" in unresolved
