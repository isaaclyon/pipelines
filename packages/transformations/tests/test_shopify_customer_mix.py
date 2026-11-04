"""Home-market D2C customer-day mix proves repeat buyers and never invents new ones."""

import json
import os
import subprocess
from datetime import date
from pathlib import Path
from typing import cast

import duckdb
import pytest

from analytics_transformations.column_declarations import check_column_declarations
from analytics_transformations.mart_contracts import mapping, read_json, sequence, text
from tests.shopify_order_fixtures import (
    GOOD_CODES,
    GOOD_LINES,
    GOOD_ORDERS,
    GOOD_TAGS,
    ROOT,
    add_financial_raw,
    write_raw,
)
from tests.test_shopify_commercial_components import REF

MODELS = Path(__file__).parents[1] / "dbt/models/intermediate/shopify_orders"
DAY = date(2026, 7, 2)
BEFORE = date(2026, 7, 1)

# shop, order, customer, reporting-zone order date, test, source name, country,
# AOV verdict, D2C verdict.
Order = tuple[
    str, str, str | None, date, bool, str | None, str | None, bool | None, bool | None
]


def order(
    order_id: str,
    customer: str | None = "c1",
    *,
    shop: str = "s1",
    day: date = DAY,
    test: bool = False,
    source: str | None = "web",
    country: str | None = "US",
    aov: bool | None = True,
    d2c: bool | None = True,
) -> Order:
    return (shop, order_id, customer, day, test, source, country, aov, d2c)


def build(orders: list[Order]) -> duckdb.DuckDBPyConnection:
    db = duckdb.connect()
    _ = db.execute(
        "create table stg_shopify_orders__orders (shop_id varchar, order_id varchar, "
        "customer_id varchar, order_date date, is_test boolean, source_name varchar, "
        "is_home_reporting_market boolean)"
    )
    _ = db.execute(
        "create table int_shopify_orders__average_eligibility (shop_id varchar, "
        "order_id varchar, is_aov_eligible boolean)"
    )
    _ = db.execute(
        "create table int_shopify_orders__classifications (shop_id varchar, "
        "order_id varchar, is_d2c boolean)"
    )
    for row in orders:
        _ = db.execute(
            "insert into stg_shopify_orders__orders values (?, ?, ?, ?, ?, ?, ?)",
            # Staging reports CA separately, as the default project vars do.
            [*row[:6], row[6] != "CA"],
        )
        _ = db.execute(
            "insert into int_shopify_orders__average_eligibility values (?, ?, ?)",
            [row[0], row[1], row[7]],
        )
        _ = db.execute(
            "insert into int_shopify_orders__classifications values (?, ?, ?)",
            [row[0], row[1], row[8]],
        )
    for model in (
        "int_shopify_customers__daily_mix",
        "int_shopify_orders__daily_mix_coverage",
    ):
        sql = REF.sub(lambda match: match[1], (MODELS / f"{model}.sql").read_text())
        _ = db.execute(f"create table {model} as {sql}")
    return db


def mix(orders: list[Order]) -> list[tuple[object, ...]]:
    with build(orders) as db:
        return db.execute(
            "select shop_id, customer_id, order_date, customer_mix_verdict, "
            "unknown_reasons, qualifying_order_count, "
            "first_prior_qualifying_order_date "
            "from int_shopify_customers__daily_mix "
            "order by shop_id, customer_id, order_date"
        ).fetchall()


def coverage(orders: list[Order]) -> list[tuple[object, ...]]:
    with build(orders) as db:
        return db.execute(
            "select shop_id, order_date, unidentified_qualifying_order_count, "
            "eligibility_unknown_customer_count, d2c_unknown_order_count "
            "from int_shopify_orders__daily_mix_coverage "
            "order by shop_id, order_date"
        ).fetchall()


UNPROVEN = "unproven_history_coverage|"
UNRESOLVED = "unresolved_prior_eligibility|"


def test_first_day_buyer_counts_once_and_is_never_new() -> None:
    # Several qualifying orders and a same-day non-US purchase on the first day.
    rows = mix([order("o1"), order("o2"), order("o3", country="CA")])
    assert rows == [("s1", "c1", DAY, "unknown", UNPROVEN, 2, None)]


def test_same_day_purchases_do_not_prove_repeat() -> None:
    later = date(2026, 7, 3)
    rows = mix([order("o1"), order("o2"), order("o3", day=later)])
    # The second day is repeat because of an earlier day, not a same-day order.
    assert rows == [
        ("s1", "c1", DAY, "unknown", UNPROVEN, 2, None),
        ("s1", "c1", later, "repeat", "", 1, DAY),
    ]


def test_prior_qualifying_purchase_anywhere_proves_repeat_despite_gaps() -> None:
    earliest = date(2025, 1, 1)
    for prior in (
        order("p1", day=BEFORE, country="CA"),
        order("p1", day=BEFORE, d2c=False),
        order("p1", day=BEFORE, d2c=None),
        order("p1", day=BEFORE, source="shopify_draft_order"),
    ):
        rows = mix(
            [
                prior,
                # Unresolved and excluded history cannot override positive proof.
                order("p2", day=earliest, aov=None),
                order("p3", day=earliest, aov=False),
                order("o1"),
            ]
        )
        assert rows == [("s1", "c1", DAY, "repeat", "", 1, BEFORE)]


def test_excluded_history_never_proves_new() -> None:
    # Fully loaded, decisively excluded history still lacks coverage proof.
    rows = mix(
        [
            order("p1", day=BEFORE, aov=False),
            order("p2", day=BEFORE, aov=False, test=True),
            order("o1"),
        ]
    )
    assert rows == [("s1", "c1", DAY, "unknown", UNPROVEN, 1, None)]


def test_unresolved_prior_eligibility_is_a_separate_reason() -> None:
    rows = mix([order("p1", day=BEFORE, aov=None), order("o1")])
    assert rows == [("s1", "c1", DAY, "unknown", UNRESOLVED + UNPROVEN, 1, None)]


def test_population_uses_the_daily_us_d2c_scope() -> None:
    excluded = [
        order("o1", "test", test=True),
        order("o2", "draft", source="shopify_draft_order"),
        order("o3", "canada", country="CA"),
        order("o4", "commercial", d2c=False),
        order("o5", "d2c-unknown", d2c=None),
        order("o6", "ineligible", aov=False),
        order("o7", "aov-unknown", aov=None),
    ]
    included = [
        order("o8", "no-country", country=None),
        order("o9", "no-source", source=None),
    ]
    rows = mix(excluded + included)
    assert [row[1] for row in rows] == ["no-country", "no-source"]


def test_shops_keep_customer_ids_distinct() -> None:
    rows = mix([order("p1", shop="s2", day=BEFORE), order("o1")])
    assert rows == [
        ("s1", "c1", DAY, "unknown", UNPROVEN, 1, None),
        ("s2", "c1", BEFORE, "unknown", UNPROVEN, 1, None),
    ]


def test_verdict_restates_from_latest_evidence() -> None:
    history = order("p1", day=BEFORE)
    assert mix([history, order("o1")])[-1][3] == "repeat"
    # A later refund or customer change removes the proof for that day.
    refunded = order("p1", day=BEFORE, aov=False)
    assert mix([refunded, order("o1")]) == [
        ("s1", "c1", DAY, "unknown", UNPROVEN, 1, None)
    ]
    moved = order("p1", "c2", day=BEFORE)
    assert mix([moved, order("o1")])[0][3:5] == ("unknown", UNPROVEN)


def test_coverage_counts_stay_outside_the_customer_population() -> None:
    rows = coverage(
        [
            # Unidentified qualifying orders are orders, never customers.
            order("u1", None),
            order("u2", None),
            # Unidentified orders outside the population are not qualifying.
            order("u3", None, aov=None),
            order("u4", None, test=True),
            # Only eligibility-unknown in scope: counted once per customer.
            order("e1", "unknown-only", aov=None),
            order("e2", "unknown-only", aov=None),
            # A qualifying order puts this customer in the population instead.
            order("q1", "qualifying"),
            order("q2", "qualifying", aov=None),
            # Unknown D2C on orders that are not already excluded.
            order("d1", "channel", d2c=None),
            order("d2", "channel", d2c=None, aov=None),
            order("d3", "channel", d2c=None, aov=False),
            order("d4", "channel", d2c=None, country="CA"),
        ]
    )
    assert rows == [("s1", DAY, 2, 1, 2)]


MIX = "int_shopify_customers__daily_mix"
COVERAGE = "int_shopify_orders__daily_mix_coverage"
# The models have no mart descendant, so no publication build selects them.
SELECTORS = ("+" + MIX, "+" + COVERAGE)
# Every YAML data test on the two models: (model, test, column).
YAML_TESTS = {
    (MIX, "unique_combination", None),
    (MIX, "not_null", "shop_id"),
    (MIX, "not_null", "customer_id"),
    (MIX, "not_null", "order_date"),
    (MIX, "not_null", "customer_mix_verdict"),
    (MIX, "accepted_values", "customer_mix_verdict"),
    (MIX, "not_null", "unknown_reasons"),
    (COVERAGE, "unique_combination", None),
    (COVERAGE, "not_null", "shop_id"),
    (COVERAGE, "not_null", "order_date"),
}


def write_mix_raw(path: Path) -> None:
    """Shared order fixtures, with one customer buying on two Pacific days.

    money-3 copies every raw row of money-1 without a customer, so it is an
    unidentified qualifying order.
    """
    write_raw(path, GOOD_ORDERS, GOOD_LINES, GOOD_TAGS, GOOD_CODES)
    add_financial_raw(path)
    with duckdb.connect(str(path)) as db:
        tables = cast(
            list[tuple[str]],
            db.execute(
                "select distinct table_name from information_schema.columns "
                "where table_schema = 'raw' and column_name = 'order_id'"
            ).fetchall(),
        )
        for (table,) in tables:
            columns = cast(
                list[tuple[str, str]],
                db.execute(
                    "select column_name, data_type from information_schema.columns "
                    "where table_schema = 'raw' and table_name = ? "
                    "order by ordinal_position",
                    [table],
                ).fetchall(),
            )
            values = ", ".join(
                f"replace(\"{name}\", 'money-1', 'money-3')"
                if kind == "VARCHAR"
                else f'"{name}"'
                for name, kind in columns
            )
            _ = db.execute(
                f"insert into raw.{table} select {values} from raw.{table} "
                "where order_id = 'money-1'"
            )
        _ = db.execute(
            "update raw.orders set customer_id = 'customer-mix' "
            "where order_id in ('money-1', 'money-2')"
        )
        _ = db.execute(
            "update raw.orders set processed_at = '2026-07-02 19:00:00+00', "
            "created_at = '2026-07-02 19:00:00+00' where order_id = 'money-2'"
        )
        _ = db.execute("checkpoint")


def _dbt(command: str, env: dict[str, str], target: Path, *args: str) -> int:
    return subprocess.run(
        [
            "dbt",
            command,
            "--project-dir",
            str(ROOT / "dbt"),
            "--profiles-dir",
            str(ROOT / "dbt"),
            "--target-path",
            str(target),
            "--no-partial-parse",
            *args,
        ],
        env=env,
        check=False,
        timeout=240,
    ).returncode


def yaml_test_results(target: Path) -> dict[tuple[str, str, str | None], str]:
    """Status of each executed data test on the mix models, by YAML identity."""
    nodes = mapping(mapping(read_json(target / "manifest.json")).get("nodes"))
    found: dict[tuple[str, str, str | None], str] = {}
    for result in sequence(mapping(read_json(target / "run_results.json"))["results"]):
        # Ancestors' dbt unit tests also run; they are not manifest nodes.
        node = mapping(nodes.get(text(mapping(result)["unique_id"]), {}))
        attached = node.get("attached_node")
        if node.get("resource_type") != "test" or not isinstance(attached, str):
            continue
        model = attached.rpartition(".")[2]
        if model not in (MIX, COVERAGE):
            continue
        column = node.get("column_name")
        identity = (
            model,
            text(mapping(node["test_metadata"])["name"]),
            None if column is None else text(column),
        )
        assert identity not in found
        found[identity] = text(mapping(result)["status"])
    return found


@pytest.mark.dbt(*SELECTORS)
def test_yaml_tests_run_in_dbt_and_fail_on_bad_rows(tmp_path: Path) -> None:
    """dbt builds both models, runs every YAML test, and each catches its defect."""
    database = tmp_path / "warehouse.duckdb"
    write_mix_raw(database)
    env = dict(os.environ) | {
        "ANALYTICS_ENVIRONMENT": "ci",
        "DBT_TARGET": "local",
        "DBT_NAMESPACE": "customer_mix",
        "DUCKDB_PATH": str(database),
        "SHOPIFY_ORDERS_DATASET": "raw",
        "SHOPIFY_CUSTOMERS_ENABLED": "false",
        "GOODDAY_INVENTORY_ENABLED": "false",
        "LOOP_RETURNS_ENABLED": "false",
    }
    target = tmp_path / "build"
    assert not _dbt(
        "build", env, target, "--select", *SELECTORS, "--indirect-selection", "cautious"
    )
    assert yaml_test_results(target) == dict.fromkeys(YAML_TESTS, "pass")
    manifest = read_json(target / "manifest.json")
    check_column_declarations(manifest)
    nodes = mapping(mapping(manifest)["nodes"])
    outputs = [
        {
            "model_id": f"model.analytics.{model}",
            "sql": "select * from "
            + text(mapping(nodes[f"model.analytics.{model}"])["relation_name"]),
            "columns": mapping(mapping(nodes[f"model.analytics.{model}"])["columns"]),
        }
        for model in (MIX, COVERAGE)
    ]
    # Built columns and types match the YAML declarations.
    assert not _dbt(
        "run-operation",
        env,
        target,
        "validate_model_outputs",
        "--args",
        json.dumps({"outputs": outputs}),
        "--quiet",
    )

    schema = "customer_mix_intermediate"
    with duckdb.connect(str(database)) as db:
        rows = db.execute(f"select * from {schema}.{MIX} order by order_date")
        assert rows.fetchall() == [
            ("shop-1", "customer-mix", BEFORE, 1, None, "unknown", UNPROVEN),
            ("shop-1", "customer-mix", DAY, 1, BEFORE, "repeat", ""),
        ]
        assert db.execute(
            "select unidentified_qualifying_order_count, d2c_unknown_order_count, "
            f"eligibility_unknown_customer_count from {schema}.{COVERAGE} "
            "where order_date = ?",
            [BEFORE],
        ).fetchall() == [(1, 1, 1)]
        # Swap each view for a table holding a defect for every YAML test:
        # a duplicate key, nulls, and a verdict outside the accepted values.
        for model, defects in (
            (
                MIX,
                "('shop-1', 'customer-mix', date '2026-07-01', 1, null, "
                "'unknown', ''), (null, null, null, 1, null, null, null), "
                "('shop-1', 'customer-x', date '2026-07-03', 1, null, 'new', '')",
            ),
            (COVERAGE, "('shop-1', date '2026-07-01', 0, 0, 0), (null, null, 0, 0, 0)"),
        ):
            _ = db.execute(
                f"create table {schema}.bad as select * from {schema}.{model}"
            )
            _ = db.execute(f"insert into {schema}.bad values {defects}")
            _ = db.execute(f"drop view {schema}.{model}")
            _ = db.execute(f"alter table {schema}.bad rename to {model}")
    defects = tmp_path / "defects"
    assert _dbt("test", env, defects, "--select", MIX, COVERAGE)
    assert yaml_test_results(defects) == dict.fromkeys(YAML_TESTS, "fail")
