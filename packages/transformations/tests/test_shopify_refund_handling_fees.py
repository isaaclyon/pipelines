"""Resolve Loop handling fees in Shopify refunds through the dbt unit tests.

The unit tests in ``_shopify_refunds__intermediate.yml`` mock every staged input.
dbt still reads each input's column types from a relation, so this module creates
those relations empty. It does not load raw Shopify or Loop data.
"""

import os
import subprocess
from pathlib import Path

import duckdb
import pytest

from analytics_transformations.mart_contracts import mapping, read_json, sequence, text

# Real dbt project nodes these tests execute (ADR 0026).
pytestmark = pytest.mark.dbt("+int_shopify_refunds__handling_fees")

ROOT = Path(__file__).parents[1]
NAMESPACE = "refunds_units"
MODEL = "int_shopify_refunds__handling_fees"
MONEY = "decimal(38,9)"
TEXT = "varchar"

# Only the columns that the model or its unit-test fixtures read.
STAGED_INPUTS: dict[str, dict[str, str]] = {
    "stg_shopify_orders__refunds": {
        "shop_id": TEXT,
        "order_id": TEXT,
        "refund_id": TEXT,
        "total_refunded_shop_amount": MONEY,
        "shop_currency_code": TEXT,
        "presentment_currency_code": TEXT,
    },
    "stg_shopify_orders__refund_lines": {
        "shop_id": TEXT,
        "order_id": TEXT,
        "refund_id": TEXT,
        "refund_line_item_id": TEXT,
        "subtotal_shop_amount": MONEY,
        "subtotal_shop_currency_code": TEXT,
        "subtotal_presentment_currency_code": TEXT,
        "total_tax_shop_amount": MONEY,
        "total_tax_shop_currency_code": TEXT,
        "total_tax_presentment_currency_code": TEXT,
    },
    "stg_shopify_orders__refund_adjustments": {
        "shop_id": TEXT,
        "order_id": TEXT,
        "refund_id": TEXT,
        "adjustment_id": TEXT,
        "adjustment_shop_amount": MONEY,
        "adjustment_shop_currency_code": TEXT,
        "adjustment_presentment_currency_code": TEXT,
        "adjustment_tax_shop_amount": MONEY,
        "adjustment_tax_shop_currency_code": TEXT,
        "adjustment_tax_presentment_currency_code": TEXT,
    },
    "stg_shopify_orders__refund_shipping_lines": {
        "shop_id": TEXT,
        "order_id": TEXT,
        "refund_id": TEXT,
        "refund_shipping_line_id": TEXT,
        "shipping_line_id": TEXT,
        "subtotal_shop_amount": MONEY,
        "subtotal_shop_currency_code": TEXT,
        "subtotal_presentment_currency_code": TEXT,
        "tax_shop_amount": MONEY,
        "tax_shop_currency_code": TEXT,
        "tax_presentment_currency_code": TEXT,
    },
    "stg_shopify_orders__refund_duties": {
        "shop_id": TEXT,
        "order_id": TEXT,
        "refund_id": TEXT,
        "duty_position": "bigint",
        "duty_id": TEXT,
        "refund_shop_amount": MONEY,
    },
    "stg_loop_returns__headers": {
        "shop_id": TEXT,
        "return_id": TEXT,
        "order_id": TEXT,
        "return_state": TEXT,
        "currency_code": TEXT,
        "return_total_amount": MONEY,
        "refund_amount": MONEY,
        "handling_fee_amount": MONEY,
        "gift_card_amount": MONEY,
        "exchange_total_amount": MONEY,
        "upsell_amount": MONEY,
        "is_snapshot_complete": "boolean",
    },
}


@pytest.fixture(scope="module")
def build_result(
    tmp_path_factory: pytest.TempPathFactory,
) -> tuple[subprocess.CompletedProcess[str], Path]:
    base = tmp_path_factory.mktemp("refund_handling_fees")
    raw = base / "units.duckdb"
    target = base / "target"
    with duckdb.connect(str(raw)) as db:
        _ = db.execute(f"create schema {NAMESPACE}_staging")
        for table, columns in STAGED_INPUTS.items():
            ddl = ", ".join(f'"{name}" {kind}' for name, kind in columns.items())
            _ = db.execute(f"create table {NAMESPACE}_staging.{table} ({ddl})")
        _ = db.execute("checkpoint")
    result = subprocess.run(
        [
            "dbt",
            "build",
            "--select",
            MODEL,
            "--quiet",
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
            "DBT_NAMESPACE": NAMESPACE,
            "DUCKDB_PATH": str(raw),
        },
        timeout=300,
        capture_output=True,
        text=True,
        check=False,
    )
    return result, target


def test_handling_fee_unit_and_data_tests_pass(
    build_result: tuple[subprocess.CompletedProcess[str], Path],
) -> None:
    result, target = build_result
    assert result.returncode == 0, result.stdout + result.stderr
    run_results = mapping(read_json(target / "run_results.json"))
    unit_results = [
        mapping(item)
        for item in sequence(run_results["results"])
        if text(mapping(item)["unique_id"]).startswith(f"unit_test.analytics.{MODEL}.")
    ]
    assert len(unit_results) == 15
    assert {item["status"] for item in unit_results} == {"pass"}
