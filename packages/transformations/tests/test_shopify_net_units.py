"""Net units use current quantities once and explicit cancellation evidence."""

import os
import subprocess
from pathlib import Path

import duckdb
import pytest

from tests.shopify_order_fixtures import GOOD_LINES, GOOD_ORDERS, ROOT, write_raw

# Real dbt project nodes these tests execute (ADR 0026).
pytestmark = pytest.mark.dbt("+int_shopify_order_lines__net_units")


def build(raw: Path, target: Path, selection: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "dbt",
            "build",
            "--select",
            *selection.split(),
            "--quiet",
            "--indirect-selection",
            "cautious",
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
            "DBT_NAMESPACE": "units",
            "DUCKDB_PATH": str(raw),
            "SHOPIFY_ORDERS_DATASET": "raw",
        },
        timeout=120,
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.mark.parametrize("new_columns", [False, True])
def test_raw_line_evidence_is_additive(tmp_path: Path, new_columns: bool) -> None:
    raw = tmp_path / "units.duckdb"
    write_raw(raw, GOOD_ORDERS, GOOD_LINES)
    if new_columns:
        with duckdb.connect(str(raw)) as db:
            _ = db.execute(
                "alter table raw.orders__lines add column "
                "unfulfilled_quantity decimal(19,0)"
            )
            _ = db.execute(
                "alter table raw.orders__lines add column "
                "non_fulfillable_quantity decimal(19,0)"
            )
            _ = db.execute(
                "alter table raw.orders__lines add column requires_shipping boolean"
            )
            _ = db.execute(
                "update raw.orders__lines set unfulfilled_quantity = 1, "
                "non_fulfillable_quantity = 0, requires_shipping = true"
            )
    result = build(
        raw,
        tmp_path / "target",
        "stg_shopify_orders__orders stg_shopify_orders__order_lines",
    )
    assert result.returncode == 0, result.stdout + result.stderr
    with duckdb.connect(str(raw), read_only=True) as db:
        actual = db.execute(
            "select unfulfilled_quantity, non_fulfillable_quantity, "
            "is_shipping_required from units_staging.stg_shopify_orders__order_lines "
            "order by line_item_id"
        ).fetchall()
    assert actual == ([(1, 0, True)] if new_columns else [(None, None, None)]) * 3


def test_net_units_fixture_cases(tmp_path: Path) -> None:
    raw = tmp_path / "units.duckdb"
    write_raw(raw, GOOD_ORDERS, GOOD_LINES)
    result = build(raw, tmp_path / "target", "+int_shopify_order_lines__net_units")
    assert result.returncode == 0, result.stdout + result.stderr
