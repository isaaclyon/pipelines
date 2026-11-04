"""Optional Loop headers must carry evidence from the same completed load."""

import os
import subprocess
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

# Real dbt project nodes these tests execute (ADR 0026).
pytestmark = pytest.mark.dbt("+stg_loop_returns__headers")

ROOT = Path(__file__).parents[1]


def test_headers_require_matching_completed_load(tmp_path: Path) -> None:
    raw = tmp_path / "loop.duckdb"
    with duckdb.connect(str(raw)) as db:
        _ = db.execute("create schema raw_loop")
        _ = db.execute("""
            create table raw_loop.returns as
            select 'shop' as shop_id, cast(n as varchar) as return_id,
                '123' as shopify_order_id, 'closed' as return_state,
                timestamptz '2026-07-02 10:00:00+00' as created_at,
                timestamptz '2026-07-02 11:00:00+00' as updated_at,
                'USD' as currency,
                cast(189.48 as decimal(38,9)) as return_total,
                cast(174.48 as decimal(38,9)) as refund,
                cast(15 as decimal(38,9)) as handling_fee,
                cast(0 as decimal(38,9)) as gift_card,
                cast(null as decimal(38,9)) as exchange_total,
                cast(0 as decimal(38,9)) as upsell,
                cast(n as varchar) as _dlt_load_id
            from range(1, 5) as fixture(n);
            create table raw_loop.return_snapshots as
            select 'shop' as shop_id, cast(n as varchar) as snapshot_id,
                timestamptz '2026-07-03 00:00:00+00' as snapshot_end,
                cast(n as varchar) as _dlt_load_id
            from range(1, 4) as fixture(n);
            create table raw_loop._dlt_loads as
            select cast(n as varchar) as load_id,
                case when n=2 then 1 else 0 end as status
            from range(1, 3) as fixture(n);
        """)
    for enabled in (False, True):
        result = subprocess.run(
            [
                "dbt",
                "build",
                "--quiet",
                "--select",
                "stg_loop_returns__headers",
                "--indirect-selection",
                "cautious",
                "--project-dir",
                str(ROOT / "dbt"),
                "--profiles-dir",
                str(ROOT / "dbt"),
                "--target-path",
                str(tmp_path / "target"),
                "--log-path",
                str(tmp_path / "logs"),
            ],
            env=os.environ
            | {
                "DBT_TARGET": "local",
                "DBT_NAMESPACE": "loop",
                "DUCKDB_PATH": str(raw),
                "LOOP_RETURNS_DATASET": "raw_loop",
                "LOOP_RETURNS_ENABLED": str(enabled).lower(),
            },
            capture_output=True,
            text=True,
            timeout=90,
            check=False,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        with duckdb.connect(str(raw)) as db:
            rows = db.execute(
                "select return_id, handling_fee_amount, exchange_total_amount, "
                "is_snapshot_complete from loop_staging.stg_loop_returns__headers "
                "order by return_id"
            ).fetchall()
            order_ids = db.execute(
                "select distinct order_id from loop_staging.stg_loop_returns__headers"
            ).fetchall()
        assert order_ids == ([("gid://shopify/Order/123",)] if enabled else [])
        assert rows == (
            [
                ("1", Decimal("15"), None, True),
                ("2", Decimal("15"), None, False),
                ("3", Decimal("15"), None, False),
                ("4", Decimal("15"), None, False),
            ]
            if enabled
            else []
        )
