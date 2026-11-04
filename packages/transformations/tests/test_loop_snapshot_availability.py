"""Snapshot evidence must describe the headers actually used, including empty loads."""

import os
import subprocess
from pathlib import Path

import duckdb
import pytest

# Real dbt project nodes these tests execute (ADR 0026).
pytestmark = pytest.mark.dbt("+stg_loop_returns__snapshots")

ROOT = Path(__file__).parents[1]


def test_loop_availability_matches_committed_current_rows(tmp_path: Path) -> None:
    raw = tmp_path / "warehouse.duckdb"
    with duckdb.connect(str(raw)) as db:
        _ = db.execute("create schema raw_loop")
        _ = db.execute("""
            create table raw_loop.return_snapshots as
            select shop_id, shop_id as snapshot_id, shop_id as _dlt_load_id,
                expected::bigint as row_count,
                timestamptz '2026-10-01 00:00:00+00' as snapshot_end,
                timestamptz '2026-10-01 00:01:00+00' as completed_at
            from (values ('valid', 2), ('wrong_load', 1), ('missing_load', 1),
                ('failed', 1), ('mismatch', 2), ('empty', 0), ('bad_time', 0))
                as t(shop_id, expected);
            update raw_loop.return_snapshots set completed_at =
                timestamptz '2026-09-30 23:00:00+00' where shop_id = 'bad_time';
            create table raw_loop.returns as
            select * from (values ('valid', 'valid'), ('valid', 'valid'),
                ('wrong_load', 'newer'), ('missing_load', 'missing_load'),
                ('failed', 'failed'), ('mismatch', 'mismatch'))
                as t(shop_id, _dlt_load_id);
            create table raw_loop._dlt_loads as
            select _dlt_load_id as load_id,
                case when shop_id = 'failed' then 1 else 0 end as status
            from raw_loop.return_snapshots where shop_id <> 'missing_load';
        """)
    for enabled in (False, True):
        result = subprocess.run(
            [
                "dbt",
                "build",
                "--quiet",
                "--select",
                "stg_loop_returns__snapshots",
                "--indirect-selection",
                "cautious",
                "--project-dir",
                str(ROOT / "dbt"),
                "--profiles-dir",
                str(ROOT / "dbt"),
                "--target-path",
                str(tmp_path / "target"),
            ],
            env={
                **os.environ,
                "DBT_TARGET": "local",
                "DBT_NAMESPACE": "ci_loop",
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
        with duckdb.connect(str(raw), read_only=True) as db:
            result_rows = db.execute(
                "select shop_id, is_snapshot_complete "
                "from ci_loop_staging.stg_loop_returns__snapshots"
            ).fetchall()
            assert dict(result_rows) == (
                {
                    "valid": True,
                    "wrong_load": False,
                    "missing_load": False,
                    "failed": False,
                    "mismatch": False,
                    "empty": True,
                    "bad_time": False,
                }
                if enabled
                else {}
            )
