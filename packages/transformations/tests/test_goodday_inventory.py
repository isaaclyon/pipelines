"""Current inventory uses complete capture evidence, never per-item latest rows."""

import os
import subprocess
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

# Real dbt project nodes these tests execute (ADR 0026).
pytestmark = pytest.mark.dbt("+fct_inventory__current")

ROOT = Path(__file__).parents[1]
QUANTITIES = (
    "on_hand_total",
    "on_hand_sellable",
    "on_hand_unsellable",
    "on_hand_reserved",
    "on_hand_committed",
    "on_hand_available",
    "available",
    "ats",
    "inbound_total",
    "inbound_available",
    "fulfilled_total",
    "transfer_in_transit",
    "return_total",
)


def write_inventory(path: Path) -> None:
    with duckdb.connect(str(path)) as db:
        db.execute("create schema goodday_raw")
        db.execute(
            "create table goodday_raw._dlt_loads (load_id varchar, status bigint)"
        )
        db.execute(
            "insert into goodday_raw._dlt_loads values ('old',0),('new',0),('failed',1)"
        )
        db.execute("""create table goodday_raw.inventory_captures (
            capture_id varchar, _dlt_load_id varchar, captured_at timestamptz,
            observed_at timestamptz, source_relation varchar,
            row_count bigint)""")
        db.execute("""insert into goodday_raw.inventory_captures values
            ('old','old','2026-10-26 10:00:01+00','2026-10-26 10:00:00+00',
             'vendor.inventory',1),
            ('new','new','2026-10-26 11:00:01+00','2026-10-26 11:00:00+00',
             'vendor.inventory',1),
            ('failed','failed','2026-10-26 12:00:01+00','2026-10-26 12:00:00+00',
             'vendor.inventory',1)
        """)
        columns = ", ".join(f"{name} decimal(38,9)" for name in QUANTITIES)
        db.execute(f"""create table goodday_raw.inventory_by_item (
            _capture_id varchar, _dlt_load_id varchar, _row_number bigint,
            merchant_id bigint, item_id bigint, channel_id bigint, location_id bigint,
            parent_item_id bigint, merchant_name varchar, item_name varchar,
            item_status varchar, sku varchar, channel_name varchar,
            location_name varchar,
            updated_at timestamptz, {columns})""")
        db.execute("""insert into goodday_raw.inventory_by_item
            (_capture_id, _dlt_load_id, _row_number, merchant_id, item_id, channel_id,
             location_id, sku, updated_at, on_hand_total, ats, available)
            values ('old','old',1,58,1,1,1,'OLD','2026-10-25 00:00:00+00',100,100,100),
                   ('new','new',1,58,2,null,null,' AbC ','2026-10-25 00:00:00+00',
                    -1.123456789,0,null),
                   ('failed','failed',1,58,3,1,1,'FAILED','2026-10-25 00:00:00+00',
                    999,999,999)
        """)
        for index, name in enumerate(QUANTITIES):
            if name not in {"on_hand_total", "ats", "available"}:
                db.execute(
                    f"update goodday_raw.inventory_by_item set {name}=? "
                    "where _capture_id='new'",
                    [Decimal(index) + Decimal("0.000000001")],
                )


def build(path: Path, target: Path, *, succeeds: bool = True) -> None:
    result = subprocess.run(
        [
            "dbt",
            "build",
            "--project-dir",
            str(ROOT / "dbt"),
            "--profiles-dir",
            str(ROOT / "dbt"),
            "--target-path",
            str(target),
            "--no-partial-parse",
            "--quiet",
            "--select",
            "+fct_inventory__current",
            "--warn-error-options",
            '{"error":["NoNodesForSelectionCriteria"]}',
        ],
        env=dict(os.environ)
        | {
            "DBT_TARGET": "local",
            "DBT_NAMESPACE": "ci_inventory",
            "DUCKDB_PATH": str(path),
            "GOODDAY_INVENTORY_ENABLED": "true",
            "GOODDAY_DATASET": "goodday_raw",
        },
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert (result.returncode == 0) == succeeds, result.stdout + result.stderr

    if succeeds:
        from analytics_transformations.column_declarations import (
            check_column_declarations,
        )
        from analytics_transformations.mart_contracts import (
            contracts,
            read_json,
            snapshot,
        )
        from analytics_transformations.model_declarations import (
            check_documentation,
            declarations,
        )

        manifest = read_json(target / "manifest.json")
        _ = declarations(manifest)
        check_documentation(manifest)
        check_column_declarations(manifest)
        assert (
            snapshot(contracts(manifest))
            == (ROOT / "dbt/goodday-mart-contracts.json").read_text()
        )


def test_latest_successful_capture_preserves_source_values(tmp_path: Path) -> None:
    path = tmp_path / "inventory.duckdb"
    write_inventory(path)
    build(path, tmp_path / "target")
    syntax = subprocess.run(
        [
            "sqlfluff",
            "parse",
            "--dialect",
            "snowflake",
            "--templater",
            "raw",
            str(tmp_path / "target/compiled/analytics"),
        ],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert syntax.returncode == 0, syntax.stdout + syntax.stderr
    with duckdb.connect(str(path)) as db:
        assert db.execute("""select goodday_item_id, sku, goodday_channel_id,
            goodday_location_id, on_hand_total_quantity, ats_quantity,
            available_quantity
            from ci_inventory_marts.fct_inventory__current""").fetchall() == [
            (2, " AbC ", None, None, Decimal("-1.123456789"), Decimal(0), None)
        ]
        quantity_columns = ", ".join(f"{name}_quantity" for name in QUANTITIES)
        assert db.execute(
            f"select {quantity_columns} from ci_inventory_marts.fct_inventory__current"
        ).fetchone() == (
            Decimal("-1.123456789"),
            Decimal("1.000000001"),
            Decimal("2.000000001"),
            Decimal("3.000000001"),
            Decimal("4.000000001"),
            Decimal("5.000000001"),
            None,
            Decimal(0),
            Decimal("8.000000001"),
            Decimal("9.000000001"),
            Decimal("10.000000001"),
            Decimal("11.000000001"),
            Decimal("12.000000001"),
        )


def test_empty_capture_supersedes_nonempty(tmp_path: Path) -> None:
    path = tmp_path / "inventory.duckdb"
    write_inventory(path)
    with duckdb.connect(str(path)) as db:
        db.execute("delete from goodday_raw.inventory_by_item where _capture_id='new'")
        db.execute(
            "update goodday_raw.inventory_captures set row_count=0 "
            "where capture_id='new'"
        )
    build(path, tmp_path / "target")
    with duckdb.connect(str(path)) as db:
        assert db.execute(
            "select count(*) from ci_inventory_marts.fct_inventory__current"
        ).fetchone() == (0,)


@pytest.mark.parametrize(
    "mutation",
    [
        "delete from goodday_raw._dlt_loads",
        "update goodday_raw.inventory_by_item set _dlt_load_id='wrong' "
        "where _capture_id='new'",
        "insert into goodday_raw.inventory_by_item select * "
        "from goodday_raw.inventory_by_item where _capture_id='new'",
    ],
)
def test_invalid_evidence_blocks_build(tmp_path: Path, mutation: str) -> None:
    path = tmp_path / "inventory.duckdb"
    write_inventory(path)
    with duckdb.connect(str(path)) as db:
        db.execute(mutation)
    build(path, tmp_path / "target", succeeds=False)


def test_rebuild_keeps_observation_time_and_rejects_bad_evidence(
    tmp_path: Path,
) -> None:
    path = tmp_path / "inventory.duckdb"
    write_inventory(path)
    for attempt in range(2):
        build(path, tmp_path / f"target-{attempt}")
        with duckdb.connect(str(path), read_only=True) as db:
            assert db.execute("""select cast(observed_at as varchar),
                cast(source_updated_at as varchar)
                from ci_inventory_marts.fct_inventory__current""").fetchone() == (
                "2026-10-26 11:00:00",
                "2026-10-25 00:00:00",
            )
    with duckdb.connect(str(path)) as db:
        db.execute(
            "update goodday_raw.inventory_captures set row_count=2 "
            "where capture_id='new'"
        )
    build(path, tmp_path / "target-bad", succeeds=False)


@pytest.mark.parametrize("duplicate", [False, True])
def test_business_grain_includes_merchant_and_nullable_dimensions(
    tmp_path: Path,
    duplicate: bool,
) -> None:
    path = tmp_path / "inventory.duckdb"
    write_inventory(path)
    with duckdb.connect(str(path)) as db:
        db.execute("""insert into goodday_raw.inventory_by_item
            select * replace (2 as _row_number, 59 as merchant_id)
            from goodday_raw.inventory_by_item where _capture_id='new'""")
        db.execute("""insert into goodday_raw.inventory_by_item
            select * replace (3 as _row_number, 1 as channel_id, 1 as location_id)
            from goodday_raw.inventory_by_item
            where _capture_id='new' and _row_number=1""")
        db.execute("""update goodday_raw.inventory_captures
            set row_count=3 where capture_id='new'""")
        if duplicate:
            db.execute("""update goodday_raw.inventory_by_item set merchant_id=58
                where _capture_id='new' and _row_number=2""")
    build(path, tmp_path / "target", succeeds=not duplicate)
    if not duplicate:
        with duckdb.connect(str(path)) as db:
            assert db.execute("""select goodday_merchant_id, goodday_item_id,
                goodday_channel_id, goodday_location_id
                from ci_inventory_marts.fct_inventory__current
                order by inventory_row_number""").fetchall() == [
                (58, 2, None, None),
                (59, 2, None, None),
                (58, 2, 1, 1),
            ]
