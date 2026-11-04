"""Coverage joins overlapping windows without concealing gaps or unknown evidence."""

import os
import subprocess
from datetime import datetime
from pathlib import Path

import duckdb
import pytest
import snowflake.connector

# Real dbt project nodes these tests execute (ADR 0026).
pytestmark = pytest.mark.dbt("+int_shopify_orders__window_coverage")

MODEL = (
    Path(__file__).parents[1]
    / "dbt/models/intermediate/shopify_orders/int_shopify_orders__window_coverage.sql"
)


def coverage(rows: list[tuple[str, int, int, bool]]) -> list[tuple[object, ...]]:
    """Hours on a fixed UTC day; valid empty extractions have normal windows."""
    with duckdb.connect() as db:
        _ = db.execute(
            "create table batches (shop_id varchar, window_started_at timestamp, "
            "window_ended_at timestamp, is_coverage_verified boolean)"
        )
        for shop, start, end, verified in rows:
            _ = db.execute(
                "insert into batches values (?, ?, ?, ?)",
                [
                    shop,
                    datetime(2026, 10, 1, start),
                    datetime(2026, 10, 1, end),
                    verified,
                ],
            )
        sql = MODEL.read_text().replace(
            "{{ ref('stg_shopify_orders__ingestion_batches') }}", "batches"
        )
        return db.execute(sql + " order by shop_id, coverage_started_at").fetchall()


@pytest.mark.skipif(
    os.getenv("ORDER_EVIDENCE_LIVE_ACCEPTANCE") != "development",
    reason="explicit read-only development Snowflake acceptance only",
)
@pytest.mark.parametrize(
    "rows",
    [
        [],
        [("s1", 1, 2, False)],
        [("s1", 1, 8, True), ("s1", 2, 3, True), ("s1", 7, 9, True)],
        [("s1", 1, 2, True), ("s1", 3, 4, True)],
        [("s1", 1, 2, True), ("s1", 3, 4, True), ("s1", 2, 3, True)],
        [("s1", 1, 2, True), ("s1", 1, 2, True), ("s2", 2, 3, True)],
        [("s1", 2, 2, True), ("s1", 3, 2, True)],
    ],
)
def test_snowflake_window_coverage_matches_duckdb(
    rows: list[tuple[str, int, int, bool]],
) -> None:
    parameters: list[object] = []
    selects: list[str] = []
    for shop, start, end, verified in rows:
        selects.append(
            "select %s::varchar as shop_id, %s::timestamp_ntz as window_started_at, "
            "%s::timestamp_ntz as window_ended_at, %s::boolean as is_coverage_verified"
        )
        parameters.extend(
            [
                shop,
                datetime(2026, 10, 1, start).isoformat(),
                datetime(2026, 10, 1, end).isoformat(),
                verified,
            ]
        )
    if not selects:
        selects.append(
            "select null::varchar as shop_id, "
            "null::timestamp_ntz as window_started_at, "
            "null::timestamp_ntz as window_ended_at, "
            "null::boolean as is_coverage_verified where false"
        )
    sql = MODEL.read_text().replace(
        "{{ ref('stg_shopify_orders__ingestion_batches') }}",
        "(" + " union all ".join(selects) + ")",
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
        _ = cursor.execute(
            sql + " order by shop_id, coverage_started_at", tuple(parameters)
        )
        assert cursor.fetchall() == coverage(rows)


def test_missing_evidence_is_not_coverage() -> None:
    assert coverage([]) == []
    assert coverage([("s1", 1, 2, False)]) == []


def test_adjacent_nested_and_overlapping_windows_form_one_interval() -> None:
    # LAG(end) is insufficient: the nested 2–3 window must not hide the 1–8 end.
    assert coverage(
        [
            ("s1", 1, 8, True),
            ("s1", 2, 3, True),
            ("s1", 7, 9, True),
            ("s1", 9, 10, True),
        ]
    ) == [("s1", datetime(2026, 10, 1, 1), datetime(2026, 10, 1, 10))]


def test_recent_load_does_not_erase_gap() -> None:
    assert coverage([("s1", 1, 2, True), ("s1", 3, 4, True)]) == [
        ("s1", datetime(2026, 10, 1, 1), datetime(2026, 10, 1, 2)),
        ("s1", datetime(2026, 10, 1, 3), datetime(2026, 10, 1, 4)),
    ]


def test_failed_window_cannot_bridge_gap_but_successful_retry_can() -> None:
    rows = [("s1", 1, 2, True), ("s1", 3, 4, True), ("s1", 2, 3, False)]
    assert len(coverage(rows)) == 2
    assert coverage([*rows, ("s1", 2, 3, True)]) == [
        ("s1", datetime(2026, 10, 1, 1), datetime(2026, 10, 1, 4))
    ]


def test_duplicate_windows_do_not_duplicate_coverage_or_cross_shops() -> None:
    assert coverage([("s1", 1, 2, True), ("s1", 1, 2, True), ("s2", 2, 3, True)]) == [
        ("s1", datetime(2026, 10, 1, 1), datetime(2026, 10, 1, 2)),
        ("s2", datetime(2026, 10, 1, 2), datetime(2026, 10, 1, 3)),
    ]


@pytest.mark.parametrize("start,end", [(3, 2), (2, 2)])
def test_invalid_interval_is_not_coverage(start: int, end: int) -> None:
    assert coverage([("s1", start, end, True)]) == []


def test_raw_evidence_requires_complete_lineage_and_accepts_empty_loads(
    tmp_path: Path,
) -> None:
    raw = tmp_path / "warehouse.duckdb"
    with duckdb.connect(str(raw)) as db:
        _ = db.execute("create schema raw")
        _ = db.execute(
            "create table raw.ingestion_batches as select 's1' as shop_id, "
            "'good' as batch_id, 'refresh' as kind, 'updated_at' as window_field, "
            "timestamptz '2026-10-01 00:00:00+00' as window_start, "
            "timestamptz '2026-10-01 01:00:00+00' as window_end, "
            "timestamptz '2026-10-01 01:00:00+00' as observed_started_at, "
            "timestamptz '2026-10-01 01:01:00+00' as observed_finished_at, "
            "timestamptz '2026-10-01 01:02:00+00' as completed_at, "
            "'complete' as status, 'complete' as completeness, "
            "0::bigint as exceptions, 0::bigint as orders_observed, "
            "3::bigint as extraction_revision, 'fixture-query' as query_version"
        )
        for batch, change in (
            ("reject", "exceptions = 1"),
            ("unknown-rejects", "exceptions = null"),
            ("incomplete", "completeness = 'incomplete'"),
            ("failed", "status = 'failed'"),
            ("created", "window_field = 'created_at'"),
            ("old-parser", "extraction_revision = 2"),
            ("no-lineage", "query_version = ''"),
            ("unknown-count", "orders_observed = null"),
            ("negative-count", "orders_observed = -1"),
            ("future-window", "window_end = completed_at"),
            ("bad-timing", "observed_finished_at = window_start"),
        ):
            _ = db.execute(
                "insert into raw.ingestion_batches "
                "select * replace (? as batch_id) "
                "from raw.ingestion_batches where batch_id = 'good'",
                [batch],
            )
            _ = db.execute(
                f"update raw.ingestion_batches set {change} where batch_id = ?",
                [batch],
            )
    build_coverage(raw)
    with duckdb.connect(str(raw), read_only=True) as db:
        verdicts = dict(
            db.execute(
                "select batch_id, is_coverage_verified "
                "from ci_coverage_staging.stg_shopify_orders__ingestion_batches"
            ).fetchall()
        )
        assert verdicts.pop("good") is True
        assert set(verdicts.values()) == {False}
        assert db.execute(
            "select * from ci_coverage_intermediate.int_shopify_orders__window_coverage"
        ).fetchall() == [("s1", datetime(2026, 10, 1), datetime(2026, 10, 1, 1))]


def build_coverage(raw: Path) -> None:
    project = Path(__file__).parents[1] / "dbt"
    result = subprocess.run(
        [
            "dbt",
            "build",
            "--select",
            "+int_shopify_orders__window_coverage",
            "--indirect-selection",
            "cautious",
            "--project-dir",
            str(project),
            "--profiles-dir",
            str(project),
            "--target-path",
            str(raw.parent / "target"),
            "--quiet",
            "--warn-error-options",
            '{"error":["NoNodesForSelectionCriteria"]}',
        ],
        env={
            **os.environ,
            "DBT_TARGET": "local",
            "DBT_NAMESPACE": "ci_coverage",
            "DUCKDB_PATH": str(raw),
            "SHOPIFY_ORDERS_DATASET": "raw",
        },
        timeout=90,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_missing_evidence_table_builds_without_claiming_coverage(
    tmp_path: Path,
) -> None:
    raw = tmp_path / "warehouse.duckdb"
    with duckdb.connect(str(raw)) as db:
        _ = db.execute("create schema raw")
    build_coverage(raw)
    with duckdb.connect(str(raw), read_only=True) as db:
        assert (
            db.execute(
                "select * from ci_coverage_intermediate."
                "int_shopify_orders__window_coverage"
            ).fetchall()
            == []
        )
