from datetime import timedelta
from pathlib import Path

import dagster as dg
import duckdb
import pytest

from analytics_transformations.defs.shopify_validation import (
    RAW_ORDERS,
    defs,
    refresh_freshness,
)

# Real dbt project nodes these tests execute (ADR 0026).
pytestmark = pytest.mark.dbt("source:shopify_orders.ingestion_batches")

DBT = Path(__file__).parents[1] / "dbt"


def warehouse(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *rows: tuple[timedelta, str, str]
) -> None:
    """Raw batches ``(age, kind, completeness)`` in a DuckDB dbt target.

    Ages are relative to the database clock when dbt queries the view, so slow
    test runs cannot push a batch across a freshness threshold.
    """
    path = tmp_path / "warehouse.duckdb"
    batches = " union all ".join(
        "select current_timestamp - to_microseconds("
        f"{int(age / timedelta(microseconds=1))}) as window_end, "
        f"'{kind}' as kind, 'complete' as status, '{completeness}' as completeness"
        for age, kind, completeness in rows
    ) or (
        "select null::timestamptz as window_end, null::varchar as kind, "
        "null::varchar as status, null::varchar as completeness where false"
    )
    with duckdb.connect(str(path)) as db:
        db.execute("create schema raw")
        db.execute(f"create view raw.ingestion_batches as {batches}")
    for name, value in {
        "DBT_TARGET": "local",
        "DUCKDB_PATH": str(path),
        "DBT_PROJECT_DIR": str(DBT),
        "DBT_PROFILES_DIR": str(DBT),
        "SHOPIFY_ORDERS_DATASET": "raw",
    }.items():
        monkeypatch.setenv(name, value)


def evaluate() -> dg.AssetCheckResult:
    # Dagster's optional settings argument is untyped.
    with dg.DagsterInstance.ephemeral() as instance:  # pyright: ignore[reportUnknownMemberType]
        result = defs.resolve_job_def("shopify_raw_validation").execute_in_process(
            instance=instance
        )
    assert result.success
    [check] = result.get_asset_check_evaluations()
    assert (check.asset_key, check.check_name) == (RAW_ORDERS, "refresh_freshness")
    return dg.AssetCheckResult(
        passed=check.passed,
        severity=check.severity,
        metadata=check.metadata,
    )


def test_recent_complete_refresh_passes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    warehouse(tmp_path, monkeypatch, (timedelta(minutes=5), "refresh", "complete"))
    check = evaluate()
    assert check.passed
    assert check.metadata["status"].value == "pass"


@pytest.mark.parametrize(
    ("age", "severity"),
    [(26, dg.AssetCheckSeverity.WARN), (31, dg.AssetCheckSeverity.ERROR)],
)
def test_stale_refresh_warns_then_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    age: int,
    severity: dg.AssetCheckSeverity,
) -> None:
    warehouse(tmp_path, monkeypatch, (timedelta(minutes=age), "refresh", "complete"))
    check = evaluate()
    assert (check.passed, check.severity) == (False, severity)


def test_only_complete_refreshes_count(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    warehouse(
        tmp_path,
        monkeypatch,
        (timedelta(hours=2), "refresh", "complete"),
        (timedelta(minutes=1), "lookback", "complete"),
        (timedelta(minutes=1), "refresh", "incomplete"),
    )
    check = evaluate()
    assert (check.passed, check.severity) == (False, dg.AssetCheckSeverity.ERROR)


def test_unreadable_source_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    warehouse(tmp_path, monkeypatch)
    monkeypatch.setenv("SHOPIFY_ORDERS_DATASET", "missing")
    check = evaluate()
    assert (check.passed, check.severity) == (False, dg.AssetCheckSeverity.ERROR)


def test_schedule_checks_freshness_all_day() -> None:
    schedule = defs.resolve_schedule_def("shopify_raw_validation_schedule")
    assert schedule.cron_schedule == "*/5 * * * *"
    assert schedule.execution_timezone == "UTC"
    assert refresh_freshness.check_specs
