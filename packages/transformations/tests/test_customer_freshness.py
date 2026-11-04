"""Customer coverage and full sweeps have independent freshness policies."""

from datetime import timedelta
from pathlib import Path

import duckdb
import pytest

from analytics_transformations.defs.shopify_validation import source_freshness

# Real dbt project nodes these tests execute (ADR 0026).
pytestmark = pytest.mark.dbt(
    "source:shopify_customers.profile_evidence",
    "source:shopify_customers.sweep_evidence",
)

DBT = Path(__file__).parents[1] / "dbt"


def evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rows: list[tuple[int, str, int]],
) -> None:
    path = tmp_path / "warehouse.duckdb"
    queries = [
        "select current_timestamp - to_microseconds("
        f"{int(timedelta(minutes=age) / timedelta(microseconds=1))}) as window_end, "
        f"'{kind}' as kind, {rejects} as rejects, "
        "'complete' as status, 'complete' as completeness, "
        f"{'true' if kind == 'sweep' else 'false'} as is_full_sweep"
        for age, kind, rejects in rows
    ]
    with duckdb.connect(str(path)) as db:
        db.execute("create schema raw_customers")
        db.execute(
            "create view raw_customers.customer_batch_evidence as "
            + " union all ".join(queries)
        )
    for name, value in {
        "DBT_TARGET": "local",
        "DUCKDB_PATH": str(path),
        "DBT_PROJECT_DIR": str(DBT),
        "DBT_PROFILES_DIR": str(DBT),
        "SHOPIFY_CUSTOMERS_DATASET": "raw_customers",
        "SHOPIFY_CUSTOMERS_ENABLED": "true",
    }.items():
        monkeypatch.setenv(name, value)


@pytest.mark.parametrize(
    ("rows", "source", "expected"),
    [
        ([(5, "refresh", 0)], "profile_evidence", "pass"),
        ([(26, "refresh", 0)], "profile_evidence", "warn"),
        ([(31, "refresh", 0), (1, "lookback", 0)], "profile_evidence", "error"),
        ([(31, "refresh", 0), (1, "refresh", 1)], "profile_evidence", "error"),
        ([(10, "refresh", 0), (60, "sweep", 0)], "sweep_evidence", "pass"),
        ([(1, "refresh", 0), (31 * 60, "sweep", 0)], "sweep_evidence", "error"),
    ],
)
def test_customer_freshness_uses_complete_coverage_not_load_time(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rows: list[tuple[int, str, int]],
    source: str,
    expected: str,
) -> None:
    evidence(tmp_path, monkeypatch, rows)
    status, _ = source_freshness(f"source:shopify_customers.{source}")
    assert status == expected
