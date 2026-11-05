"""analytics-check-client checks a client's dbt project offline."""

import os
import shutil
from pathlib import Path

import duckdb
import pytest

from analytics_transformations.check_client import (
    CheckFailed,
    check_client,
    create_raw,
)

ROOT = Path(__file__).parents[1]
SAMPLE = ROOT.parents[1] / "examples" / "sample-client"


def _sources(**columns: dict[str, object]) -> dict[str, object]:
    return {
        "sources": {
            "source.analytics_toolkit.shop.orders": {
                "database": "raw_shop",
                "schema": "shop_raw",
                "identifier": "orders",
                "meta": {"raw_file": "shop/local/shop.duckdb"},
                "columns": {
                    "order_id": {"data_type": "text"},
                    "total_amount": {"data_type": "decimal(38, 9)"},
                    "processed_at": {"data_type": "timestamp"},
                },
            },
            "source.client.sheets.targets": {
                "database": "warehouse",
                "schema": "sheets",
                "identifier": "Targets",
                "meta": {},
                "columns": columns or {"target": {"data_type": "bigint"}},
            },
        }
    }


def _columns(path: Path, table: str) -> list[tuple[object, ...]]:
    with duckdb.connect(str(path), read_only=True) as db:
        assert db.execute(f"select count(*) from {table}").fetchall() == [(0,)]
        return db.execute(
            "select column_name, data_type from information_schema.columns "
            f"where table_schema || '.' || table_name = '{table}' "
            "order by ordinal_position"
        ).fetchall()


def test_empty_raw_relations_follow_source_declarations(tmp_path: Path) -> None:
    warehouse = tmp_path / "warehouse.duckdb"
    create_raw(_sources(), warehouse=warehouse, raw_root=tmp_path / "raw")
    # A source in another database lives in its own raw file, which
    # attach_raw_files() attaches; others live in the warehouse itself.
    assert _columns(tmp_path / "raw/shop/local/shop.duckdb", "shop_raw.orders") == [
        ("order_id", "VARCHAR"),
        ("total_amount", "DECIMAL(38,9)"),
        ("processed_at", "TIMESTAMP"),
    ]
    assert _columns(warehouse, "sheets.Targets") == [("target", "BIGINT")]


def test_untyped_source_columns_fail(tmp_path: Path) -> None:
    manifest = _sources(target={"description": "Monthly target"})
    with pytest.raises(CheckFailed, match="sheets.targets.*target.*data_type"):
        create_raw(manifest, warehouse=tmp_path / "warehouse.duckdb", raw_root=tmp_path)


def test_source_in_another_database_needs_a_raw_file(tmp_path: Path) -> None:
    manifest = _sources()
    del manifest["sources"]["source.analytics_toolkit.shop.orders"]["meta"]  # pyright: ignore[reportIndexIssue]
    with pytest.raises(CheckFailed, match="shop.orders.*raw_file"):
        create_raw(manifest, warehouse=tmp_path / "warehouse.duckdb", raw_root=tmp_path)


def _client(tmp_path: Path) -> tuple[Path, dict[str, str]]:
    """A copy of the sample client, and the environment its image provides."""
    client = tmp_path / "sample-client"
    _ = shutil.copytree(SAMPLE, client, ignore=shutil.ignore_patterns("target"))
    env = {
        **os.environ,
        "ANALYTICS_CLIENT_CONFIG": str(client / "client.yaml"),
        "ANALYTICS_TOOLKIT_DBT": str(ROOT / "dbt"),
        "DBT_PROJECT_DIR": str(client / "dbt"),
        "DBT_PROFILES_DIR": str(ROOT / "dbt"),
    }
    return client, env


# The client build runs every toolkit node its sources enable (ADR 0026).
CLIENT_BUILD = pytest.mark.dbt("fqn:*", "source:*")


@CLIENT_BUILD
def test_sample_client_passes(tmp_path: Path) -> None:
    _, env = _client(tmp_path)
    check_client(env, tmp_path / "work")
    # The check leaves the client's checkout untouched.
    assert not (Path(env["DBT_PROJECT_DIR"]) / "dbt_packages").exists()


def test_invalid_client_yaml_fails(tmp_path: Path) -> None:
    client, env = _client(tmp_path)
    _ = (client / "client.yaml").write_text("version: 1\n")
    with pytest.raises(CheckFailed, match="client.yaml"):
        check_client(env, tmp_path / "work")


@CLIENT_BUILD
def test_layer_violation_fails_the_manifest_checks(tmp_path: Path) -> None:
    client, env = _client(tmp_path)
    marts = client / "dbt/models/marts"
    _ = (marts / "orders.sql").write_text(
        "select 1 as order_count\nfrom {{ ref('fct_orders__daily') }}\n"
    )
    with pytest.raises(CheckFailed, match="manifest checks.*orders"):
        check_client(env, tmp_path / "work")


@CLIENT_BUILD
def test_lint_failure_fails(tmp_path: Path) -> None:
    client, env = _client(tmp_path)
    model = client / "dbt/models/marts/fct_orders__daily.sql"
    _ = model.write_text(model.read_text().replace("select", "SELECT", 1))
    with pytest.raises(CheckFailed, match="sqlfluff lint failed"):
        check_client(env, tmp_path / "work")


@CLIENT_BUILD
def test_failing_unit_test_fails_the_build(tmp_path: Path) -> None:
    client, env = _client(tmp_path)
    model = client / "dbt/models/marts/fct_orders__daily.sql"
    _ = model.write_text(model.read_text().replace("where not is_test", "where true"))
    with pytest.raises(CheckFailed, match="dbt build failed"):
        check_client(env, tmp_path / "work")
