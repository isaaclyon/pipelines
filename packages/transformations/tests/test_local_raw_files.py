"""ADR 0029: the local target reads each source's raw DuckDB file read-only.

Loaders write separate raw files under the ingestion state root. With
``DBT_RAW_ROOT`` set on the local target, each enabled source with a
``meta.raw_file`` reads that file through a read-only attachment. Snowflake
targets and fixture builds without the setting keep reading the target database.
"""

import json
import os
import subprocess
from pathlib import Path
from typing import cast

import duckdb
import pytest

from tests.client_files import ALL_SOURCES, write_client
from tests.goodday_fixtures import write_inventory

# Real dbt project nodes these tests execute or parse (ADR 0026).
pytestmark = pytest.mark.dbt(
    "+stg_goodday__inventory_items",
    "stg_shopify_orders__orders",
    "+stg_shopify_orders__ingestion_batches",
    "+stg_shopify_orders__refund_duties",
)

ROOT = Path(__file__).parents[1]
GOODDAY_FILE = Path("goodday/local/goodday.duckdb")
SHOPIFY_FILE = Path("shopify_orders/local/shopify_orders.duckdb")


def _dbt(*arguments: str, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "analytics-dbt",
            *arguments,
            "--project-dir",
            str(ROOT / "dbt"),
            "--profiles-dir",
            str(ROOT / "dbt"),
            "--no-partial-parse",
        ],
        env=dict(os.environ) | env,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )


def _local(tmp_path: Path, root: Path) -> dict[str, str]:
    return {
        "DBT_TARGET": "local",
        "DBT_NAMESPACE": "ci_raw_files",
        "DUCKDB_PATH": str(tmp_path / "warehouse.duckdb"),
        "DBT_RAW_ROOT": str(root),
        **write_client(tmp_path / "client", "goodday", "shopify_orders"),
        "GOODDAY_DATASET": "goodday_raw",
    }


def test_local_build_reads_a_separate_raw_file_read_only(tmp_path: Path) -> None:
    root = tmp_path / "ingestion"
    raw = root / GOODDAY_FILE
    raw.parent.mkdir(parents=True)
    write_inventory(raw)
    before = raw.read_bytes()

    # The Shopify file is absent, but this selection does not read it.
    result = _dbt(
        "build",
        "--target-path",
        str(tmp_path / "target"),
        "--select",
        "+stg_goodday__inventory_items",
        env=_local(tmp_path, root),
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert raw.read_bytes() == before
    assert not (root / SHOPIFY_FILE).exists()
    with duckdb.connect(str(tmp_path / "warehouse.duckdb"), read_only=True) as db:
        # The warehouse holds no copy of raw tables.
        assert db.execute(
            "select count(*) from information_schema.schemata "
            "where schema_name = 'goodday_raw'"
        ).fetchone() == (0,)
        # The staging view reads the raw file through its attachment.
        _ = db.execute(f"attach '{raw}' as raw_goodday (read_only)")
        assert db.execute(
            "select item_id from ci_raw_files_staging.stg_goodday__inventory_items "
            "order by item_id"
        ).fetchall() == [(1,), (2,), (3,)]


def test_missing_raw_file_fails_the_dependent_build_clearly(tmp_path: Path) -> None:
    root = tmp_path / "ingestion"
    root.mkdir()

    result = _dbt(
        "run",
        "--target-path",
        str(tmp_path / "target"),
        "--select",
        "stg_shopify_orders__orders",
        env=_local(tmp_path, root),
    )

    assert result.returncode != 0
    assert (
        f"Raw file for source shopify_orders is missing: {root / SHOPIFY_FILE}"
        in result.stdout
    ), result.stdout
    assert list(root.iterdir()) == []


def test_loader_shaped_shopify_file_builds_without_sparse_child_tables(
    tmp_path: Path,
) -> None:
    """The loader lowercases its default schema, and dlt creates a nested table
    only once a row needs it: a short live window may have no refund duties."""
    root = tmp_path / "ingestion"
    raw = root / SHOPIFY_FILE
    raw.parent.mkdir(parents=True)
    with duckdb.connect(str(raw)) as db:
        _ = db.execute("create schema shopify_orders_raw")
        _ = db.execute(
            "create table shopify_orders_raw.ingestion_batches as select "
            "'s1' as shop_id, 'b1' as batch_id, 'lookback' as kind, "
            "'updated_at' as window_field, "
            "timestamptz '2026-11-01 00:00:00+00' as window_start, "
            "timestamptz '2026-11-01 01:00:00+00' as window_end, "
            "timestamptz '2026-11-01 01:00:00+00' as observed_started_at, "
            "timestamptz '2026-11-01 01:01:00+00' as observed_finished_at, "
            "timestamptz '2026-11-01 01:02:00+00' as completed_at, "
            "'complete' as status, 'complete' as completeness, "
            "0::bigint as exceptions, 0::bigint as orders_observed, "
            "3::bigint as extraction_revision, 'q' as query_version"
        )

    result = _dbt(
        "run",
        "--target-path",
        str(tmp_path / "target"),
        "--select",
        "stg_shopify_orders__ingestion_batches stg_shopify_orders__refund_duties",
        env=_local(tmp_path, root),
    )

    assert result.returncode == 0, result.stdout + result.stderr
    with duckdb.connect(str(tmp_path / "warehouse.duckdb")) as db:
        _ = db.execute(f"attach '{raw}' as raw_shopify_orders (read_only)")
        batches = "ci_raw_files_staging.stg_shopify_orders__ingestion_batches"
        assert db.execute(f"select batch_id from {batches}").fetchall() == [("b1",)]
        duties = db.execute(
            "select * from ci_raw_files_staging.stg_shopify_orders__refund_duties"
        )
        assert duties.fetchall() == []
        assert [column[0] for column in duties.description][:3] == [
            "shop_id",
            "order_id",
            "refund_id",
        ]


def _source_relations(target: Path, env: dict[str, str]) -> dict[str, str]:
    result = _dbt("parse", "--target-path", str(target), env=env)
    assert result.returncode == 0, result.stdout + result.stderr
    manifest = cast(
        dict[str, dict[str, dict[str, str]]],
        json.loads((target / "manifest.json").read_text()),
    )
    return {key: node["relation_name"] for key, node in manifest["sources"].items()}


@pytest.mark.parametrize("dbt_target", ["snowflake-development", "production"])
def test_remote_targets_ignore_local_raw_files(tmp_path: Path, dbt_target: str) -> None:
    env = {
        "DBT_TARGET": dbt_target,
        "DBT_NAMESPACE": "candidate",
        "SNOWFLAKE_ACCOUNT": "fixture",
        "SNOWFLAKE_USER": "fixture",
        "SNOWFLAKE_DATABASE": "FIXTURE_RAW",
        "ANALYTICS_CLIENT_CONFIG": str(ALL_SOURCES),
    }
    plain = _source_relations(tmp_path / "plain", env)
    attached = _source_relations(
        tmp_path / "attached", env | {"DBT_RAW_ROOT": str(tmp_path)}
    )

    assert attached == plain
    assert all(name.startswith("FIXTURE_RAW.") for name in plain.values()), plain


def test_local_target_names_attached_files_only_when_configured(
    tmp_path: Path,
) -> None:
    env = _local(tmp_path, tmp_path)
    attached = _source_relations(tmp_path / "attached", env)
    plain = _source_relations(
        tmp_path / "plain", {k: v for k, v in env.items() if k != "DBT_RAW_ROOT"}
    )

    def database(relations: dict[str, str], source: str) -> set[str]:
        return {
            name.split(".")[0]
            for key, name in relations.items()
            if key.split(".")[2] == source
        }

    assert set(plain) == set(attached)
    assert {name.split(".")[0] for name in plain.values()} == {'"warehouse"'}
    assert database(attached, "shopify_orders") == {'"raw_shopify_orders"'}
    assert database(attached, "goodday") == {'"raw_goodday"'}
