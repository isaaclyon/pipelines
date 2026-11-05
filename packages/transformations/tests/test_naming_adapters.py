"""Executed naming and concurrent namespace acceptance on both adapters."""

import json
import os
import shutil
import subprocess
from collections.abc import Callable, Generator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, nullcontext
from dataclasses import replace
from pathlib import Path
from threading import Barrier, Lock
from typing import cast
from uuid import uuid4

import duckdb
import pytest
import snowflake.connector

from analytics_transformations.layer_dependencies import (
    check,
    governed_projects,
    parse_manifest,
)

# A sample dbt project with copies of the real macros (ADR 0026).
pytestmark = pytest.mark.dbt(files=("dbt/macros",))
ROOT = Path(__file__).resolve().parents[1]
LONG_ALIAS = "fct_" + "a" * 251
LAYERS = ("staging", "intermediate", "marts", "seeds", "snapshots", "test_failures")
type Rows = list[tuple[object, ...]]


def _project(path: Path) -> None:
    for folder in ("models/marts", "macros", "seeds", "snapshots", "tests"):
        (path / folder).mkdir(parents=True, exist_ok=True)
    shutil.copy(ROOT / "dbt/macros/generate_schema_name.sql", path / "macros")
    shutil.copy(ROOT / "dbt/profiles.yml", path)
    (path / "dbt_project.yml").write_text(
        "name: naming_acceptance\nversion: '1.0'\n"
        "config-version: 2\nprofile: analytics\n"
        "models:\n  naming_acceptance:\n    marts:\n      +schema: marts\n"
        "      +materialized: view\n"
    )
    for name, config in (
        ("fct_orders_v1", ""),
        ("fct_orders_v2", ""),
        ("fct_customers", "{{ config(alias='fct_customers__descriptive') }}"),
        ("fct_identifiers", "{{ config(alias='" + LONG_ALIAS + "') }}"),
    ):
        (path / "models/marts" / f"{name}.sql").write_text(
            config + "\nselect {{ var('fixture_value') }} as fixture_id\n"
        )
    (path / "models/marts/models.yml").write_text(
        "version: 2\nmodels:\n  - name: fct_orders\n    latest_version: 2\n"
        "    versions:\n      - v: 1\n      - v: 2\n"
        "sources:\n  - name: fixtures\n"
        "    schema: \"{{ env_var('DBT_NAMESPACE') }}_seeds\"\n"
        "    tables:\n      - name: customer_examples\n"
        "        columns:\n          - name: fixture_id\n"
        "            data_tests:\n              - accepted_values:\n"
        "                  arguments:\n                    values: [1, 2]\n"
    )
    (path / "seeds/customer_examples.csv").write_text("fixture_id\n1\n")
    (path / "snapshots/customer_history.sql").write_text(
        "{% snapshot customer_history %}\n"
        "{{ config(strategy='check', unique_key='fixture_id', check_cols='all') }}\n"
        "select * from {{ source('fixtures', 'customer_examples') }}\n"
        "{% endsnapshot %}\n"
    )
    (path / "tests/customer_consistency.sql").write_text(
        "{{ config(store_failures=true) }}\n"
        "select * from {{ ref('fct_orders', version=2) }} where fixture_id < 0\n"
    )


def _build(project: Path, env: dict[str, str], value: int) -> None:
    result = subprocess.run(
        [
            "dbt",
            "build",
            "--quiet",
            "--project-dir",
            str(project),
            "--profiles-dir",
            str(project),
            "--select",
            "*",
            "--vars",
            json.dumps({"fixture_value": value}),
            "--no-partial-parse",
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=240,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


@contextmanager
def _database(backend: str, path: Path) -> Generator[Callable[[str], Rows]]:
    if backend == "duckdb":

        def local(statement: str) -> Rows:
            with duckdb.connect(str(path)) as connection:
                return connection.execute(statement).fetchall()

        yield local
        return
    assert os.environ["ANALYTICS_ENVIRONMENT"] == "development"
    assert os.environ["SNOWFLAKE_DATABASE"]
    assert os.environ["SNOWFLAKE_WAREHOUSE"]
    with snowflake.connector.connect(  # pyright: ignore[reportUnknownMemberType]
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        private_key_file=os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
        login_timeout=20,
        network_timeout=60,
        session_parameters={"STATEMENT_TIMEOUT_IN_SECONDS": 60},
    ) as connection:

        def remote(statement: str) -> Rows:
            with connection.cursor() as cursor:
                _ = cursor.execute(statement)
                return [tuple(row) for row in cursor.fetchall()]  # pyright: ignore[reportAny]

        yield remote


@pytest.mark.parametrize(
    "backend",
    [
        "duckdb",
        pytest.param(
            "snowflake",
            marks=pytest.mark.skipif(
                os.getenv("NAMING_LIVE_ACCEPTANCE") != "development",
                reason="requires explicit naming development acceptance",
            ),
        ),
    ],
)
def test_executed_identifiers_and_independent_namespaces(
    tmp_path: Path, backend: str
) -> None:
    namespaces = ["naming_accept_" + uuid4().hex[:12] for _ in range(2)]
    projects = [tmp_path / name for name in namespaces]
    database = tmp_path / "shared.duckdb"
    environments = [
        dict(os.environ)
        | {
            "DBT_NAMESPACE": name,
            "DUCKDB_PATH": str(database),
            "DBT_TARGET": "local" if backend == "duckdb" else "snowflake-development",
            "SNOWFLAKE_SCHEMA": name + "_marts",
            "DBT_STATEMENT_TIMEOUT_SECONDS": "60",
            "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
        }
        for name in namespaces
    ]
    for project in projects:
        _project(project)
    barrier, writer = Barrier(2), Lock()

    def build(index: int) -> None:
        _ = barrier.wait(timeout=20)
        # Both workers coexist; DuckDB explicitly serializes shared-file writers.
        with writer if backend == "duckdb" else nullcontext():
            _build(projects[index], environments[index], index + 10)

    with _database(backend, database) as sql:

        def cleanup(namespace: str) -> None:
            for layer in LAYERS:
                _ = sql(f"drop schema if exists {namespace}_{layer} cascade")

        def outputs(namespace: str, value: int) -> None:
            for alias in (
                "fct_orders_v1",
                "fct_orders_v2",
                "fct_customers__descriptive",
                LONG_ALIAS,
            ):
                assert sql(f"select fixture_id from {namespace}_marts.{alias}") == [
                    (value,)
                ]
                assert sql(
                    f"select FIXTURE_ID from {namespace.upper()}_MARTS.{alias.upper()}"
                ) == [(value,)]
            assert sql(
                f"select fixture_id from {namespace}_seeds.customer_examples"
            ) == [(1,)]
            assert sql(
                f"select fixture_id from {namespace}_snapshots.customer_history"
            ) == [(1,)]
            assert (
                sql(f"select * from {namespace}_test_failures.customer_consistency")
                == []
            )

        try:
            with ThreadPoolExecutor(max_workers=2) as pool:
                list(pool.map(build, range(2)))
            for index, namespace in enumerate(namespaces):
                outputs(namespace, index + 10)
                data = cast(
                    object,
                    json.loads((projects[index] / "target/manifest.json").read_text()),
                )
                resources = parse_manifest(data)
                assert (
                    check(resources, governed_projects(data), namespace=namespace) == []
                )
                model = next(node for node in resources if node.name == "fct_customers")
                for bad in (
                    "fct_" + "a" * 252,
                    "fct_bad-name",
                    "fct_bad;drop",
                ):
                    invalid = [
                        replace(node, alias=bad) if node == model else node
                        for node in resources
                    ]
                    assert check(
                        invalid, governed_projects(data), namespace=namespace
                    ), bad
                collision = [
                    replace(node, alias="fct_orders_v1") if node == model else node
                    for node in resources
                ]
                assert check(collision, governed_projects(data), namespace=namespace)
            _build(projects[0], environments[0], 99)
            outputs(namespaces[0], 99)
            outputs(namespaces[1], 11)
            cleanup(namespaces[0])
            outputs(namespaces[1], 11)
        finally:
            for namespace in namespaces:
                cleanup(namespace)
