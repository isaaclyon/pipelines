"""Declared decimal precision and scale must match output on both adapters."""

import os
import shutil
import subprocess
from collections.abc import Mapping, Sequence
from pathlib import Path
from uuid import uuid4

import duckdb
import pytest
import snowflake.connector

from analytics_transformations.mart_contracts import identifier
from analytics_transformations.publication import (
    publish_snowflake_marts,
)
from tests.model_build import build_models

# A sample dbt project with copies of the real macros (ADR 0026).
pytestmark = pytest.mark.dbt(files=("dbt/macros",))
ROOT = Path(__file__).parents[1]
DRIFTS = ["decimal(18, 3)", "decimal(17, 2)"]


def run_dbt(command: Sequence[str], env: Mapping[str, str], timeout: int) -> None:
    """Retain adapter diagnostics and assert the specific rejection reason."""
    result = subprocess.run(
        command, env=env, timeout=timeout, capture_output=True, text=True, check=False
    )
    if result.returncode:
        raise RuntimeError(result.stdout + result.stderr)


def ephemeral_project(tmp_path: Path, declared: str | None = None) -> Path:
    """Fixture whose decimal column passes through an ephemeral model.

    `declared` changes only the ephemeral model's declaration, so the drift is
    visible solely through the ephemeral output's resolved type.
    """
    project = tmp_path / "project"
    _ = shutil.copytree(ROOT / "tests/fixtures/publication", project)
    _ = shutil.copytree(ROOT / "dbt/macros", project / "macros")
    intermediate = project / "models/intermediate"
    intermediate.mkdir()
    _ = (intermediate / "int_observations__identity.sql").write_text(
        "{{ config(materialized='ephemeral', schema='intermediate') }}\n"
        "select * from {{ ref('stg_fixture__observations') }}\n"
    )
    columns = (project / "models/staging/schema.yml").read_text()
    columns = columns.replace("stg_fixture__observations", "int_observations__identity")
    if declared:
        columns = columns.replace("decimal(18, 2)", declared)
    _ = (intermediate / "schema.yml").write_text(columns)
    mart = project / "models/marts/fct_observations.sql"
    _ = mart.write_text(
        mart.read_text().replace(
            "stg_fixture__observations", "int_observations__identity"
        )
    )
    return project


@pytest.mark.parametrize("declared", DRIFTS)
def test_duckdb_ephemeral_decimal_drift_fails_validation(
    tmp_path: Path, declared: str
) -> None:
    raw = tmp_path / "raw.duckdb"
    with duckdb.connect(str(raw)):
        pass
    build_models(raw, namespace="ci_eph", project=ephemeral_project(tmp_path / "good"))
    with pytest.raises((RuntimeError, ValueError)):
        build_models(
            raw,
            namespace="ci_eph",
            project=ephemeral_project(tmp_path / "bad", declared),
        )


@pytest.mark.skipif(
    os.getenv("NAMING_LIVE_ACCEPTANCE") != "development",
    reason="explicit development acceptance only",
)
@pytest.mark.parametrize("declared", DRIFTS)
def test_snowflake_ephemeral_decimal_drift_preserves_publication(
    tmp_path: Path, declared: str
) -> None:
    """Snowflake reports NUMBER(18,2) as FIXED unless metadata is captured."""
    assert os.environ["ANALYTICS_ENVIRONMENT"] == "development"
    assert os.environ["SNOWFLAKE_DATABASE"]
    assert os.environ["SNOWFLAKE_WAREHOUSE"]
    reader_role = identifier(os.environ["PUBLICATION_READER_ROLE"].lower())
    namespace = "naming_types_" + uuid4().hex[:10]
    database = os.environ["SNOWFLAKE_DATABASE"]

    def publish(project: Path) -> str:
        return publish_snowflake_marts(
            environment="development",
            namespace=namespace,
            project_dir=project,
            profiles_dir=ROOT / "dbt",
            reader_role=reader_role,
            runner=run_dbt,
        ).publication_id

    with snowflake.connector.connect(  # pyright: ignore[reportUnknownMemberType]
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=database,
        private_key_file=os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
        login_timeout=20,
        network_timeout=60,
        session_parameters={"TIMEZONE": "UTC", "STATEMENT_TIMEOUT_IN_SECONDS": 60},
    ) as connection:

        def sql(statement: str, role: str | None = None) -> list[tuple[object, ...]]:
            with connection.cursor() as cursor:
                cursor.execute(f"use role {role or os.environ['SNOWFLAKE_ROLE']}")
                cursor.execute("use secondary roles none")
                cursor.execute(statement)
                return [tuple(row) for row in cursor.fetchall()]  # pyright: ignore[reportAny] # Connector rows checked by assertions.

        marker = f"select publication_id from {namespace}_marts._publication"
        try:
            first = publish(ephemeral_project(tmp_path / "good"))
            assert sql(marker, reader_role) == [(first,)]
            with pytest.raises(
                RuntimeError,
                match=(
                    r"model\.publication_fixture\.int_observations__identity: "
                    "actual output"
                ),
            ):
                _ = publish(ephemeral_project(tmp_path / "bad", declared))
            assert sql(marker, reader_role) == [(first,)]
            assert sql(
                f"select observed_amount from {namespace}_marts.fct_observations",
                reader_role,
            ) == [(100,)]
        finally:
            schemas = sql(
                f"select schema_name from {database}.information_schema.schemata "
                f"where startswith(schema_name, '{namespace.upper()}_')"
            )
            for row in schemas:
                assert isinstance(row[0], str) and row[0].lower().startswith(
                    namespace + "_"
                )
                _ = sql(f'drop schema "{row[0]}" cascade')
