"""Declared decimal precision and scale must match the built output."""

import shutil
from pathlib import Path

import duckdb
import pytest

from tests.model_build import build_models

# A sample dbt project with copies of the real macros (ADR 0026).
pytestmark = pytest.mark.dbt(files=("dbt/macros",))
ROOT = Path(__file__).parents[1]
DRIFTS = ["decimal(18, 3)", "decimal(17, 2)"]


def ephemeral_project(tmp_path: Path, declared: str | None = None) -> Path:
    """Fixture whose decimal column passes through an ephemeral model.

    `declared` changes only the ephemeral model's declaration, so the drift is
    visible solely through the ephemeral output's resolved type.
    """
    project = tmp_path / "project"
    _ = shutil.copytree(ROOT / "tests/fixtures/sample", project)
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
