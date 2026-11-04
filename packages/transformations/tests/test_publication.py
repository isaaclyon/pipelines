"""Execute deterministic model validation without a serving boundary."""

import shutil
from pathlib import Path

import duckdb
import pytest

from analytics_transformations.publication import validate_results
from tests.model_build import build_models
from tests.test_mart_contracts import manifest
from tests.test_output_types_adapters import run_dbt

# A sample dbt project with copies of the real macros (ADR 0026).
pytestmark = pytest.mark.dbt(files=("dbt/macros",))
ROOT = Path(__file__).parents[1]


def test_required_results_cannot_be_omitted() -> None:
    value = manifest()
    value["metadata"] = {"invocation_id": "one"}
    with pytest.raises(ValueError, match="lacks required"):
        validate_results(value, {"metadata": {"invocation_id": "one"}, "results": []})


def test_stale_artifacts_cannot_publish() -> None:
    value = manifest()
    value["metadata"] = {"invocation_id": "one"}
    with pytest.raises(ValueError, match="invocation mismatch"):
        validate_results(value, {"metadata": {"invocation_id": "two"}, "results": []})


@pytest.mark.parametrize("drift", ["extra_column", "decimal_scale", "missing_pii"])
def test_internal_output_drift_fails_validation(tmp_path: Path, drift: str) -> None:
    project = tmp_path / "project"
    _ = shutil.copytree(ROOT / "tests/fixtures/publication", project)
    _ = shutil.copytree(ROOT / "dbt/macros", project / "macros")
    raw = tmp_path / "raw.duckdb"
    with duckdb.connect(str(raw)):
        pass

    def build() -> None:
        _ = build_models(raw, namespace="ci_drift", project=project)

    build()
    staging = project / "models/staging/stg_fixture__observations.sql"
    schema = project / "models/staging/schema.yml"
    if drift == "extra_column":
        _ = staging.write_text(
            staging.read_text() + ", cast(1 as bigint) as debug_count\n"
        )
    elif drift == "decimal_scale":
        _ = schema.write_text(
            schema.read_text().replace("decimal(18, 2)", "decimal(18, 3)")
        )
    elif drift == "missing_pii":
        _ = schema.write_text(schema.read_text().replace("pii: false", "pii: null", 1))
    with pytest.raises((RuntimeError, ValueError)):
        build()


@pytest.mark.dbt_subprocess
def test_real_dbt_ephemeral_ancestor_builds(tmp_path: Path) -> None:
    project = tmp_path / "project"
    _ = shutil.copytree(ROOT / "tests/fixtures/publication", project)
    _ = shutil.copytree(ROOT / "dbt/macros", project / "macros")
    intermediate = project / "models/intermediate"
    intermediate.mkdir()
    _ = (intermediate / "int_observations__identity.sql").write_text(
        "{{ config(materialized='ephemeral', schema='intermediate') }}\n"
        "select * from {{ ref('stg_fixture__observations') }}\n"
    )
    _ = (intermediate / "int_observations__reused.sql").write_text(
        "{{ config(materialized='ephemeral', schema='intermediate') }}\n"
        "select * from {{ ref('int_observations__identity') }}\n"
    )
    columns = (project / "models/staging/schema.yml").read_text()
    _ = (intermediate / "schema.yml").write_text(
        columns.replace("stg_fixture__observations", "int_observations__identity")
        + columns.partition("models:\n")[2].replace(
            "stg_fixture__observations", "int_observations__reused"
        )
    )
    mart = project / "models/marts/fct_observations.sql"
    _ = mart.write_text(
        mart.read_text().replace(
            "stg_fixture__observations", "int_observations__reused"
        )
    )
    raw = tmp_path / "raw.duckdb"
    with duckdb.connect(str(raw)):
        pass
    _ = build_models(raw, namespace="analytics", project=project)
    with duckdb.connect(str(raw), read_only=True) as reader:
        assert reader.execute(
            "select observed_amount from analytics_marts.fct_observations"
        ).fetchone() == (100,)


@pytest.mark.parametrize("ephemeral", [False, True])
def test_internal_column_without_type_keeps_privacy_inventory_gate(
    tmp_path: Path, ephemeral: bool
) -> None:
    project = tmp_path / "project"
    _ = shutil.copytree(ROOT / "tests/fixtures/publication", project)
    _ = shutil.copytree(ROOT / "dbt/macros", project / "macros")
    schema = project / "models/staging/schema.yml"
    _ = schema.write_text(schema.read_text().replace("        data_type: bigint\n", ""))
    staging = project / "models/staging/stg_fixture__observations.sql"
    materialized = "ephemeral" if ephemeral else "view"
    _ = staging.write_text(
        "{{ config(alias='stg_fixture__final', materialized='"
        + materialized
        + "') }}\n"
        + staging.read_text()
    )
    raw = tmp_path / "raw.duckdb"
    with duckdb.connect(str(raw)):
        pass

    def build() -> None:
        _ = build_models(raw, namespace="ci_inventory", project=project, runner=run_dbt)

    build()
    _ = staging.write_text(staging.read_text() + ", 'private' as undeclared_contact\n")
    with pytest.raises(RuntimeError, match="actual output columns"):
        build()


def test_native_warning_result_needs_no_duplicate_blocking_flag() -> None:
    from analytics_transformations.mart_contracts import mapping

    value = manifest()
    value["metadata"] = {"invocation_id": "one"}
    nodes = mapping(value["nodes"])
    nodes["test.analytics.advisory"] = {
        "resource_type": "test",
        "name": "advisory",
        "depends_on": {"nodes": ["model.analytics.fct_examples"]},
        "config": {"severity": "warn"},
    }
    value["nodes"] = nodes
    results = {
        "metadata": {"invocation_id": "one"},
        "results": [
            {
                "unique_id": key,
                "status": "warn" if key.endswith("advisory") else "success",
            }
            for key in nodes
        ],
    }
    assert validate_results(value, results)
    mapping_node = mapping(nodes["test.analytics.advisory"])
    mapping_node["config"] = {"severity": "error"}
    nodes["test.analytics.advisory"] = mapping_node
    with pytest.raises(ValueError, match="Unsuccessful publication"):
        validate_results(value, results)
