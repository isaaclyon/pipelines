"""Build deterministic fixtures in disposable files through the model gates.

dbt runs through ``analytics-dbt``, so the client configuration that
``ANALYTICS_CLIENT_CONFIG`` names chooses the enabled sources.

The gates are the manifest checks, ``dbt build`` with every data and unit test,
and a comparison of each built model's output columns and types with its
declarations, including decimal precision and scale.
"""

import json
import os
import subprocess
from collections.abc import Mapping, Sequence
from pathlib import Path

from analytics_transformations.client_dbt import executable
from analytics_transformations.layer_dependencies import governed_projects
from analytics_transformations.manifest_checks import check_manifest
from analytics_transformations.manifest_values import (
    mapping,
    read_json,
    sequence,
    text,
)

ROOT = Path(__file__).parents[1]


def run_dbt(command: Sequence[str], env: Mapping[str, str], timeout: int) -> None:
    """Run dbt through analytics-dbt; raise with its console output on failure."""
    target = Path(command[command.index("--target-path") + 1])
    target.mkdir(parents=True, exist_ok=True)
    result = subprocess.run(
        [executable("analytics-dbt"), *command[1:]],
        env={**env, "DBT_LOG_PATH": str(target)},
        timeout=timeout,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode:
        raise RuntimeError(f"dbt {command[1]} failed:\n{result.stdout}{result.stderr}")


def _options(project: Path, target: Path) -> list[str]:
    return [
        "--project-dir",
        str(project),
        "--profiles-dir",
        str(ROOT / "dbt"),
        "--target-path",
        str(target),
        "--no-partial-parse",
    ]


def build_models(
    database: Path,
    *,
    namespace: str,
    project: Path = ROOT / "dbt",
    timeout: int = 240,
) -> Path:
    """Build every node in the test-owned file; return the build artifacts."""
    env = dict(os.environ)
    env.update(
        ANALYTICS_ENVIRONMENT="ci",
        DBT_TARGET="local",
        DBT_NAMESPACE=namespace,
        DUCKDB_PATH=str(database),
    )
    artifacts = database.parent / "target"
    parsed, built = artifacts / "parse", artifacts / "build"
    run_dbt(["dbt", "parse", *_options(project, parsed)], env, timeout)
    check_manifest(read_json(parsed / "manifest.json"), namespace=namespace)
    run_dbt(["dbt", "build", *_options(project, built)], env, timeout)
    manifest = mapping(read_json(built / "manifest.json"))
    nodes = mapping(manifest.get("nodes"))
    projects = governed_projects(manifest)
    outputs: list[dict[str, object]] = []
    for node_id, value in sorted(nodes.items()):
        node = mapping(value)
        if (
            node.get("resource_type") != "model"
            or node.get("package_name") not in projects
        ):
            continue
        config = mapping(node.get("config"))
        # Ephemeral SQL has dbt's injected CTEs; other models are read directly.
        sql = (
            text(node.get("compiled_code"))
            if config.get("materialized") == "ephemeral"
            else "select * from " + text(node.get("relation_name"))
        )
        # Only type declarations: full column metadata can exceed Linux's
        # per-argument limit for a moderately sized project.
        columns = {
            name: {"name": name, "data_type": mapping(column).get("data_type")}
            for name, column in mapping(node.get("columns")).items()
        }
        outputs.append({"model_id": node_id, "sql": sql, "columns": columns})
    _ = sequence(outputs)
    run_dbt(
        [
            "dbt",
            "run-operation",
            "validate_model_outputs",
            *_options(project, artifacts / "validate-outputs"),
            "--args",
            json.dumps({"outputs": outputs}),
        ],
        env,
        timeout,
    )
    return built
