"""One manifest gate runs every parsed-manifest rule, for every caller."""

import copy
import os
import shutil
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import cast

import pytest

from analytics_transformations.manifest_checks import check_manifest
from analytics_transformations.manifest_values import read_json

# The synthetic sample project with copies of the real macros (ADR 0026).
pytestmark = pytest.mark.dbt(files=("dbt/macros",))
ROOT = Path(__file__).parents[1]
STAGING = "model.sample_fixture.stg_fixture__observations"
MART = "model.sample_fixture.fct_observations"
NAMESPACE = "check"

type Manifest = dict[str, dict[str, dict[str, object]]]


@pytest.fixture(scope="module")
def parsed(tmp_path_factory: pytest.TempPathFactory) -> Manifest:
    root = tmp_path_factory.mktemp("manifest-checks")
    project = root / "project"
    _ = shutil.copytree(ROOT / "tests/fixtures/sample", project)
    _ = shutil.copytree(ROOT / "dbt/macros", project / "macros")
    env = dict(os.environ) | {
        "DBT_TARGET": "local",
        "DBT_NAMESPACE": NAMESPACE,
        "DUCKDB_PATH": str(root / "fixture.duckdb"),
    }
    _ = subprocess.run(
        [
            str(Path(sys.executable).with_name("dbt")),
            "parse",
            "--project-dir",
            str(project),
            "--profiles-dir",
            str(ROOT / "dbt"),
            "--target-path",
            str(root / "target"),
            "--no-partial-parse",
        ],
        env=env,
        check=True,
        timeout=120,
    )
    return cast(Manifest, read_json(root / "target" / "manifest.json"))


def _config(node: dict[str, object]) -> dict[str, object]:
    return cast(dict[str, object], node["config"])


def _meta(node: dict[str, object]) -> dict[str, object]:
    return cast(dict[str, object], _config(node)["meta"])


def _column(node: dict[str, object], name: str) -> dict[str, object]:
    return cast(dict[str, dict[str, object]], node["columns"])[name]


def _layer(data: Manifest) -> None:
    data["nodes"][STAGING]["original_file_path"] = "models/other/stg.sql"


def _key(data: Manifest) -> None:
    _meta(data["nodes"][MART])["key"] = []


def _declaration(data: Manifest) -> None:
    del _meta(data["nodes"][STAGING])["owner"]


def _column_description(data: Manifest) -> None:
    _column(data["nodes"][STAGING], "currency_code")["description"] = ""


def _documentation(data: Manifest) -> None:
    del _meta(data["nodes"][STAGING])["schema_description"]


def _pii(data: Manifest) -> None:
    column = _column(data["nodes"][STAGING], "currency_code")
    del cast(dict[str, object], _config(column)["meta"])["pii"]


def test_fixture_passes_every_check(parsed: Manifest) -> None:
    _ = check_manifest(parsed, namespace=NAMESPACE)


@pytest.mark.parametrize(
    ("breakage", "message"),
    [
        (_layer, "unknown-layer"),
        (_key, "key must name"),
        (_declaration, "owner and grain"),
        (_column_description, "column description"),
        (_documentation, "schema_description"),
        (_pii, "config.meta.pii"),
    ],
)
def test_every_rule_runs(
    parsed: Manifest, breakage: Callable[[Manifest], None], message: str
) -> None:
    data = copy.deepcopy(parsed)
    breakage(data)
    with pytest.raises(ValueError, match=message):
        _ = check_manifest(data, namespace=NAMESPACE)


def test_namespace_is_checked(parsed: Manifest) -> None:
    with pytest.raises(ValueError, match="resource-schema"):
        _ = check_manifest(parsed, namespace="other")
