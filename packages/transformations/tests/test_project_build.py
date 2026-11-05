"""The shared synthetic build covers the whole project (ADR 0026)."""

import json

import pytest

from tests.project_build import ProjectBuild

# The shared build executes every node, so any node change runs this test.
pytestmark = pytest.mark.dbt("fqn:*", "source:*")


def test_project_builds_through_the_model_gates(
    project_build: ProjectBuild,
) -> None:
    """The build passed the model gates and ran every kind of check."""
    results = json.loads((project_build.target / "run_results.json").read_text())  # pyright: ignore[reportAny] # dbt artifact read in a test.
    kinds = {
        str(result["unique_id"]).split(".")[0]  # pyright: ignore[reportAny]
        for result in results["results"]  # pyright: ignore[reportAny]
    }
    assert {"model", "test", "unit_test"} <= kinds
