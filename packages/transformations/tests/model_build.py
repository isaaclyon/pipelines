"""Build deterministic fixtures in disposable files, without serving or promotion."""

import os
from pathlib import Path

from analytics_transformations.publication import Runner, build_candidate, run_dbt

ROOT = Path(__file__).parents[1]


def build_models(
    database: Path,
    *,
    namespace: str,
    project: Path = ROOT / "dbt",
    runner: Runner = run_dbt,
    publication_group: str | None = None,
) -> str:
    """Run the same model gates as Snowflake, directly in the test-owned file."""
    generation = "deterministic-fixture"
    env = dict(os.environ)
    env.update(
        ANALYTICS_ENVIRONMENT="ci",
        DBT_TARGET="local",
        DBT_NAMESPACE=namespace,
        DUCKDB_PATH=str(database),
        DBT_PUBLICATION_ID=generation,
    )
    build_candidate(
        project,
        ROOT / "dbt",
        database.parent / "target",
        env,
        240,
        runner,
        publication_group=(publication_group or "orders")
        if project == ROOT / "dbt"
        else publication_group,
    )
    return generation
