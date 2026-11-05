"""Build the dbt documentation site that the docs service serves.

A sensor requests one build per transformations image, so each deployed release
refreshes the site. ``dbt docs generate`` reads warehouse metadata for the
catalog (column types and table sizes); it loads, builds and grants nothing.
The result is dbt's single self-contained page plus a column lineage page from
dbt-colibri at ``lineage/``. Both are swapped in only after both build: a failed
build leaves the previous site in place. dbt runs through ``analytics-dbt``
against the client's dbt project at ``DBT_PROJECT_DIR``, so the site documents
the client's models and the toolkit models it enables.
"""

import json
import os
import shutil
import subprocess
import tempfile
from collections.abc import Mapping
from pathlib import Path

import dagster as dg

from analytics_transformations.client_dbt import executable
from analytics_transformations.manifest_values import mapping, read_json, text

JOB_NAME = "dbt_docs"
SENSOR_NAME = "dbt_docs_release_sensor"
ENVIRONMENTS = frozenset({"local", "ci", "development", "production"})
DOCS_DIR = Path("/var/lib/analytics/docs")
TIMEOUT_SECONDS = 600


def docs_namespace(env: Mapping[str, str]) -> str:
    """Document the environment's namespace: its models build in
    ``<environment>_staging`` and ``<environment>_marts``."""
    environment = env.get("ANALYTICS_ENVIRONMENT", "")
    if environment not in ENVIRONMENTS:
        raise ValueError("Select an environment with ANALYTICS_ENVIRONMENT")
    return environment


def lineage_catalog(manifest: object, catalog: object) -> dict[str, object]:
    """Add declared columns for models the warehouse catalog lacks.

    The catalog has no columns for a model that has not been built in the
    documented namespace, and dbt-colibri cannot trace through it. Declared
    columns stand in; the package tests compare them with built outputs.
    """
    catalog = mapping(catalog)
    nodes = mapping(catalog["nodes"])
    for unique_id, value in mapping(mapping(manifest)["nodes"]).items():
        node = mapping(value)
        if node["resource_type"] != "model" or unique_id in nodes:
            continue
        columns = [mapping(column) for column in mapping(node["columns"]).values()]
        nodes[unique_id] = {
            "unique_id": unique_id,
            "metadata": {
                "type": "VIEW",
                "database": text(node["database"]),
                "schema": text(node["schema"]),
                "name": text(node["alias"] or node["name"]),
                "comment": None,
                "owner": None,
            },
            "columns": {
                text(column["name"]): {
                    "name": text(column["name"]),
                    "type": text(column["data_type"]),
                    "index": index,
                    "comment": None,
                }
                for index, column in enumerate(columns, start=1)
            },
            "stats": {},
        }
    return {**catalog, "nodes": nodes}


def _run(command: list[str], env: Mapping[str, str], failure: str) -> None:
    try:
        _ = subprocess.run(command, env=env, timeout=TIMEOUT_SECONDS, check=True)
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
        raise RuntimeError(failure) from error


def _stage(page: Path, directory: Path) -> Path:
    """Copy a nonempty page beside its destination so the rename is atomic."""
    if not page.is_file() or page.stat().st_size == 0:
        raise RuntimeError(f"{page.name} is missing or empty")
    directory.mkdir(exist_ok=True)
    handle, staged = tempfile.mkstemp(prefix=".index.", dir=directory)
    try:
        with open(handle, "wb") as out, page.open("rb") as source:
            shutil.copyfileobj(source, out)
            out.flush()
            os.fsync(out.fileno())
        os.chmod(staged, 0o644)
    except BaseException:
        Path(staged).unlink(missing_ok=True)
        raise
    return Path(staged)


def build_docs(env: Mapping[str, str], site: Path) -> tuple[Path, Path]:
    """Generate the docs and lineage pages and replace both in ``site``."""
    namespace = docs_namespace(env)
    project = env["DBT_PROJECT_DIR"]
    with tempfile.TemporaryDirectory() as directory:
        target = Path(directory)
        _run(
            [
                executable("analytics-dbt"),
                "docs",
                "generate",
                "--static",
                "--project-dir",
                project,
                "--profiles-dir",
                env.get("DBT_PROFILES_DIR", project),
                "--target-path",
                str(target),
                "--no-partial-parse",
            ],
            # Opting out also stops the docs page from loading dbt's tracker.
            {**env, "DBT_NAMESPACE": namespace, "DO_NOT_TRACK": "1"},
            "dbt docs generate failed",
        )
        catalog = lineage_catalog(
            read_json(target / "manifest.json"), read_json(target / "catalog.json")
        )
        _ = (target / "lineage_catalog.json").write_text(json.dumps(catalog))
        _run(
            [
                env.get(
                    "COLIBRI_EXECUTABLE",
                    str(
                        Path(__file__).resolve().parents[3]
                        / "lineage/.venv/bin/colibri"
                    ),
                ),
                "generate",
                "--manifest",
                str(target / "manifest.json"),
                "--catalog",
                str(target / "lineage_catalog.json"),
                "--output-dir",
                str(target / "lineage"),
                "--disable-telemetry",
            ],
            {**env, "DO_NOT_TRACK": "1"},
            "colibri generate failed",
        )
        pages = (site / "index.html", site / "lineage" / "index.html")
        staged: list[Path] = []
        try:
            for built, page in zip(
                (target / "static_index.html", target / "lineage" / "index.html"),
                pages,
                strict=True,
            ):
                staged.append(_stage(built, page.parent))
            for path, page in zip(staged, pages, strict=True):
                _ = os.replace(path, page)
        finally:
            for path in staged:
                path.unlink(missing_ok=True)
    return pages


# Dagster's op and job decorators expose untyped third-party signatures.
@dg.op  # pyright: ignore[reportUnknownMemberType]
def generate_dbt_docs(context: dg.OpExecutionContext) -> None:
    for page in build_docs(os.environ, DOCS_DIR):
        context.log.info(f"Wrote {page} ({page.stat().st_size} bytes)")


@dg.job(  # pyright: ignore[reportUnknownMemberType]
    name=JOB_NAME,
    executor_def=dg.in_process_executor,
    tags={"dagster/max_retries": "0"},
)
def docs_job() -> None:
    """Rebuild the dbt docs and column lineage pages from warehouse metadata."""
    generate_dbt_docs()


def release_request(env: Mapping[str, str]) -> dg.RunRequest | dg.SkipReason:
    # The run key deduplicates: each release image builds the site once.
    image = env.get("DAGSTER_CURRENT_IMAGE", "")
    if not image:
        return dg.SkipReason("DAGSTER_CURRENT_IMAGE names no release image")
    return dg.RunRequest(run_key=f"{JOB_NAME}:{image}")


@dg.sensor(  # pyright: ignore[reportUnknownMemberType]
    name=SENSOR_NAME,
    job=docs_job,
    minimum_interval_seconds=300,
    default_status=dg.DefaultSensorStatus.STOPPED,
)
def docs_sensor() -> dg.RunRequest | dg.SkipReason:
    return release_request(os.environ)


defs = dg.Definitions(jobs=[docs_job], sensors=[docs_sensor])
