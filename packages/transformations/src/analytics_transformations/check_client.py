"""Check a client's dbt project offline: ``analytics-check-client``.

A client repository runs this in its transformations image, which provides the
toolkit package (``ANALYTICS_TOOLKIT_DBT``), the dbt targets
(``DBT_PROFILES_DIR``) and the client's dbt project (``DBT_PROJECT_DIR``). It
needs no credentials or network. It stops at the first failure:

1. Load ``client.yaml`` (``ANALYTICS_CLIENT_CONFIG``).
2. Copy the client's dbt project to a scratch directory, so the checkout stays
   untouched, and run ``dbt deps``, which installs the toolkit package.
3. ``dbt parse`` and the manifest checks, which govern the client's nodes and
   the toolkit nodes its sources enable.
4. ``sqlfluff lint`` of the client's own model SQL, with the toolkit's rules.
5. ``dbt build`` on a scratch local DuckDB target. Each enabled source reads
   an empty table with the columns and types its declaration names, so the
   build checks SQL, contracts and tests without data; dbt unit tests in the
   client project check its logic.

dbt runs through ``analytics-dbt``, so it sees the client's settings.
"""

import os
import shutil
import subprocess
import sys
import tempfile
from collections import defaultdict
from collections.abc import Mapping
from pathlib import Path

import duckdb
from analytics_client_config import ConfigError

from analytics_transformations.client_dbt import dbt_env, executable
from analytics_transformations.manifest_checks import check_manifest
from analytics_transformations.manifest_values import mapping, read_json, text

NAMESPACE = "client_check"
# The toolkit's lint settings (packages/transformations/.sqlfluff), with the
# scratch project and the image's dbt targets.
_SQLFLUFF = """\
[sqlfluff]
dialect = duckdb
templater = dbt

[sqlfluff:templater:dbt]
project_dir = {project}
profiles_dir = {profiles}
profile = analytics
target = local
"""


class CheckFailed(Exception):
    """A check step failed; the message says which and why."""


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def create_raw(manifest: object, *, warehouse: Path, raw_root: Path) -> None:
    """Create every source in ``manifest`` as an empty table of its declared columns.

    A source in the warehouse's own database is created there. A source in
    another database is created in its ``meta.raw_file`` under ``raw_root``,
    which ``attach_raw_files()`` attaches under that database name.
    """
    tables: defaultdict[Path, list[str]] = defaultdict(list)
    for unique_id, value in mapping(mapping(manifest).get("sources")).items():
        source = mapping(value)
        name = unique_id.split(".", 2)[-1]
        database = text(source.get("database"))
        path = warehouse
        if database != warehouse.stem:
            raw_file = mapping(source.get("meta", {})).get("raw_file")
            if not isinstance(raw_file, str) or not raw_file.strip():
                raise CheckFailed(
                    f"Source {name} reads database {database} but names no "
                    "meta.raw_file"
                )
            path = raw_root / raw_file
        columns: list[str] = []
        for column, declaration in mapping(source.get("columns", {})).items():
            data_type = mapping(declaration).get("data_type")
            if not isinstance(data_type, str) or not data_type.strip():
                raise CheckFailed(
                    f"Source {name}: column {column} declares no data_type, "
                    "which the empty raw table needs"
                )
            columns.append(f"{_quote(column)} {data_type}")
        if not columns:
            raise CheckFailed(f"Source {name} declares no columns")
        schema = _quote(text(source.get("schema")))
        table = f"{schema}.{_quote(text(source.get('identifier')))}"
        tables[path] += [
            f"create schema if not exists {schema}",
            f"create table {table} ({', '.join(columns)})",
        ]
    for path, statements in tables.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with duckdb.connect(str(path)) as db:
                for statement in statements:
                    _ = db.execute(statement)
        except duckdb.Error as error:
            raise CheckFailed(f"Cannot create raw tables in {path}: {error}") from None


def _variable(env: Mapping[str, str], name: str) -> str:
    value = env.get(name, "")
    if not value:
        raise CheckFailed(f"Set {name}; the transformations image provides it")
    return value


def _run(command: list[str], env: Mapping[str, str], step: str) -> None:
    # Output goes to the console, so the failure's details precede the message.
    result = subprocess.run(command, env=env, check=False)
    if result.returncode:
        raise CheckFailed(f"{step} failed (exit {result.returncode}); see above")


def _client_models(manifest: object, project: Path) -> list[str]:
    """The client's own model SQL files, which the lint checks."""
    data = mapping(manifest)
    root = mapping(data.get("metadata")).get("project_name")
    return sorted(
        str(project / text(node.get("original_file_path")))
        for node in map(mapping, mapping(data.get("nodes")).values())
        if node.get("resource_type") == "model"
        and node.get("package_name") == root
        and text(node.get("original_file_path")).endswith(".sql")
    )


def check_client(env: Mapping[str, str], work: Path) -> None:
    """Check the client project at ``DBT_PROJECT_DIR``, using ``work`` as scratch.

    Raises ``CheckFailed`` at the first failed step.
    """
    try:
        _ = dbt_env(env)
    except ConfigError as error:
        raise CheckFailed(f"Client configuration: {error}") from None
    source = Path(_variable(env, "DBT_PROJECT_DIR"))
    profiles = _variable(env, "DBT_PROFILES_DIR")
    _ = _variable(env, "ANALYTICS_TOOLKIT_DBT")
    project = work / "project"
    target = work / "target"
    _ = shutil.copytree(
        source,
        project,
        ignore=lambda folder, names: (
            {"target", "dbt_packages", "logs"} & set(names)
            if Path(folder) == source
            else set()
        ),
    )
    scratch = {
        **env,
        "DBT_PROJECT_DIR": str(project),
        "DBT_TARGET": "local",
        "DBT_NAMESPACE": NAMESPACE,
        "DUCKDB_PATH": str(work / "warehouse.duckdb"),
        "DBT_RAW_ROOT": str(work / "raw"),
        "DBT_TARGET_PATH": str(target),
        "DBT_LOG_PATH": str(work / "logs"),
    }
    dbt = [executable("analytics-dbt")]
    _run([*dbt, "deps", "--project-dir", str(project)], scratch, "dbt deps")
    _run([*dbt, "parse", "--project-dir", str(project)], scratch, "dbt parse")
    manifest = read_json(target / "manifest.json")
    try:
        check_manifest(manifest, namespace=NAMESPACE)
    except ValueError as error:
        raise CheckFailed(f"The manifest checks failed: {error}") from None
    models = _client_models(manifest, project)
    if models:
        config = work / "sqlfluff.cfg"
        _ = config.write_text(_SQLFLUFF.format(project=project, profiles=profiles))
        lint = [executable("sqlfluff"), "lint", "--config", str(config), *models]
        _run(lint, dbt_env(scratch), "sqlfluff lint")
    create_raw(manifest, warehouse=work / "warehouse.duckdb", raw_root=work / "raw")
    _run([*dbt, "build", "--project-dir", str(project)], scratch, "dbt build")


def main() -> None:
    """The ``analytics-check-client`` console script."""
    try:
        with tempfile.TemporaryDirectory(prefix="client-check-") as work:
            check_client(os.environ, Path(work))
    except CheckFailed as error:
        print(f"analytics-check-client: {error}", file=sys.stderr)
        raise SystemExit(1) from None
    print("analytics-check-client: the client project passed every check")
