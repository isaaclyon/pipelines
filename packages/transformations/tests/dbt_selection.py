"""Choose which real-project dbt tests run for a change (ADR 0026).

A test module that executes nodes of the real dbt project declares them with
``pytestmark = pytest.mark.dbt(<selector>, ...)``, using dbt selector syntax.
The suite fails a test that executes real-project nodes outside its
declaration. ``DBT_CHECKS`` decides which declared tests run:

- ``all`` (default): every test.
- ``none``: no declared test; the change cannot affect dbt results.
- ``changed``: declared tests whose module or declared files changed, or
  whose selection includes a node that differs from the base project in
  ``DBT_BASE_PROJECT`` (``state:modified``).

A test that only compiles or reads model SQL may declare those models too.
A test that executes a sample project, but copies real project files such as
the macros, declares those paths instead:
``pytest.mark.dbt(files=("dbt/macros",))``. It may execute no real-project
node. Tests without a declaration always run. Selectors are
evaluated with every optional source enabled, so a node is known whatever a
test enables.

The shared project build (``tests.project_build``) builds every node once per
test process, outside any test's account. A test that reads it declares the
models it reads with their ancestors, such as ``+fct_orders``; the suite
cannot check reads, so the full run on ``main`` backs those declarations.
"""

from __future__ import annotations

import ast
import contextlib
import io
import json
import os
import tempfile
from collections.abc import Generator
from functools import cache
from pathlib import Path
from typing import cast

from dbt.cli.main import dbtRunner
from dbt.contracts.graph.manifest import Manifest

from analytics_transformations.client_dbt import dbt_env
from analytics_transformations.layer_dependencies import TOOLKIT_PROJECT
from tests.client_files import ALL_SOURCES

ROOT = Path(__file__).parents[1]
PROJECT = ROOT / "dbt"
PROJECT_NAME = TOOLKIT_PROJECT
MODES = ("all", "none", "changed")
# Selection and state comparison need every source enabled.
ENV = dbt_env(
    {
        "ANALYTICS_CLIENT_CONFIG": str(ALL_SOURCES),
        "DBT_TARGET": "local",
        "DBT_NAMESPACE": "ci_selection",
        "ANALYTICS_ENVIRONMENT": "local",
    }
)


# Real-project nodes that dbt executed for the running test.
executed: set[str] = set()


@contextlib.contextmanager
def unaccounted() -> Generator[None]:
    """Leave nodes that a shared fixture executes out of the running test's account."""
    saved = set(executed)
    try:
        yield
    finally:
        executed.clear()
        executed.update(saved)


def mode() -> str:
    value = os.environ.get("DBT_CHECKS", "all")
    if value not in MODES:
        raise ValueError(f"DBT_CHECKS must be one of {MODES}, not {value!r}")
    return value


@cache
def _directory() -> Path:
    return Path(tempfile.mkdtemp(prefix="dbt-selection-"))


@contextlib.contextmanager
def _environment(target: Path) -> Generator[None]:
    saved = dict(os.environ)
    os.environ.update(ENV | {"DUCKDB_PATH": str(target / "selection.duckdb")})
    try:
        # dbt ls prints each node even when quiet.
        with contextlib.redirect_stdout(io.StringIO()):
            yield
    finally:
        os.environ.clear()
        os.environ.update(saved)


def _arguments(project: Path, target: Path) -> list[str]:
    return [
        "--project-dir",
        str(project),
        "--profiles-dir",
        str(PROJECT),
        "--target-path",
        str(target),
        "--log-path",
        str(target / "logs"),
        "--quiet",
        "--no-partial-parse",
    ]


def _parse(project: Path, target: Path) -> Manifest:
    with _environment(target):
        # dbt leaves the keyword arguments of invoke untyped.
        result = dbtRunner().invoke(["parse", *_arguments(project, target)])  # pyright: ignore[reportUnknownMemberType]
    if not result.success or not isinstance(result.result, Manifest):
        raise RuntimeError(f"dbt could not parse {project}: {result.exception}")
    return result.result


@cache
def _manifest() -> Manifest:
    return _parse(PROJECT, _directory() / "current")


def _ls(*arguments: str) -> frozenset[str]:
    target = _directory() / "current"
    with _environment(target):
        result = dbtRunner(manifest=_manifest()).invoke(  # pyright: ignore[reportUnknownMemberType]
            [
                "ls",
                *_arguments(PROJECT, target),
                "--resource-types",
                "all",
                "--output",
                "json",
                "--output-keys",
                "unique_id",
                *arguments,
            ]
        )
    if not result.success:
        raise RuntimeError(f"dbt ls {arguments} failed: {result.exception}")
    lines = cast(list[str], result.result)
    return frozenset(
        str(cast(dict[str, object], json.loads(line))["unique_id"]) for line in lines
    )


@cache
def selected(selectors: tuple[str, ...]) -> frozenset[str]:
    """Real-project nodes that the selectors choose, as unique IDs."""
    nodes = _ls("--select", *selectors)
    if not nodes:
        raise ValueError(f"dbt selectors {selectors} choose no nodes")
    return nodes


@cache
def _all() -> frozenset[str]:
    return _ls()


@cache
def modified() -> frozenset[str]:
    """Nodes that differ from the base project.

    Data tests of a changed model are not themselves changed: a test that spans
    staging and marts would otherwise tie every staging test to the marts.
    """
    base = Path(os.environ["DBT_BASE_PROJECT"])
    state = _directory() / "base"
    _ = _parse(base / "dbt", state)
    return _ls(
        "--select",
        "state:modified",
        "--state",
        str(state),
        "--indirect-selection",
        "empty",
    )


# Local code a test can import; dbt never imports it, so imports find every use.
_LOCAL_PACKAGES = {"tests": ROOT, "analytics_transformations": ROOT / "src"}


def _local(name: str) -> Path | None:
    if name.split(".")[0] not in _LOCAL_PACKAGES:
        return None
    path = _LOCAL_PACKAGES[name.split(".")[0]].joinpath(*name.split("."))
    for candidate in (path.with_suffix(".py"), path / "__init__.py"):
        if candidate.is_file():
            return candidate
    return None


def _imports(path: Path) -> set[Path]:
    """Local modules a test module imports, transitively, with conftest."""
    found = {path, ROOT / "tests" / "conftest.py"}
    pending = [path]
    while pending:
        current = pending.pop()
        tree = ast.parse(current.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                if node.level:
                    package = current.parents[node.level - 1]
                    root = ROOT / "src" if "src" in current.parts else ROOT
                    module = ".".join(
                        [*package.relative_to(root).parts, *filter(None, [module])]
                    )
                # ``from package import module`` may name modules or attributes.
                names = [module] + [f"{module}.{alias.name}" for alias in node.names]
            else:
                continue
            for name in names:
                imported = _local(name)
                if imported is not None and imported not in found:
                    found.add(imported)
                    pending.append(imported)
    return found


def module_changed(path: Path) -> bool:
    """Whether a test module, or test code it imports, differs from the base."""
    base = Path(os.environ["DBT_BASE_PROJECT"])
    for current in _imports(path):
        previous = base / current.relative_to(ROOT)
        if not previous.is_file() or previous.read_bytes() != current.read_bytes():
            return True
    return False


def files_changed(paths: tuple[str, ...]) -> bool:
    """Whether package files under these paths differ from the base."""
    base = Path(os.environ["DBT_BASE_PROJECT"])
    for path in paths:
        current = {item.relative_to(ROOT) for item in (ROOT / path).rglob("*")}
        previous = {item.relative_to(base) for item in (base / path).rglob("*")}
        if current != previous:
            return True
        for item in current:
            if (ROOT / item).is_file() and (
                (ROOT / item).read_bytes() != (base / item).read_bytes()
            ):
                return True
    return False


def runs(path: Path, selectors: tuple[str, ...], files: tuple[str, ...] = ()) -> bool:
    """Whether a declared test runs under the current mode."""
    current = mode()
    if current == "all":
        return True
    if current == "none":
        return False
    return (
        module_changed(path)
        or files_changed(files)
        or (bool(selectors) and bool(selected(selectors) & modified()))
    )


def _footprint(executed: set[str]) -> set[str]:
    """Executed nodes with the sources and ephemeral models compiled into them.

    ``state:modified`` does not mark a model whose source or ephemeral parent
    changed, so a declaration must cover those parents too.
    """
    nodes = _manifest().nodes
    found: set[str] = set()
    pending = list(executed)
    while pending:
        unique_id = pending.pop()
        if unique_id in found:
            continue
        found.add(unique_id)
        node = nodes.get(unique_id)
        if node is None:
            continue
        parents: list[str] = getattr(node.depends_on, "nodes", [])
        pending.extend(
            parent
            for parent in parents
            if parent.startswith("source.")
            or (parent in nodes and nodes[parent].config.materialized == "ephemeral")
        )
    return found


def undeclared(executed: set[str], selectors: tuple[str, ...]) -> set[str]:
    """Executed real-project nodes outside a test's declared selection."""
    if not executed:
        return set()
    nodes = _footprint(executed) & _all()
    return nodes - selected(selectors) if selectors else nodes
