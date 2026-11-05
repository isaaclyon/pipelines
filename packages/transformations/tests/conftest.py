"""Run dbt commands inside the test process.

Most tests start dbt several times. A new dbt process spends about a second on
imports before it reads the project, so the suite sends ``dbt`` commands given
to ``subprocess.run`` to ``dbtRunner`` in this process instead. Each command
sees the environment it was given, and its log output becomes ``stdout``.
In-process commands ignore ``timeout``.

Parsing the project takes most of a small command's time, and most tests parse
the same project with the same settings. An in-process command therefore reuses
the manifest that an earlier command parsed when the project files, the
parse-relevant arguments, and the environment all match. Each test uses its own
DuckDB file, so only that file's name, which names the database, takes part in
the match. The reused manifest is written to the target like a fresh one, and
the first command for each combination still parses.

Writing artifacts takes much of the rest: ``json.dump`` encodes in pure Python,
so the suite writes dbt's JSON artifacts with the identical, C-encoded
``json.dumps``. dbt usage tracking is off, so commands do not wait on the network.

``analytics-dbt`` commands run the same way, with the environment its
``dbt_env`` derives from the client configuration, so they see what the wrapper
would pass to dbt. Tests that choose no client use the package check's client.

Tests marked ``dbt_subprocess`` start real dbt processes, so the production
subprocess path stays covered.
"""

from __future__ import annotations

import contextlib
import dataclasses
import gc
import io
import json
import os
import pickle
import subprocess
from collections import OrderedDict
from collections.abc import Callable, Generator, Iterator, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import dbt.artifacts.schemas.base
import dbt.artifacts.schemas.run.v5.run
import pytest
from analytics_client_config import ConfigError
from dbt.adapters.duckdb.connections import DuckDBConnectionManager
from dbt.adapters.factory import get_adapter
from dbt.artifacts.schemas.freshness import FreshnessResult
from dbt.artifacts.schemas.run import RunExecutionResult
from dbt.cli.main import dbtRunner
from dbt.context.query_header import generate_query_header_context
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import SqlNode
from dbt.parser.manifest import ManifestLoader
from dbt_common.clients.system import make_directory
from dbt_common.events.functions import get_metadata_vars
from dbt_common.invocation import get_invocation_id, get_invocation_started_at
from dbt_common.utils.encoding import JSONEncoder

from analytics_transformations.client_dbt import dbt_env
from tests import dbt_selection
from tests.client_files import CHECK_CLIENT
from tests.project_build import ProjectBuild, build_project

Output = str | bytes | None
Completed = subprocess.CompletedProcess[Output]
Run = Callable[..., Completed]
Key = tuple[object, ...]
_REAL_RUN = cast(Run, subprocess.run)
_subprocess_test = False
_executed = dbt_selection.executed

# Commands whose parse step depends only on the project, arguments, and
# environment. Docs, deps, and debug always run in full.
_PARSING_COMMANDS = {
    "build",
    "source",
    "compile",
    "parse",
    "run",
    "run-operation",
    "seed",
    "show",
    "snapshot",
    "test",
}
# Options applied after parsing, mapped to whether they take several values.
_UNPARSED_OPTIONS = {
    "--select": True,
    "-s": True,
    "--exclude": True,
    "--selector": False,
    "--indirect-selection": False,
    "--inline": False,
    "--target-path": False,
    "--log-path": False,
}
_UNPARSED_ENV = {"PYTEST_CURRENT_TEST", "DBT_LOG_PATH", "DBT_TARGET_PATH"}
_MANIFEST_LIMIT = 32
_manifests: OrderedDict[Key, bytes] = OrderedDict()
_reused: bytes | None = None
_parsed: list[bytes] = []
_REAL_FULL_MANIFEST = cast(
    Callable[..., Manifest],
    ManifestLoader.__dict__["get_full_manifest"].__func__,
)


def _full_manifest(
    cls: type[ManifestLoader], config: object, **options: object
) -> Manifest:
    if _reused is None:
        manifest = _REAL_FULL_MANIFEST(cls, config, **options)
        # Execution adds compiled code to the manifest; keep the parser's output.
        _parsed.append(pickle.dumps(manifest))
        return manifest
    manifest = cast(Manifest, pickle.loads(_reused))
    # Artifacts name the invocation that wrote them; keep a reused manifest's
    # invocation consistent with the run results written beside it.
    manifest.metadata = dataclasses.replace(
        manifest.metadata,
        generated_at=datetime.now(UTC).replace(tzinfo=None),
        invocation_id=get_invocation_id(),
        invocation_started_at=get_invocation_started_at(),
        env=get_metadata_vars(),
    )
    # The rest of what the parser does after loading the project.
    adapter = get_adapter(config)  # pyright: ignore[reportArgumentType, reportUnknownVariableType] # dbt's adapter factory is untyped.
    adapter.set_macro_resolver(manifest)
    adapter.connections.set_query_header(  # pyright: ignore[reportUnknownMemberType]
        generate_query_header_context(adapter.config, manifest)  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType, reportAttributeAccessIssue]
    )
    return manifest


def _files(directory: Path) -> tuple[tuple[str, int, int], ...]:
    found: list[tuple[str, int, int]] = []
    for root, dirs, names in os.walk(directory):
        dirs[:] = sorted(d for d in dirs if d not in {"target", "logs"})
        for name in sorted(names):
            path = Path(root) / name
            stat = path.stat()
            found.append((str(path), stat.st_mtime_ns, stat.st_size))
    return tuple(found)


def _manifest_key(arguments: list[str], env: Mapping[str, str]) -> Key | None:
    """Everything that can change what dbt parses, or None to always parse."""
    command = next((part for part in arguments if not part.startswith("-")), None)
    if command not in _PARSING_COMMANDS:
        return None
    kept: list[str] = []
    index = 0
    while index < len(arguments):
        part = arguments[index]
        index += 1
        if part == command:
            continue
        if part not in _UNPARSED_OPTIONS:
            kept.append(part)
            continue
        index += 1
        while (
            _UNPARSED_OPTIONS[part]
            and index < len(arguments)
            and not arguments[index].startswith("-")
        ):
            index += 1

    def directory(option: str, variable: str) -> Path:
        if option in kept[:-1]:
            return Path(kept[kept.index(option) + 1]).resolve()
        return Path(env.get(variable, ".")).resolve()

    variables = tuple(
        sorted(
            (name, Path(value).name if name == "DUCKDB_PATH" else value)
            for name, value in env.items()
            if name not in _UNPARSED_ENV
        )
    )
    return (
        tuple(kept),
        variables,
        _files(directory("--project-dir", "DBT_PROJECT_DIR")),
        _files(directory("--profiles-dir", "DBT_PROFILES_DIR")),
    )


# Real dbt processes that tests start inherit this; in-process commands set it.
_NO_TRACKING = {"DBT_SEND_ANONYMOUS_USAGE_STATS": "false"}
os.environ.update(_NO_TRACKING)
_ = os.environ.setdefault("ANALYTICS_CLIENT_CONFIG", str(CHECK_CLIENT))
_DBT_NAMES = {"dbt", "analytics-dbt"}


def _write_json(path: str, data: object) -> bool:
    make_directory(os.path.dirname(path))
    _ = Path(path).write_text(json.dumps(data, cls=JSONEncoder), encoding="utf-8")
    return True


@pytest.fixture(autouse=True, scope="session")
def fast_artifacts() -> Iterator[None]:
    with pytest.MonkeyPatch.context() as patch:
        for module in (dbt.artifacts.schemas.base, dbt.artifacts.schemas.run.v5.run):
            patch.setattr(module, "write_json", _write_json)
        yield


@pytest.fixture(autouse=True, scope="session")
def reuse_manifests() -> Iterator[None]:
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(ManifestLoader, "get_full_manifest", classmethod(_full_manifest))
        yield


def _dbt_command(command: object) -> list[str] | None:
    if not isinstance(command, (list, tuple)):
        return None
    parts = [str(part) for part in cast(Sequence[object], command)]
    if not parts or Path(parts[0]).name not in _DBT_NAMES:
        return None
    return parts


def _outputs(text: str, options: Mapping[str, object]) -> tuple[Output, Output]:
    """Captured ``stdout`` and ``stderr``, in the form ``options`` requested."""
    captured = options.get("capture_output") or options.get("stdout") == (
        subprocess.PIPE
    )
    if not captured:
        return None, None
    if options.get("text") or options.get("universal_newlines"):
        return text, ""
    return text.encode(), b""


def _refused(
    command: list[str], options: Mapping[str, object], message: str
) -> Completed:
    """The wrapper's own refusal: it prints to stderr and exits 2."""
    stdout, stderr = _outputs("", options)
    if stderr is not None:
        stderr = message if isinstance(stderr, str) else message.encode()
    if options.get("check"):
        raise subprocess.CalledProcessError(2, command, stdout, stderr)
    return subprocess.CompletedProcess(command, 2, stdout, stderr)


def _invoke(command: list[str], options: Mapping[str, object]) -> Completed:
    global _reused
    env = cast(Mapping[str, str] | None, options.get("env"))
    if Path(command[0]).name == "analytics-dbt":
        try:
            env = dbt_env(os.environ if env is None else env)
        except ConfigError as error:
            return _refused(command, options, f"analytics-dbt: {error}\n")
    saved = dict(os.environ)
    if env is not None:
        os.environ.clear()
        os.environ.update(env)
    os.environ.update(_NO_TRACKING)
    output = io.StringIO()
    key = _manifest_key(command[1:], os.environ)
    _reused = _manifests.get(key) if key is not None else None
    _parsed.clear()
    try:
        with contextlib.redirect_stdout(output):
            # dbt leaves the keyword arguments of invoke untyped.
            result = dbtRunner().invoke(command[1:])  # pyright: ignore[reportUnknownMemberType]
    finally:
        if key is not None:
            if _reused is None and len(_parsed) == 1:
                _manifests[key] = _parsed[0]
            if key in _manifests:
                _manifests.move_to_end(key)
            while len(_manifests) > _MANIFEST_LIMIT:
                _ = _manifests.popitem(last=False)
        _reused = None
        _parsed.clear()
        # dbt-duckdb keeps the database open between invocations; a later
        # command may attach the same file under another name.
        DuckDBConnectionManager.close_all_connections()
        os.environ.clear()
        os.environ.update(saved)
    if isinstance(result.result, RunExecutionResult):
        for executed in result.result.results:
            _executed.add(executed.node.unique_id)
            # Inline queries execute the nodes they reference.
            if isinstance(executed.node, SqlNode):
                _executed.update(executed.node.depends_on.nodes)
    elif isinstance(result.result, FreshnessResult):
        _executed.update(source.node.unique_id for source in result.result.results)
    code = 0 if result.success else 2 if result.exception is not None else 1
    stdout, stderr = _outputs(output.getvalue(), options)
    if options.get("check") and code:
        raise subprocess.CalledProcessError(code, command, stdout)
    return subprocess.CompletedProcess(command, code, stdout, stderr)


def _run(*args: object, **options: object) -> Completed:
    command = _dbt_command(args[0] if args else options.get("args"))
    if command is None or _subprocess_test:
        return _REAL_RUN(*args, **options)
    return _invoke(command, options)


@pytest.fixture(autouse=True, scope="session")
def in_process_dbt() -> Iterator[None]:
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(subprocess, "run", _run)
        yield


@pytest.fixture(autouse=True)
def dbt_subprocess(request: pytest.FixtureRequest) -> Iterator[None]:
    global _subprocess_test
    test = cast(pytest.Item, request.node)
    _subprocess_test = test.get_closest_marker("dbt_subprocess") is not None
    try:
        yield
    finally:
        _subprocess_test = False


Declaration = tuple[tuple[str, ...], tuple[str, ...]]


def _declared(item: pytest.Item) -> Declaration | None:
    """The dbt selectors and project files a test declares, if it declares any."""
    marker = item.get_closest_marker("dbt")
    if marker is None:
        return None
    selectors = cast(tuple[object, ...], marker.args)
    files = cast(object, marker.kwargs.get("files", ()))
    if not isinstance(files, tuple) or set(marker.kwargs) - {"files"}:
        raise TypeError(f"{item.nodeid}: pytest.mark.dbt takes files=(path, ...)")
    declared = (*selectors, *cast(tuple[object, ...], files))
    if not declared or not all(isinstance(value, str) for value in declared):
        raise TypeError(f"{item.nodeid}: pytest.mark.dbt needs dbt selector strings")
    return cast(Declaration, (selectors, files))


def pytest_collection_finish(session: pytest.Session) -> None:
    """Keep garbage collection from rescanning objects that live all session.

    Imports and the manifests that change selection parsed hold millions of
    objects, and dbt commands allocate enough to trigger frequent full scans.
    """
    _ = gc.collect()
    gc.freeze()
    gc.set_threshold(50_000, 20, 20)


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    """Deselect declared dbt tests that the change cannot affect (ADR 0026)."""
    kept: list[pytest.Item] = []
    dropped: list[pytest.Item] = []
    # Tests of a module usually share a declaration; decide each pair once.
    decided: dict[tuple[Path, Declaration], bool] = {}
    for item in items:
        declared = _declared(item)
        if declared is None:
            runs = True
        else:
            key = (item.path, declared)
            if key not in decided:
                decided[key] = dbt_selection.runs(item.path, *declared)
            runs = decided[key]
        (kept if runs else dropped).append(item)
    if dropped:
        config.hook.pytest_deselected(items=dropped)
        items[:] = kept


@pytest.hookimpl(wrapper=True)
def pytest_runtest_protocol(item: pytest.Item) -> Generator[None, object, object]:
    # Module fixtures build during the first test's setup; count them there.
    _executed.clear()
    result = yield
    return result


@pytest.fixture(scope="session")
def project_build(tmp_path_factory: pytest.TempPathFactory) -> ProjectBuild:
    """Build the real project once per test process (ADR 0026)."""
    with dbt_selection.unaccounted():
        return build_project(tmp_path_factory.mktemp("project"))


@pytest.fixture(autouse=True)
def declared_dbt_nodes(request: pytest.FixtureRequest) -> Iterator[None]:
    """Fail a test that executes real-project nodes it does not declare."""
    yield
    selectors, _ = _declared(cast(pytest.Item, request.node)) or ((), ())
    undeclared = sorted(dbt_selection.undeclared(set(_executed), selectors))
    if undeclared:
        pytest.fail(
            f"Executed real dbt project nodes outside pytest.mark.dbt{selectors}: "
            + ", ".join(undeclared[:5])
            + (f" and {len(undeclared) - 5} more" if len(undeclared) > 5 else "")
        )
