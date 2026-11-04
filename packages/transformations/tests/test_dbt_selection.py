"""Declared dbt tests run only when their nodes or code change (ADR 0026)."""

import re
import shutil
from collections.abc import Iterator
from pathlib import Path
from typing import cast

import pytest

from tests import dbt_selection

ROOT = Path(__file__).parents[1]
SAMPLE = "tests/fixtures/selection"
LOOP_TEST = ROOT / "tests" / "test_loop_returns_staging.py"
ORDERS = "model.selection_fixture.stg_shop__orders"
# The tests select nodes of a sample project, so they run when the selection
# code or the sample changes, not when real models change (ADR 0026).
pytestmark = pytest.mark.dbt(files=(SAMPLE,))
_CACHED = (
    dbt_selection._manifest,  # pyright: ignore[reportPrivateUsage]
    dbt_selection._all,  # pyright: ignore[reportPrivateUsage]
    dbt_selection.selected,
    dbt_selection.modified,
)


def _clear() -> None:
    for cached in _CACHED:
        cached.cache_clear()


@pytest.fixture
def sample(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """Point selection at a copy of the sample project."""
    project = tmp_path / "current"
    _ = shutil.copytree(ROOT / SAMPLE, project)
    monkeypatch.setattr(dbt_selection, "PROJECT", project)
    _clear()
    yield project
    # Selection of the real project caches with the same keys.
    _clear()


@pytest.fixture
def base(sample: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A base checkout identical to the current package and sample project."""
    base = tmp_path / "base"
    _ = shutil.copytree(sample, base / "dbt")
    for folder in ("tests", "src"):
        _ = shutil.copytree(
            ROOT / folder,
            base / folder,
            ignore=shutil.ignore_patterns("target", "logs", "__pycache__"),
        )
    monkeypatch.setenv("DBT_CHECKS", "changed")
    monkeypatch.setenv("DBT_BASE_PROJECT", str(base))
    return base


def test_modes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DBT_CHECKS", "none")
    assert not dbt_selection.runs(LOOP_TEST, ("+stg_loop_returns__headers",))
    monkeypatch.setenv("DBT_CHECKS", "all")
    assert dbt_selection.runs(LOOP_TEST, ("+stg_loop_returns__headers",))
    monkeypatch.setenv("DBT_CHECKS", "some")
    with pytest.raises(ValueError, match="DBT_CHECKS"):
        _ = dbt_selection.mode()


def test_changed_node_runs_only_tests_that_select_it(base: Path) -> None:
    unchanged = ("+stg_shop__orders",)
    assert not dbt_selection.runs(LOOP_TEST, unchanged)
    model = base / "dbt/models/staging/stg_shop__orders.sql"
    _ = model.write_text(model.read_text() + "\n-- base revision\n")
    dbt_selection.modified.cache_clear()
    assert dbt_selection.runs(LOOP_TEST, unchanged)
    assert not dbt_selection.runs(LOOP_TEST, ("+stg_shop__returns",))


def test_changed_model_skips_tests_that_share_only_a_data_test(base: Path) -> None:
    """A data test across staging and marts does not tie staging tests to marts."""
    staging = ("+path:models/staging",)
    model = base / "dbt/models/marts/fct_orders.sql"
    _ = model.write_text(model.read_text() + "\n-- base revision\n")
    dbt_selection.modified.cache_clear()
    assert dbt_selection.runs(LOOP_TEST, ("+fct_orders",))
    assert not dbt_selection.runs(LOOP_TEST, staging)
    # A change to the shared data test itself runs both.
    check = base / "dbt/tests/fct_orders_reconcile_staging.sql"
    _ = check.write_text(check.read_text() + "\n-- base revision\n")
    dbt_selection.modified.cache_clear()
    assert dbt_selection.runs(LOOP_TEST, staging)


@pytest.mark.parametrize(
    "changed",
    [
        "tests/test_loop_returns_staging.py",
        "tests/conftest.py",
        # Imported by test_shopify_orders_marts through tests.model_build.
        "src/analytics_transformations/publication.py",
    ],
)
def test_changed_test_code_runs_its_tests(base: Path, changed: str) -> None:
    module = (
        LOOP_TEST
        if changed.startswith("tests/")
        else ROOT / "tests/test_shopify_orders_marts.py"
    )
    selectors = ("+stg_shop__orders",)
    assert not dbt_selection.runs(module, selectors)
    _ = (base / changed).write_text("# base revision\n")
    assert dbt_selection.runs(module, selectors)


@pytest.mark.parametrize("change", ["edit", "add", "remove"])
def test_changed_project_file_runs_tests_that_declare_it(
    base: Path, change: str
) -> None:
    """A test that copies package files, such as the macros, runs when they change."""
    files = (f"{SAMPLE}/models",)
    assert not dbt_selection.runs(LOOP_TEST, (), files)
    model = base / SAMPLE / "models/marts/fct_orders.sql"
    if change == "edit":
        _ = model.write_text(model.read_text() + "\n-- base revision\n")
    elif change == "add":
        model.unlink()
    else:
        _ = (model.parent / "removed.sql").write_text("-- removed\n")
    assert dbt_selection.runs(LOOP_TEST, (), files)
    assert not dbt_selection.runs(LOOP_TEST, (), (f"{SAMPLE}/tests",))


@pytest.mark.usefixtures("sample")
def test_undeclared_nodes_include_sources_and_ignore_other_projects() -> None:
    executed = {ORDERS, "model.publication_fixture.fct_observations"}
    assert not dbt_selection.undeclared(executed, ("+stg_shop__orders",))
    # A model's sources are part of what it executes.
    sources = dbt_selection.undeclared(executed, ("stg_shop__orders",))
    assert sources == {"source.selection_fixture.shop.orders"}
    assert ORDERS in dbt_selection.undeclared(executed, ())
    with pytest.raises(ValueError, match="choose no nodes"):
        _ = dbt_selection.selected(("missing_model",))


@pytest.mark.dbt("fqn:*", "source:*")
def test_selection_enables_every_optional_source() -> None:
    """A disabled model has no dependencies, so selection would miss changes."""
    flags = {
        flag
        for path in (ROOT / "dbt").rglob("*")
        if path.suffix in {".sql", ".yml"}
        for flag in cast(
            list[str], re.findall(r"env_var\('([A-Z_]+_ENABLED)'", path.read_text())
        )
    }
    assert flags
    assert {flag: dbt_selection.ENV.get(flag) for flag in flags} == dict.fromkeys(
        flags, "true"
    )
