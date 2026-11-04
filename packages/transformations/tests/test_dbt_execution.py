"""The publication boundary must not hide the candidate graph or failures."""

import os
from functools import cache
from pathlib import Path

import dagster as dg
import pytest

from analytics_transformations.defs import development_publication

# Sample dbt projects with copies of the real macros; tests that read the real
# project's definitions declare all of it (ADR 0026).
pytestmark = pytest.mark.dbt(files=("dbt/macros",))
WHOLE_PROJECT = pytest.mark.dbt("fqn:*", "source:*")


@cache
def _publication() -> dg.AssetsDefinition:
    return development_publication.defs().resolve_assets_def(
        development_publication.PUBLICATION
    )


@WHOLE_PROJECT
def test_publication_exposes_executable_models_and_checks() -> None:
    assets = _publication()
    assert dg.AssetKey(["fct_orders"]) in assets.keys
    assert dg.AssetKey(["stg_shopify_orders__orders"]) in assets.keys
    assert development_publication.PUBLICATION in assets.keys
    assert not assets.can_subset
    for spec in assets.specs:
        assert "dagster/table_name" not in spec.metadata
        assert "dagster/storage_kind" not in spec.metadata
        assert "duckdb" not in spec.kinds
    assert list(assets.check_specs)
    assert (
        dg.AssetKey(["fct_orders"])
        in assets.asset_deps[development_publication.PUBLICATION]
    )


def test_lineage_has_an_independent_locked_environment() -> None:
    root = Path(__file__).parents[1]
    assert (root / "lineage" / "uv.lock").is_file()
    assert '"dbt-colibri==0.3.7"' not in (root / "pyproject.toml").read_text()


@pytest.mark.parametrize("failure", ["test", "model", "timeout"])
@pytest.mark.parametrize("group", [None, "orders"])
def test_candidate_execution_records_checks_and_retains_failed_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    failure: str,
    group: str | None,
) -> None:
    import shutil
    from collections.abc import Iterator, Mapping, Sequence

    import duckdb

    from analytics_transformations.dbt_execution import (
        CandidateRunner,
        Event,
        model_definition,
        project_manifest,
    )
    from analytics_transformations.publication import (
        build_candidate,
        publication_manifest,
    )

    root = Path(__file__).parents[1]
    project = tmp_path / "project"
    shutil.copytree(root / "tests/fixtures/publication", project)
    shutil.copytree(root / "dbt/macros", project / "macros")
    reconciliation = project / "tests/observation_reconciliation.sql"
    reconciliation.write_text(
        "{{ config(meta={'dagster': {'ref': {'name': 'fct_observations'}}}) }}\n"
        + reconciliation.read_text()
    )
    if group:
        mart = project / "models/marts/fct_observations.sql"
        mart.write_text(
            "{{ config(meta={'publication_group': 'orders'}) }}\n" + mart.read_text()
        )
    profiles = root / "dbt"
    monkeypatch.setenv("DBT_TARGET_PATH", str(tmp_path / "artifacts"))
    models = model_definition(
        publication_manifest(project_manifest(project, profiles, os.environ), group)
    )
    attempts: list[Path] = []

    @dg.multi_asset(specs=list(models.specs), check_specs=list(models.check_specs))
    def candidate(context: dg.AssetExecutionContext) -> Iterator[Event]:
        # These are disposable model builds, not an offline publication backend.
        artifacts = tmp_path / f"attempt-{len(attempts)}"
        artifacts.mkdir(mode=0o700)
        attempts.append(artifacts)
        database = artifacts / "fixture.duckdb"
        with duckdb.connect(str(database)):
            pass
        env = dict(os.environ)
        env.update(
            DBT_TARGET="local",
            DBT_NAMESPACE="test_run",
            DUCKDB_PATH=str(database),
            DBT_PUBLICATION_ID="deterministic-fixture",
        )
        runner = CandidateRunner(context)

        def run_command(
            command: Sequence[str], env: Mapping[str, str], timeout: int
        ) -> None:
            if failure == "timeout" and len(attempts) == 2 and command[1] == "build":
                timeout = 0
            runner(command, env, timeout)

        try:
            build_candidate(
                project,
                profiles,
                artifacts,
                env,
                240,
                run_command,
                publication_group=group,
            )
        finally:
            yield from runner.events

    first = dg.materialize([candidate])
    assert first.success
    assert {
        event.asset_key for event in first.get_asset_materialization_events()
    } == models.keys
    assert first.get_asset_check_evaluations()
    model = project / "models/staging/stg_fixture__observations.sql"
    if failure == "test":
        model.write_text(
            model.read_text().replace("var('amount', 100)", "var('amount', -1)")
        )
    elif failure == "model":
        model.write_text(
            "select cast('missing_private_value' as integer) as observation_id"
        )
    failed = dg.materialize([candidate], raise_on_error=False)
    assert not failed.success
    if failure == "test":
        assert any(not check.passed for check in failed.get_asset_check_evaluations())
    else:
        assert "missing_private" not in str(failed.all_events)
        assert "missing_private" not in str(capsys.readouterr())
    builds = [attempt / "build" for attempt in attempts]
    assert len(builds) == 2
    for build in builds:
        if failure != "timeout":
            assert (build / "run_results.json").is_file()
            assert (build / "manifest.json").is_file()
        assert (build / "console.log").is_file()
        assert build.stat().st_mode & 0o777 == 0o700


@pytest.mark.parametrize("timeout", [False, True])
def test_command_failure_keeps_raw_diagnostics_out_of_exception(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    timeout: bool,
) -> None:
    import subprocess
    from typing import TextIO

    from analytics_transformations.publication import run_dbt

    target = tmp_path / "artifacts"
    secret = "private-row-and-credential-fixture"

    def fail(command: list[str], *, stdout: TextIO, **_: object) -> None:
        stdout.write(secret)
        if timeout:
            raise subprocess.TimeoutExpired(command, 1, output=secret)
        raise subprocess.CalledProcessError(1, command, output=secret)

    monkeypatch.setattr(subprocess, "run", fail)
    with pytest.raises(RuntimeError) as error:
        run_dbt(["dbt", "parse", "--target-path", str(target)], {}, 1)
    assert secret not in str(error.value)
    assert str(target) in str(error.value)
    assert secret not in str(capsys.readouterr())
    assert (target / "console.log").read_text() == secret
    assert target.stat().st_mode & 0o777 == 0o700


@WHOLE_PROJECT
def test_enabled_model_families_appear_in_definitions(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from analytics_transformations.dbt_execution import (
        model_definition,
        project_manifest,
    )

    project = Path(__file__).parents[1] / "dbt"
    monkeypatch.setenv("DBT_TARGET_PATH", str(tmp_path))
    monkeypatch.setenv("SHOPIFY_CUSTOMERS_ENABLED", "true")
    monkeypatch.setenv("GOODDAY_INVENTORY_ENABLED", "true")
    models = model_definition(project_manifest(project, project, os.environ))
    assert dg.AssetKey("dim_customers") in models.keys
    assert dg.AssetKey("fct_inventory__current") in models.keys
    assert (
        dg.AssetKey(["raw", "goodday", "goodday_inventory_raw"])
        in models.asset_deps[dg.AssetKey("stg_goodday__inventory_items")]
    )


def test_unexpected_model_output_fails_before_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from collections.abc import Iterator

    from analytics_transformations.dbt_execution import CandidateRunner, Event

    # Exercise the real event consumer with a small integration-event fixture.
    # The runner must reject an output missing from the loaded graph before the
    # enclosing publisher can perform its swap.
    class Process:
        def kill(self) -> None:
            pass

        def poll(self) -> int:
            return 0

        def wait(self, timeout: int) -> int:
            return 0

    class Message:
        raw_event: dict[str, object] = {"data": {}}

        def to_default_asset_events(self, **_: object) -> Iterator[dg.Output[None]]:
            yield dg.Output(None, output_name="unexpected_model")

    class Invocation:
        process = Process()

        def stream_raw_events(self) -> Iterator[Message]:
            yield Message()

        def is_successful(self) -> bool:
            return True

    class Resource:
        def __init__(self, **_: object) -> None:
            pass

        def cli(self, *_: object, **__: object) -> Invocation:
            return Invocation()

    monkeypatch.setattr(
        "analytics_transformations.dbt_execution.DbtCliResource", Resource
    )
    parsed = tmp_path / "parse"
    parsed.mkdir()
    (parsed / "manifest.json").write_text('{"nodes": {}}')
    promoted: list[bool] = []

    @dg.multi_asset(specs=[dg.AssetSpec("expected_model")])
    def candidate(context: dg.AssetExecutionContext) -> Iterator[Event]:
        runner = CandidateRunner(context)
        runner(
            [
                "dbt",
                "build",
                "--project-dir",
                str(tmp_path),
                "--profiles-dir",
                str(tmp_path),
                "--target-path",
                str(tmp_path / "build"),
            ],
            {},
            60,
        )
        promoted.append(True)
        yield from runner.events

    result = dg.materialize([candidate], raise_on_error=False)
    assert not result.success
    assert promoted == []


@WHOLE_PROJECT
def test_published_reconciliations_are_asset_checks() -> None:
    checks = {spec.name for spec in _publication().check_specs}
    assert {
        "fct_orders_reconcile_staging",
        "fct_order_lines_match_staging",
        "shopify_published_commercial_reconcile",
        "shopify_published_net_units_reconcile",
        "shopify_published_revenue_eligibility",
        "data_availability_consistent",
    } <= checks
