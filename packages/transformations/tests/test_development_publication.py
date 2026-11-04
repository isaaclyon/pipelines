import os
from collections.abc import Iterator
from datetime import UTC, datetime
from functools import cache
from pathlib import Path

import dagster as dg
import pytest

from analytics_transformations.dbt_execution import CandidateRunner
from analytics_transformations.defs import development_publication as module
from analytics_transformations.publication import PublicationResult


@cache
def _defs() -> dg.Definitions:
    """The code location's definitions, loaded once with the session's settings.

    A test that changes settings loads them first, as a running location would.
    """
    return module.defs()


def test_settings_derive_the_namespace_from_the_cube_schema() -> None:
    env = {
        "ANALYTICS_MARTS_SCHEMA": "development_marts",
        "ANALYTICS_MARTS_READER_ROLE": "DEV_MARTS_READER_ROLE",
    }
    assert module.publication_settings(env) == (
        "development",
        "DEV_MARTS_READER_ROLE",
    )


@pytest.mark.parametrize(
    "env",
    [
        {"ANALYTICS_MARTS_READER_ROLE": "reader"},
        {"ANALYTICS_MARTS_SCHEMA": "development", "ANALYTICS_MARTS_READER_ROLE": "r"},
        {"ANALYTICS_MARTS_SCHEMA": "_marts", "ANALYTICS_MARTS_READER_ROLE": "r"},
        {"ANALYTICS_MARTS_SCHEMA": "development_marts"},
        {
            "ANALYTICS_MARTS_SCHEMA": "development_marts",
            "ANALYTICS_MARTS_READER_ROLE": "",
        },
    ],
    ids=["no-schema", "no-suffix", "no-namespace", "no-reader", "blank-reader"],
)
def test_settings_fail_closed(env: dict[str, str]) -> None:
    with pytest.raises(ValueError):
        _ = module.publication_settings(env)


def _run(status: dg.DagsterRunStatus, job: str, **tags: str) -> dg.DagsterRun:
    return dg.DagsterRun(job_name=job, status=status, tags=tags)


@pytest.fixture
def instance() -> Iterator[dg.DagsterInstance]:
    # Dagster's optional settings argument is untyped.
    with dg.DagsterInstance.ephemeral() as value:  # pyright: ignore[reportUnknownMemberType]
        yield value


ALL_ENABLED = {
    "SHOPIFY_CUSTOMERS_ENABLED": "true",
    "LOOP_RETURNS_ENABLED": "true",
    "GOODDAY_INVENTORY_ENABLED": "true",
}


@pytest.mark.parametrize(
    ("job", "group"),
    [
        ("shopify_orders_dlt_load", "orders"),
        ("shopify_orders_reconcile_counts", "orders"),
        ("shopify_customers_dlt_load", "customers"),
        ("shopify_customers_reconcile", "customers"),
        ("loop_returns_dlt_snapshot", "orders"),
        ("goodday_inventory_capture", "inventory"),
    ],
)
def test_each_input_independently_requests_publication(
    instance: dg.DagsterInstance,
    job: str,
    group: str,
) -> None:
    run = _run(dg.DagsterRunStatus.SUCCESS, job)
    request = module.publication_request(
        instance, run, module.publication_inputs(ALL_ENABLED, group), group
    )
    assert isinstance(request, dg.RunRequest)
    assert request.run_key == f"{module.GROUP_SETTINGS[group][0]}:{run.run_id}"


@pytest.mark.parametrize(
    "job",
    [
        "shopify_customers_dlt_load",
        "loop_returns_dlt_snapshot",
        "goodday_inventory_capture",
        "unrelated_job",
    ],
)
def test_disabled_or_unrelated_inputs_do_not_trigger(
    instance: dg.DagsterInstance,
    job: str,
) -> None:
    run = _run(dg.DagsterRunStatus.SUCCESS, job)
    assert isinstance(
        module.publication_request(instance, run, module.publication_inputs({})),
        dg.SkipReason,
    )


@pytest.mark.parametrize(
    "status", [dg.DagsterRunStatus.FAILURE, dg.DagsterRunStatus.STARTED]
)
def test_unsuccessful_input_does_not_trigger(
    instance: dg.DagsterInstance, status: dg.DagsterRunStatus
) -> None:
    run = _run(status, "shopify_orders_dlt_load")
    assert isinstance(
        module.publication_request(instance, run, module.publication_inputs({})),
        dg.SkipReason,
    )


@pytest.mark.parametrize(
    ("status", "requested"),
    [
        (dg.DagsterRunStatus.NOT_STARTED, False),
        # STARTING may already have acquired its input locks: never coalesce it.
        (dg.DagsterRunStatus.STARTING, True),
        (dg.DagsterRunStatus.STARTED, True),
        (dg.DagsterRunStatus.FAILURE, True),
    ],
)
def test_only_waiting_publication_covers_later_inputs(
    instance: dg.DagsterInstance, status: dg.DagsterRunStatus, requested: bool
) -> None:
    instance.add_run(_run(status, module.JOB_NAME))
    refresh = _run(dg.DagsterRunStatus.SUCCESS, "shopify_orders_dlt_load")
    request = module.publication_request(
        instance, refresh, module.publication_inputs({})
    )
    assert isinstance(request, dg.RunRequest) is requested


def test_graph_sensor_and_locks_share_enabled_inputs() -> None:
    definitions = module.build_definitions({**os.environ, **ALL_ENABLED})
    job = definitions.resolve_job_def(module.JOB_NAME)
    assert job.tags["dagster/max_retries"] == "0"
    assert job.tags["analytics/publication-input"] == "development-marts"
    graph = definitions.resolve_asset_graph()
    assert graph.get(module.PUBLICATION).parent_keys == {
        *definitions.resolve_assets_def(module.PUBLICATION).keys - {module.PUBLICATION},
        dg.AssetKey(["raw", "shopify", "shopify_orders_raw"]),
        dg.AssetKey(["raw", "shopify", "shopify_order_source_status"]),
        dg.AssetKey(["raw", "loop_returns", "loop_returns_raw"]),
    }
    sensor = definitions.get_sensor_def("development_marts_publication_sensor")
    assert sensor.default_status == dg.DefaultSensorStatus.STOPPED


def test_job_publishes_with_the_deployment_settings(
    monkeypatch: pytest.MonkeyPatch, instance: dg.DagsterInstance
) -> None:
    definitions = _defs()
    calls: list[dict[str, object]] = []
    published_at = datetime(2026, 10, 21, tzinfo=UTC)

    def publish(**arguments: object) -> PublicationResult:
        runner = arguments.pop("runner")
        assert isinstance(runner, CandidateRunner)
        runner.events.extend(
            dg.AssetCheckResult(
                passed=True, asset_key=spec.asset_key, check_name=spec.name
            )
            for spec in definitions.resolve_assets_def(module.PUBLICATION).check_specs
        )
        calls.append(arguments)
        return PublicationResult("development", "abc123", published_at)

    monkeypatch.setattr(module, "publish_snowflake_marts", publish)
    for name, value in {
        "ANALYTICS_ENVIRONMENT": "development",
        "ANALYTICS_MARTS_SCHEMA": "development_marts",
        "ANALYTICS_MARTS_READER_ROLE": "DEV_MARTS_READER_ROLE",
        "DBT_PROJECT_DIR": "/project",
        "DBT_PROFILES_DIR": "/profiles",
    }.items():
        monkeypatch.setenv(name, value)
    result = definitions.resolve_job_def(module.JOB_NAME).execute_in_process(
        instance=instance
    )
    assert result.success
    assert calls == [
        {
            "environment": "development",
            "namespace": "development",
            "publication_group": "orders",
            "project_dir": Path("/project"),
            "profiles_dir": Path("/profiles"),
            "reader_role": "DEV_MARTS_READER_ROLE",
            "expected_sources": frozenset({"shopify_orders"}),
        }
    ]
    [materialization] = result.asset_materializations_for_node(
        "marts__development_publication"
    )
    assert materialization.metadata["publication_id"] == dg.TextMetadataValue("abc123")


def test_failed_publication_fails_the_run(
    monkeypatch: pytest.MonkeyPatch, instance: dg.DagsterInstance
) -> None:
    definitions = _defs()
    check = next(iter(definitions.resolve_assets_def(module.PUBLICATION).check_specs))

    def publish(**kwargs: object) -> PublicationResult:
        runner = kwargs["runner"]
        assert isinstance(runner, CandidateRunner)
        runner.events.append(
            dg.AssetCheckResult(
                passed=False, asset_key=check.asset_key, check_name=check.name
            )
        )
        raise RuntimeError("dbt candidate validation/publication failed")

    monkeypatch.setattr(module, "publish_snowflake_marts", publish)
    monkeypatch.setenv("ANALYTICS_MARTS_SCHEMA", "development_marts")
    monkeypatch.setenv("ANALYTICS_MARTS_READER_ROLE", "DEV_MARTS_READER_ROLE")
    result = definitions.resolve_job_def(module.JOB_NAME).execute_in_process(
        instance=instance, raise_on_error=False
    )
    assert not result.success
    assert [check.passed for check in result.get_asset_check_evaluations()] == [False]
    assert not result.get_asset_materialization_events()


@pytest.mark.parametrize("flag", list(ALL_ENABLED))
def test_changed_enablement_fails_before_publisher_runs(
    monkeypatch: pytest.MonkeyPatch, instance: dg.DagsterInstance, flag: str
) -> None:
    definitions = _defs()
    calls: list[dict[str, object]] = []

    def publish(**arguments: object) -> PublicationResult:
        calls.append(arguments)
        raise AssertionError("Publisher must not run with changed inputs")

    monkeypatch.setattr(module, "publish_snowflake_marts", publish)
    monkeypatch.setenv(flag, "true")
    result = definitions.resolve_job_def(module.JOB_NAME).execute_in_process(
        instance=instance, raise_on_error=False
    )
    assert not result.success
    failure = result.failure_data_for_node("marts__development_publication")
    assert failure is not None and failure.error is not None
    assert "reload the code location" in failure.error.to_string()
    assert calls == []


@pytest.mark.parametrize(
    "enabled",
    [
        {},
        {"SHOPIFY_CUSTOMERS_ENABLED": "true"},
        {"LOOP_RETURNS_ENABLED": "true"},
        {"GOODDAY_INVENTORY_ENABLED": "true"},
        ALL_ENABLED,
    ],
)
def test_inputs_match_real_dbt_dependencies(
    tmp_path: Path, enabled: dict[str, str]
) -> None:
    import os
    import subprocess
    import sys

    from analytics_transformations.mart_contracts import read_json
    from analytics_transformations.publication import (
        publication_manifest,
        validate_publication_sources,
    )

    project = Path(__file__).parents[1] / "dbt"
    target = tmp_path / "target"
    _ = subprocess.run(
        [
            str(Path(sys.executable).parent / "dbt"),
            "parse",
            "--quiet",
            "--no-partial-parse",
            "--project-dir",
            str(project),
            "--profiles-dir",
            str(project),
            "--target-path",
            str(target),
            "--log-path",
            str(tmp_path / "logs"),
        ],
        env={
            **os.environ,
            **dict.fromkeys(ALL_ENABLED, "false"),
            **enabled,
            "DBT_TARGET": "local",
            "DBT_NAMESPACE": "check",
            "DUCKDB_PATH": str(tmp_path / "raw.duckdb"),
            "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
        },
        check=True,
        capture_output=True,
        timeout=60,
    )
    manifest = read_json(target / "manifest.json")
    for group in module.GROUP_SETTINGS:
        inputs = module.publication_inputs(enabled, group)
        if not inputs:
            continue
        scoped = publication_manifest(manifest, group)
        expected = frozenset(item.source for item in inputs)
        validate_publication_sources(scoped, expected)
        with pytest.raises(ValueError, match="undeclared"):
            validate_publication_sources(scoped, frozenset())


@pytest.mark.parametrize(
    ("job_name", "group"),
    [
        ("shopify_customers_dlt_load", "customers"),
        ("goodday_inventory_capture", "inventory"),
    ],
)
def test_sensor_observes_success_in_ingestion_location(
    instance: dg.DagsterInstance,
    job_name: str,
    group: str,
) -> None:
    # Origins model the separate ingestion location without starting a server.
    from dagster._core.remote_origin import (
        RegisteredCodeLocationOrigin,
        RemoteJobOrigin,
        RemoteRepositoryOrigin,
    )

    definitions = module.build_definitions({**os.environ, **ALL_ENABLED})
    sensor = definitions.get_sensor_def(module.GROUP_SETTINGS[group][0] + "_sensor")
    context = dg.build_sensor_context(instance=instance)
    initial = sensor.evaluate_tick(context)
    run = instance.add_run(
        dg.DagsterRun(
            job_name=job_name,
            remote_job_origin=RemoteJobOrigin(
                RemoteRepositoryOrigin(
                    RegisteredCodeLocationOrigin("ingestion"), "__repository__"
                ),
                job_name,
            ),
        )
    )
    instance.report_dagster_event(
        dg.DagsterEvent(dg.DagsterEventType.RUN_SUCCESS.value, job_name), run.run_id
    )
    tick = sensor.evaluate_tick(
        dg.build_sensor_context(instance=instance, cursor=initial.cursor)
    )
    assert tick.run_requests is not None
    assert len(tick.run_requests) == 1
    assert (
        tick.run_requests[0].run_key
        == f"{module.GROUP_SETTINGS[group][0]}:{run.run_id}"
    )


def test_waiting_inventory_does_not_coalesce_orders(
    instance: dg.DagsterInstance,
) -> None:
    instance.add_run(
        _run(dg.DagsterRunStatus.NOT_STARTED, "goodday_inventory_publication")
    )
    request = module.publication_request(
        instance,
        _run(dg.DagsterRunStatus.SUCCESS, "shopify_orders_dlt_load"),
        module.publication_inputs(ALL_ENABLED),
    )
    assert isinstance(request, dg.RunRequest)


def test_enabled_groups_have_disjoint_model_checks_and_input_locks() -> None:
    definitions = module.build_definitions({**os.environ, **ALL_ENABLED})
    dg.Definitions.validate_loadable(definitions)
    claimed: set[dg.AssetKey] = set()
    locks: set[str] = set()
    for group, (job_name, key, _, lock) in module.GROUP_SETTINGS.items():
        assets = definitions.resolve_assets_def(key)
        assert assets.check_specs
        assert not claimed.intersection(assets.keys)
        claimed.update(assets.keys)
        assert lock not in locks
        locks.add(lock)
        assert definitions.resolve_job_def(job_name).tags[module.INPUT_TAG] == lock
        assert all(spec.asset_key in assets.keys for spec in assets.check_specs)
        assert {
            item.group for item in module.publication_inputs(ALL_ENABLED, group)
        } == {group}
    assert (
        dg.AssetKey(["fct_orders"])
        in definitions.resolve_assets_def(module.PUBLICATION).keys
    )
    assert (
        dg.AssetKey(["fct_data_availability"])
        in definitions.resolve_assets_def(module.PUBLICATION).keys
    )


@pytest.mark.parametrize("group", ["orders", "customers", "inventory"])
def test_migration_prepublishes_each_group_without_switching_consumers(
    monkeypatch: pytest.MonkeyPatch, instance: dg.DagsterInstance, group: str
) -> None:
    calls: list[dict[str, object]] = []
    for setting, value in ALL_ENABLED.items():
        monkeypatch.setenv(setting, value)
    definitions = module.build_definitions(os.environ)
    job_name, key, schema_setting, _ = module.GROUP_SETTINGS[group]
    assets = definitions.resolve_assets_def(key)

    def publish(**arguments: object) -> PublicationResult:
        runner = arguments.pop("runner")
        assert isinstance(runner, CandidateRunner)
        runner.events.extend(
            dg.AssetCheckResult(
                passed=True, asset_key=spec.asset_key, check_name=spec.name
            )
            for spec in assets.check_specs
        )
        calls.append(arguments)
        return PublicationResult("dev_" + group, "generation", datetime.now(UTC))

    monkeypatch.setattr(module, "publish_snowflake_marts", publish)
    monkeypatch.setenv(schema_setting, "legacy_combined_marts")
    monkeypatch.setenv("ANALYTICS_MARTS_READER_ROLE", "reader")
    result = definitions.resolve_job_def(job_name).execute_in_process(
        instance=instance,
        run_config={
            "ops": {"__".join(key.path): {"config": {"namespace": "dev_" + group}}}
        },
    )
    assert result.success
    assert calls[0]["namespace"] == "dev_" + group
    assert calls[0]["publication_group"] == group
    assert calls[0]["expected_sources"] == frozenset(
        item.source for item in module.publication_inputs(ALL_ENABLED, group)
    )
    assert module.publication_settings(os.environ, group)[0] == "legacy_combined"


def test_groups_cannot_share_a_destination() -> None:
    with pytest.raises(ValueError, match="distinct schemas"):
        module.publication_settings(
            {
                "ANALYTICS_MARTS_SCHEMA": "combined_marts",
                "ANALYTICS_CUSTOMERS_MARTS_SCHEMA": "combined_marts",
                "ANALYTICS_MARTS_READER_ROLE": "reader",
            },
            "customers",
        )
