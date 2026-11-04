"""The shared source kit: on/off list, settings files, destination and job."""

import json
from pathlib import Path

import dagster as dg
import pytest

from analytics_ingestion.kit.dagster import manual_capture
from analytics_ingestion.kit.runtime import enabled, read_settings, target


def secrets(root: Path, source: str, value: object) -> dict[str, str]:
    folder = root / "secrets"
    folder.mkdir(exist_ok=True)
    (folder / f"{source}.json").write_text(json.dumps(value))
    return {
        "ANALYTICS_ENVIRONMENT": "local",
        "ANALYTICS_SOURCE_SECRETS_ROOT": str(folder),
        "ANALYTICS_INGESTION_STATE_ROOT": str(root / "state"),
    }


@pytest.mark.parametrize(
    ("value", "expected"),
    [("", False), ("fixture", True), (" other , fixture ", True), ("other", False)],
)
def test_sources_are_enabled_by_one_list(value: str, expected: bool) -> None:
    assert enabled({"ANALYTICS_SOURCES": value}, "fixture") is expected


@pytest.mark.parametrize("value", ["Fixture", "fixture,,other", "fix-ture"])
def test_source_list_rejects_unclear_names(value: str) -> None:
    with pytest.raises(ValueError, match="ANALYTICS_SOURCES"):
        _ = enabled({"ANALYTICS_SOURCES": value}, "fixture")


def test_settings_come_from_the_source_file(tmp_path: Path) -> None:
    env = secrets(tmp_path, "fixture", {"token": "t", "limit": 3})
    assert read_settings(env, "fixture") == {"token": "t", "limit": 3}


@pytest.mark.parametrize("content", ["", "[1]", '{"token": "s3cr3t-fixture'])
def test_invalid_settings_file_does_not_leak(tmp_path: Path, content: str) -> None:
    env = secrets(tmp_path, "fixture", {})
    (tmp_path / "secrets" / "fixture.json").write_text(content)
    with pytest.raises(ValueError, match="fixture.json") as error:
        _ = read_settings(env, "fixture")
    assert "s3cr3t" not in str(error.value)


def test_missing_settings_file_names_the_expected_path(tmp_path: Path) -> None:
    env = secrets(tmp_path, "other", {})
    with pytest.raises(ValueError, match=r"secrets/fixture\.json"):
        _ = read_settings(env, "fixture")


def test_local_target_uses_the_raw_file_local_dbt_attaches(tmp_path: Path) -> None:
    env = secrets(tmp_path, "fixture", {})
    local = target(env, "fixture")
    assert local.duckdb_path == tmp_path / "state/fixture/local/fixture.duckdb"
    assert local.identity == ("duckdb", str(local.duckdb_path))
    assert local.snowflake is None


@pytest.mark.parametrize("environment", ["", "ci", "prod"])
def test_live_targets_need_an_explicit_environment(
    tmp_path: Path, environment: str
) -> None:
    env = secrets(tmp_path, "fixture", {}) | {"ANALYTICS_ENVIRONMENT": environment}
    with pytest.raises(ValueError, match="ANALYTICS_ENVIRONMENT"):
        _ = target(env, "fixture")


def test_remote_target_loads_snowflake(tmp_path: Path) -> None:
    env = secrets(tmp_path, "fixture", {}) | {
        "ANALYTICS_ENVIRONMENT": "development",
        "SNOWFLAKE_ACCOUNT": "account",
        "SNOWFLAKE_USER": "loader",
        "SNOWFLAKE_ROLE": "loader_role",
        "SNOWFLAKE_DATABASE": "development",
        "SNOWFLAKE_WAREHOUSE": "loader_warehouse",
        "SNOWFLAKE_PRIVATE_KEY_PATH": str(tmp_path / "key.pem"),
    }
    remote = target(env, "fixture")
    assert remote.identity == ("account", "development")
    with pytest.raises(ValueError, match="local target"):
        _ = target(env, "fixture", local_only=True)
    del env["SNOWFLAKE_ROLE"]
    with pytest.raises(ValueError, match="SNOWFLAKE_ROLE"):
        _ = target(env, "fixture")


def build(env: dict[str, str]) -> dg.Definitions:
    return manual_capture(
        env,
        source="fixture",
        asset="fixture_raw",
        job="fixture_capture",
        kinds={"dlt"},
        description="Fixture capture.",
        run=lambda _env: {"rows": 1},
    )


def test_disabled_source_registers_no_job(tmp_path: Path) -> None:
    assert not build(secrets(tmp_path, "fixture", {})).jobs


def test_enabled_source_registers_a_manual_serialized_job(tmp_path: Path) -> None:
    env = secrets(tmp_path, "fixture", {}) | {"ANALYTICS_SOURCES": "fixture"}
    definitions = build(env)
    job = definitions.resolve_job_def("fixture_capture")
    assert job.tags["analytics/exclusive-resource"] == "fixture"
    assert job.tags["dagster/retry_on_asset_or_op_failure"] == "false"
    assert job.tags["dagster/max_runtime"] == "3600"
    assert definitions.schedules is None
    asset = definitions.resolve_assets_def(
        dg.AssetKey(["raw", "fixture", "fixture_raw"])
    )
    assert dg.materialize([asset]).success


def test_activation_requires_a_readable_settings_file(tmp_path: Path) -> None:
    env = secrets(tmp_path, "other", {}) | {"ANALYTICS_SOURCES": "fixture"}
    with pytest.raises(ValueError, match=r"fixture\.json"):
        _ = build(env)
