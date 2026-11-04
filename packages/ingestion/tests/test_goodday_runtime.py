"""Live access requires an explicit remote target and complete configuration."""

import json
from pathlib import Path

import duckdb
import pytest

from analytics_ingestion.goodday.bigquery import SourceSettings
from analytics_ingestion.goodday.runtime import settings_from_env

SETTINGS: dict[str, object] = {
    "project": "source-project",
    "dataset": "merchant_fixture",
    "billing_project": "billing-project",
    "location": "US",
    "service_account": {"type": "service_account"},
}


def write_settings(root: Path, settings: dict[str, object]) -> None:
    folder = root / "secrets"
    folder.mkdir(exist_ok=True)
    (folder / "goodday.json").write_text(json.dumps(settings))


def environment(root: Path) -> dict[str, str]:
    write_settings(root, SETTINGS)
    return {
        "ANALYTICS_ENVIRONMENT": "development",
        "ANALYTICS_SOURCE_SECRETS_ROOT": str(root / "secrets"),
        "ANALYTICS_INGESTION_STATE_ROOT": str(root / "state"),
        "SNOWFLAKE_ACCOUNT": "account",
        "SNOWFLAKE_USER": "loader",
        "SNOWFLAKE_ROLE": "loader_role",
        "SNOWFLAKE_DATABASE": "development",
        "SNOWFLAKE_WAREHOUSE": "loader_warehouse",
        "SNOWFLAKE_PRIVATE_KEY_PATH": str(root / "key.pem"),
    }


@pytest.mark.parametrize("target", ["", "ci", "prod", "invalid"])
def test_live_entrypoint_rejects_implicit_or_ci_access(
    tmp_path: Path, target: str
) -> None:
    env = environment(tmp_path)
    env["ANALYTICS_ENVIRONMENT"] = target
    with pytest.raises(ValueError, match="ANALYTICS_ENVIRONMENT"):
        settings_from_env(env)


def test_invalid_source_settings_fail_before_credentials_are_read(
    tmp_path: Path,
) -> None:
    env = environment(tmp_path)
    write_settings(tmp_path, {**SETTINGS, "location": 5})
    with pytest.raises(ValueError, match="location"):
        settings_from_env(env)


def test_pipeline_state_separates_environment_source_and_destination(
    tmp_path: Path,
) -> None:
    env = environment(tmp_path)
    dev = settings_from_env(env)
    env["SNOWFLAKE_DATABASE"] = "another_database"
    other = settings_from_env(env)
    assert dev.pipeline_name != other.pipeline_name
    write_settings(tmp_path, {**SETTINGS, "dataset": "another_merchant"})
    assert settings_from_env(env).pipeline_name != other.pipeline_name


def local_environment(root: Path) -> dict[str, str]:
    """ADR 0029: local needs the source controls but no Snowflake settings."""
    env = {
        key: value
        for key, value in environment(root).items()
        if not key.startswith("SNOWFLAKE_")
    }
    env["ANALYTICS_ENVIRONMENT"] = "local"
    return env


def test_local_loads_the_raw_file_local_dbt_attaches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from analytics_ingestion.goodday import runtime
    from tests.test_goodday_source import Snapshot

    read: list[tuple[str, str]] = []
    disposed: list[bool] = []

    class Engine:
        def dispose(self) -> None:
            disposed.append(True)

    def engine(settings: SourceSettings, info: dict[str, object]) -> Engine:
        assert info == SETTINGS["service_account"]
        return Engine()

    def inventory(_engine: Engine, schema: str, relation: str) -> Snapshot:
        read.append((schema, relation))
        return Snapshot([1, 2])

    monkeypatch.setattr(runtime, "engine_from_info", engine)
    monkeypatch.setattr(runtime, "read_inventory", inventory)

    report = runtime.load_inventory(local_environment(tmp_path))

    assert report.row_count == 2
    assert read == [
        (
            "source-project.merchant_fixture",
            "source-project.merchant_fixture.inventory_by_item",
        )
    ]
    assert disposed == [True]
    raw = tmp_path / "state" / "goodday" / "local" / "goodday.duckdb"
    with duckdb.connect(str(raw), read_only=True) as connection:
        rows = connection.execute(
            "select count(*) from raw_goodday.inventory_by_item"
        ).fetchone()
    assert rows == (2,)
