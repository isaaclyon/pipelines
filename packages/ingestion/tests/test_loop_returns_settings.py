"""Loop Returns runtime configuration: explicit and validated before any work."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from analytics_ingestion.client import LoopReturns
from analytics_ingestion.kit.runtime import Target
from analytics_ingestion.loop_returns.dlt_load import pipeline
from analytics_ingestion.loop_returns.errors import ConfigurationError
from analytics_ingestion.loop_returns.settings import (
    DEFAULT_DATASET,
    LoopSettings,
    read_credentials,
)
from analytics_ingestion.loop_returns.settings import (
    settings_from_env as loop_settings,
)
from analytics_ingestion.loop_returns.source import (
    _scope,  # pyright: ignore[reportPrivateUsage]
)
from tests.loop_returns_fakes import API_KEY, SHOP_ID

SOURCE = LoopReturns(SHOP_ID, datetime(2025, 3, 1, tzinfo=UTC), 0)


def settings_from_env(values: dict[str, str]) -> LoopSettings:
    return loop_settings(values, SOURCE)


def credentials(tmp_path: Path, **changes: object) -> Path:
    path = tmp_path / "secrets" / "loop_returns.json"
    path.parent.mkdir(exist_ok=True)
    data: dict[str, object] = {"api_key": API_KEY}
    data.update(changes)
    _ = path.write_text(json.dumps(data))
    return path


def env(tmp_path: Path, **changes: str) -> dict[str, str]:
    values = {
        "ANALYTICS_ENVIRONMENT": "local",
        "ANALYTICS_SOURCE_SECRETS_ROOT": str(credentials(tmp_path).parent),
        "ANALYTICS_INGESTION_STATE_ROOT": str(tmp_path / "state"),
    }
    values.update(changes)
    return values


def test_local_defaults_are_explicit_and_scoped_to_the_shop(tmp_path: Path) -> None:
    settings = settings_from_env(env(tmp_path))

    assert settings.environment == "local"
    assert settings.api_key == API_KEY
    assert settings.shop_id == SHOP_ID
    assert settings.dataset == DEFAULT_DATASET == "LOOP_RETURN_HEADERS_RAW"
    assert settings.history_start == SOURCE.history_start
    assert settings.max_removed_rows == 0
    assert settings.target.snowflake is None
    assert settings.target.duckdb_path == (
        tmp_path / "state" / "loop_returns" / "local" / "loop_returns.duckdb"
    )


def test_api_key_is_not_shown_in_repr(tmp_path: Path) -> None:
    settings = settings_from_env(env(tmp_path))

    assert API_KEY not in repr(settings)


def test_missing_environment_selects_nothing(tmp_path: Path) -> None:
    values = env(tmp_path)
    del values["ANALYTICS_ENVIRONMENT"]

    with pytest.raises(ConfigurationError, match="ANALYTICS_ENVIRONMENT"):
        _ = settings_from_env(values)


def test_shop_history_and_removal_allowance_come_from_client_yaml(
    tmp_path: Path,
) -> None:
    source = LoopReturns("gid://shopify/Shop/7", datetime(2026, 1, 1, tzinfo=UTC), 3)
    settings = loop_settings(env(tmp_path), source)
    assert (settings.shop_id, settings.history_start, settings.max_removed_rows) == (
        "gid://shopify/Shop/7",
        datetime(2026, 1, 1, tzinfo=UTC),
        3,
    )


def test_credential_file_needs_an_api_key(tmp_path: Path) -> None:
    path = credentials(tmp_path, api_key="")

    with pytest.raises(ConfigurationError, match="api_key"):
        _ = read_credentials(path)


def test_missing_settings_file_is_named(tmp_path: Path) -> None:
    """Like every source, Loop Returns reads loop_returns.json from the folder."""
    values = env(tmp_path)
    settings_file = tmp_path / "secrets" / "loop_returns.json"
    settings_file.unlink()

    with pytest.raises(ConfigurationError, match=f"{settings_file} is unreadable"):
        _ = settings_from_env(values)


def test_dataset_must_be_an_identifier(tmp_path: Path) -> None:
    values = env(tmp_path, LOOP_RETURNS_DATASET="bad-name; drop")

    with pytest.raises(ConfigurationError, match="LOOP_RETURNS_DATASET"):
        _ = settings_from_env(values)


def test_production_requires_every_snowflake_value(tmp_path: Path) -> None:
    values = env(tmp_path, ANALYTICS_ENVIRONMENT="production")

    with pytest.raises(ConfigurationError, match="SNOWFLAKE_"):
        _ = settings_from_env(values)


def test_production_target_is_the_configured_snowflake_database(tmp_path: Path) -> None:
    key = tmp_path / "key.p8"
    _ = key.write_text("not read by settings")
    values = env(
        tmp_path,
        ANALYTICS_ENVIRONMENT="production",
        SNOWFLAKE_ACCOUNT="acct",
        SNOWFLAKE_USER="loader",
        SNOWFLAKE_ROLE="LOADER",
        SNOWFLAKE_DATABASE="ANALYTICS",
        SNOWFLAKE_WAREHOUSE="WH",
        SNOWFLAKE_PRIVATE_KEY_PATH=str(key),
    )

    settings = settings_from_env(values)

    assert settings.target.snowflake is not None
    assert settings.target.snowflake["database"] == "ANALYTICS"
    assert settings.target.identity == ("acct", "ANALYTICS")


@pytest.mark.parametrize("environment", ["ci", "staging"])
def test_ci_and_unknown_environments_are_rejected(
    tmp_path: Path, environment: str
) -> None:
    with pytest.raises(ConfigurationError, match="ANALYTICS_ENVIRONMENT"):
        _ = settings_from_env(env(tmp_path, ANALYTICS_ENVIRONMENT=environment))


def test_pipeline_names_keep_their_destination_state(tmp_path: Path) -> None:
    """dlt restores snapshot state from the destination by pipeline name."""
    settings = settings_from_env(env(tmp_path))
    assert pipeline(settings).pipeline_name == "loop_returns_dlt_local_1"
    assert (
        pipeline(settings, candidate=True).pipeline_name
        == "loop_returns_dlt_local_1_candidate"
    )


def test_snowflake_scope_is_unchanged_so_checkpoints_resume() -> None:
    """An unfinished remote snapshot resumes only while its scope matches."""
    settings = LoopSettings(
        api_key="k" * 20,
        shop_id="gid://shopify/Shop/1",
        dataset="LOOP_RETURN_HEADERS_RAW",
        history_start=datetime.fromisoformat("2024-06-01T00:00:00+00:00"),
        max_removed_rows=0,
        target=Target(
            "loop_returns",
            "production",
            Path("/state/loop_returns/production"),
            {
                "host": "acct",
                "username": "loader",
                "role": "LOADER",
                "database": "ANALYTICS",
                "warehouse": "WH",
                "private_key_path": "/k.p8",
            },
        ),
    )
    assert _scope(settings, 6, None) == (
        "b3b3f646774f36c26484161cd307855db91c6780600eaf4d7ec3d8d5f96618ba"
    )
