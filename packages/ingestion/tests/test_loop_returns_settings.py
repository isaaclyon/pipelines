"""Loop Returns runtime configuration: explicit and validated before any work."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest

from analytics_ingestion.kit.runtime import Target
from analytics_ingestion.loop_returns.dlt_load import pipeline
from analytics_ingestion.loop_returns.errors import ConfigurationError
from analytics_ingestion.loop_returns.settings import (
    DEFAULT_DATASET,
    DEFAULT_HISTORY_START,
    LoopSettings,
    read_credentials,
    settings_from_env,
)
from analytics_ingestion.loop_returns.source import (
    _scope,  # pyright: ignore[reportPrivateUsage]
)
from tests.loop_returns_fakes import API_KEY, SHOP_ID


def credentials(tmp_path: Path, **changes: object) -> Path:
    path = tmp_path / "loop.json"
    data: dict[str, object] = {"api_key": API_KEY}
    data.update(changes)
    _ = path.write_text(json.dumps(data))
    return path


def env(tmp_path: Path, **changes: str) -> dict[str, str]:
    values = {
        "ANALYTICS_ENVIRONMENT": "local",
        "LOOP_RETURNS_CREDENTIALS_PATH": str(credentials(tmp_path)),
        "LOOP_RETURNS_SHOPIFY_SHOP_ID": SHOP_ID,
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
    assert settings.history_start.isoformat() == DEFAULT_HISTORY_START
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


@pytest.mark.parametrize(
    "shop_id",
    ["1", "gid://shopify/Order/1", "gid://shopify/Shop/abc", "  "],
)
def test_shop_identity_must_be_a_shopify_shop_gid(tmp_path: Path, shop_id: str) -> None:
    with pytest.raises(ConfigurationError, match="LOOP_RETURNS_SHOPIFY_SHOP_ID"):
        _ = settings_from_env(env(tmp_path, LOOP_RETURNS_SHOPIFY_SHOP_ID=shop_id))


def test_credential_file_needs_an_api_key(tmp_path: Path) -> None:
    path = credentials(tmp_path, api_key="")

    with pytest.raises(ConfigurationError, match="api_key"):
        _ = read_credentials(path)


def test_unreadable_credentials_do_not_echo_the_path(tmp_path: Path) -> None:
    with pytest.raises(ConfigurationError) as caught:
        _ = read_credentials(tmp_path / "missing.json")

    assert str(tmp_path) not in str(caught.value)


def test_dataset_must_be_an_identifier(tmp_path: Path) -> None:
    values = env(tmp_path, LOOP_RETURNS_DATASET="bad-name; drop")

    with pytest.raises(ConfigurationError, match="LOOP_RETURNS_DATASET"):
        _ = settings_from_env(values)


def test_history_start_needs_an_offset_and_whole_seconds(tmp_path: Path) -> None:
    with pytest.raises(ConfigurationError, match="LOOP_RETURNS_HISTORY_START"):
        _ = settings_from_env(
            env(tmp_path, LOOP_RETURNS_HISTORY_START="2024-06-01T00:00:00")
        )


def test_removal_limit_must_be_a_non_negative_integer(tmp_path: Path) -> None:
    with pytest.raises(ConfigurationError, match="LOOP_RETURNS_MAX_REMOVED_ROWS"):
        _ = settings_from_env(env(tmp_path, LOOP_RETURNS_MAX_REMOVED_ROWS="-1"))


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
