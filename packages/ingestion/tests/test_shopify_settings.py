"""Shopify destination configuration comes from the shared kit target (ADR 0029)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from analytics_ingestion.shopify.customer_settings import customer_settings_from_env
from analytics_ingestion.shopify.customer_source import (
    build_pipeline as customer_pipeline,
)
from analytics_ingestion.shopify.errors import ConfigurationError
from analytics_ingestion.shopify.settings import settings_from_env
from analytics_ingestion.shopify.source import build_pipeline as orders_pipeline


def env(tmp_path: Path, **changes: str) -> dict[str, str]:
    credentials = tmp_path / "shopify.json"
    _ = credentials.write_text(
        json.dumps({"shop_domain": "example.myshopify.com", "access_token": "x" * 32})
    )
    return {
        "ANALYTICS_ENVIRONMENT": "local",
        "SHOPIFY_CREDENTIALS_PATH": str(credentials),
        "ANALYTICS_INGESTION_STATE_ROOT": str(tmp_path / "state"),
        **changes,
    }


def test_local_loads_the_raw_file_local_dbt_attaches(tmp_path: Path) -> None:
    """The path must match the dbt source's ``meta.raw_file``."""
    settings = settings_from_env(env(tmp_path))

    assert settings.target.duckdb_path == (
        tmp_path / "state" / "shopify_orders" / "local" / "shopify_orders.duckdb"
    )


def test_customers_are_their_own_source(tmp_path: Path) -> None:
    settings = customer_settings_from_env(env(tmp_path))

    assert settings.target.duckdb_path == (
        tmp_path / "state" / "shopify_customers" / "local" / "shopify_customers.duckdb"
    )
    assert settings.dataset == "SHOPIFY_CUSTOMERS_RAW"


def test_pipeline_names_keep_their_destination_state(tmp_path: Path) -> None:
    """dlt restores cursors from the destination by pipeline name."""
    assert (
        orders_pipeline(settings_from_env(env(tmp_path))).pipeline_name
        == "shopify_orders_dlt_local_example"
    )
    assert (
        customer_pipeline(customer_settings_from_env(env(tmp_path))).pipeline_name
        == "shopify_customers_dlt_local_example"
    )


@pytest.mark.parametrize("environment", ["", "ci", "staging"])
def test_ci_and_unknown_environments_are_refused(
    tmp_path: Path, environment: str
) -> None:
    with pytest.raises(ConfigurationError, match="ANALYTICS_ENVIRONMENT"):
        _ = settings_from_env(env(tmp_path, ANALYTICS_ENVIRONMENT=environment))


def test_remote_key_path_must_be_absolute(tmp_path: Path) -> None:
    values = env(
        tmp_path,
        ANALYTICS_ENVIRONMENT="development",
        SNOWFLAKE_ACCOUNT="acct",
        SNOWFLAKE_USER="loader",
        SNOWFLAKE_ROLE="LOADER",
        SNOWFLAKE_DATABASE="ANALYTICS",
        SNOWFLAKE_WAREHOUSE="WH",
        SNOWFLAKE_PRIVATE_KEY_PATH="relative/key.p8",
    )
    with pytest.raises(ConfigurationError, match="SNOWFLAKE_PRIVATE_KEY_PATH"):
        _ = settings_from_env(values)


def test_unconfigured_local_credential_fails_closed(tmp_path: Path) -> None:
    """Local Compose mounts an empty file when no credential path is set."""
    empty = tmp_path / "empty"
    _ = empty.write_bytes(b"")

    with pytest.raises(ConfigurationError):
        _ = settings_from_env(env(tmp_path, SHOPIFY_CREDENTIALS_PATH=str(empty)))
