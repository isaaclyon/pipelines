"""Shopify orders local configuration (ADR 0029)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from analytics_ingestion.shopify.errors import ConfigurationError
from analytics_ingestion.shopify.settings import DuckDbTarget, settings_from_env


def test_local_loads_the_raw_file_local_dbt_attaches(tmp_path: Path) -> None:
    """The path must match the dbt source's ``meta.raw_file``."""
    credentials = tmp_path / "shopify.json"
    _ = credentials.write_text(
        json.dumps({"shop_domain": "example.myshopify.com", "access_token": "x" * 32})
    )
    settings = settings_from_env(
        {
            "ANALYTICS_ENVIRONMENT": "local",
            "SHOPIFY_CREDENTIALS_PATH": str(credentials),
            "SHOPIFY_STATE_ROOT": str(tmp_path / "state"),
        }
    )

    assert settings.destination == DuckDbTarget(
        tmp_path / "state" / "shopify_orders" / "local" / "raw_shopify_orders.duckdb"
    )


def test_unconfigured_local_credential_fails_closed(tmp_path: Path) -> None:
    """Local Compose mounts an empty file when no credential path is set."""
    empty = tmp_path / "empty"
    _ = empty.write_bytes(b"")

    with pytest.raises(ConfigurationError):
        _ = settings_from_env(
            {
                "ANALYTICS_ENVIRONMENT": "local",
                "SHOPIFY_CREDENTIALS_PATH": str(empty),
                "SHOPIFY_STATE_ROOT": str(tmp_path / "state"),
            }
        )
