"""Customer runtime configuration: its own kit source, sharing the orders shop."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import replace

from analytics_ingestion.shopify.errors import ConfigurationError
from analytics_ingestion.shopify.settings import Settings, settings_from_env

DEFAULT_CUSTOMERS_DATASET = "SHOPIFY_CUSTOMERS_RAW"
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def customer_settings_from_env(env: Mapping[str, str]) -> Settings:
    """Settings for the customer pipeline.

    Customers read the store's shopify.json, like orders. They are their own
    kit source: their own state folder, local raw file and dataset, in the same
    Snowflake database as orders.
    """
    dataset = env.get("SHOPIFY_CUSTOMERS_DATASET", DEFAULT_CUSTOMERS_DATASET)
    if not _IDENTIFIER.fullmatch(dataset):
        raise ConfigurationError("SHOPIFY_CUSTOMERS_DATASET must be an identifier")
    return replace(settings_from_env(env, source="shopify_customers"), dataset=dataset)
