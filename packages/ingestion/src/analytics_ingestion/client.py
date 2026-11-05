"""The client's ``client.yaml`` (ADR 0030), as ingestion reads it.

``client_config()`` reads the file ``ANALYTICS_CLIENT_CONFIG`` names, or the
mounted ``/etc/analytics/client.yaml``. Errors name the file and the field.
Each definitions module reads it once at load; run-time resources read it again.
"""

import os

from analytics_client_config import (
    ClientConfig,
    ConfigError,
    LoopReturns,
    Schedule,
    ShopifyCustomers,
    ShopifyOrders,
    SlackAlerts,
    config_path,
    load,
)


def client_config() -> ClientConfig:
    return load(os.environ)


def not_listed(source: str) -> str:
    """The error for a run whose source client.yaml no longer lists."""
    return f"{config_path(os.environ)}: sources.{source} is not listed"


__all__ = [
    "ClientConfig",
    "ConfigError",
    "LoopReturns",
    "Schedule",
    "ShopifyCustomers",
    "ShopifyOrders",
    "SlackAlerts",
    "client_config",
    "not_listed",
]
