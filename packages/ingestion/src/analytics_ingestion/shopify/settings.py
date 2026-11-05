"""Explicit runtime configuration, parsed before any source read or write.

Both Shopify sources, orders and customers, read the store's credential from
``shopify.json`` in the source settings folder (``ANALYTICS_SOURCE_SECRETS_ROOT``).
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

from analytics_ingestion.kit.runtime import Target, settings_path, target
from analytics_ingestion.shopify.errors import ConfigurationError
from analytics_ingestion.shopify.jsonvalue import load_object

DEFAULT_DATASET = "SHOPIFY_ORDERS_RAW"
# The settings file's name; the file serves both Shopify sources.
SETTINGS_NAME = "shopify"
_DOMAIN = re.compile(r"[a-z0-9][a-z0-9-]*\.myshopify\.com")
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


@dataclass(frozen=True)
class ShopifyCredentials:
    shop_domain: str
    access_token: str = field(repr=False)


@dataclass(frozen=True)
class Settings:
    credentials: ShopifyCredentials
    dataset: str
    target: Target

    @property
    def environment(self) -> str:
        return self.target.environment

    @property
    def shop_handle(self) -> str:
        return self.credentials.shop_domain.removesuffix(".myshopify.com")


def read_credentials(path: Path) -> ShopifyCredentials:
    """Errors name the file, never its content."""
    try:
        raw = path.read_bytes()
    except OSError:
        raise ConfigurationError(f"{path} is unreadable") from None
    if not raw.strip():
        raise ConfigurationError(f"{path} is empty")
    data = load_object(raw, str(path))
    domain = data.get("shop_domain")
    token = data.get("access_token")
    if not isinstance(domain, str) or not _DOMAIN.fullmatch(domain):
        raise ConfigurationError("Credential shop_domain must be <shop>.myshopify.com")
    if not isinstance(token, str) or len(token) < 20:
        raise ConfigurationError("Credential access_token is missing")
    return ShopifyCredentials(domain, token)


def settings_from_env(
    env: Mapping[str, str], *, source: str = "shopify_orders"
) -> Settings:
    """Fail on missing or inconsistent configuration; never fall back to production.

    ``source`` names the kit target: its state folder and local raw file.
    """
    try:
        selected = target(env, source)
        path = settings_path(env, SETTINGS_NAME)
    except ValueError as error:
        raise ConfigurationError(str(error)) from None
    credentials = read_credentials(path)
    dataset = env.get("SHOPIFY_ORDERS_DATASET", DEFAULT_DATASET)
    if not _IDENTIFIER.fullmatch(dataset):
        raise ConfigurationError("SHOPIFY_ORDERS_DATASET must be an identifier")
    return Settings(credentials, dataset, selected)
