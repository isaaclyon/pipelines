"""Explicit runtime configuration, parsed before any source read or write."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

from analytics_ingestion.kit.runtime import Target, target
from analytics_ingestion.shopify.errors import ConfigurationError
from analytics_ingestion.shopify.jsonvalue import load_object

DEFAULT_DATASET = "SHOPIFY_ORDERS_RAW"
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
    try:
        raw = path.read_bytes()
    except OSError:
        raise ConfigurationError("Shopify credential file is unreadable") from None
    if not raw.strip():
        # Compose mounts an empty file when no host credential path is set.
        raise ConfigurationError("Shopify credential file is empty")
    data = load_object(raw, "credential file")
    domain = data.get("shop_domain")
    token = data.get("access_token")
    if not isinstance(domain, str) or not _DOMAIN.fullmatch(domain):
        raise ConfigurationError("Credential shop_domain must be <shop>.myshopify.com")
    if not isinstance(token, str) or len(token) < 20:
        raise ConfigurationError("Credential access_token is missing")
    return ShopifyCredentials(domain, token)


def _required(env: Mapping[str, str], name: str) -> str:
    value = env.get(name, "").strip()
    if not value:
        raise ConfigurationError(f"Set {name}")
    return value


def settings_from_env(
    env: Mapping[str, str], *, source: str = "shopify_orders"
) -> Settings:
    """Fail on missing or inconsistent configuration; never fall back to production.

    ``source`` names the kit target: its state folder and local raw file.
    """
    try:
        selected = target(env, source)
    except ValueError as error:
        raise ConfigurationError(str(error)) from None
    credentials = read_credentials(Path(_required(env, "SHOPIFY_CREDENTIALS_PATH")))
    dataset = env.get("SHOPIFY_ORDERS_DATASET", DEFAULT_DATASET)
    if not _IDENTIFIER.fullmatch(dataset):
        raise ConfigurationError("SHOPIFY_ORDERS_DATASET must be an identifier")
    return Settings(credentials, dataset, selected)
