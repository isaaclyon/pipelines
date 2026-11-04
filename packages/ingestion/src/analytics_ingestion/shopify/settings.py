"""Explicit runtime configuration, parsed before any source read or write."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from analytics_ingestion.shopify.errors import ConfigurationError
from analytics_ingestion.shopify.jsonvalue import load_object

Environment = Literal["local", "ci", "development", "production"]
ENVIRONMENTS: tuple[Environment, ...] = ("local", "ci", "development", "production")
DEFAULT_DATASET = "SHOPIFY_ORDERS_RAW"
DEFAULT_STATE_ROOT = Path("/var/lib/analytics/ingestion")
_DOMAIN = re.compile(r"[a-z0-9][a-z0-9-]*\.myshopify\.com")
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


@dataclass(frozen=True)
class DuckDbTarget:
    path: Path

    @property
    def key(self) -> str:
        return "duckdb"


@dataclass(frozen=True)
class SnowflakeTarget:
    account: str
    user: str
    role: str
    database: str
    warehouse: str
    private_key_path: Path

    @property
    def key(self) -> str:
        return f"snowflake-{self.database.lower()}"


@dataclass(frozen=True)
class ShopifyCredentials:
    shop_domain: str
    access_token: str = field(repr=False)


@dataclass(frozen=True)
class Settings:
    environment: Environment
    credentials: ShopifyCredentials
    state_root: Path
    dataset: str
    destination: DuckDbTarget | SnowflakeTarget

    @property
    def shop_handle(self) -> str:
        return self.credentials.shop_domain.removesuffix(".myshopify.com")

    @property
    def target_root(self) -> Path:
        """Working state isolated by environment/store/pipeline/destination."""
        return (
            self.state_root
            / "shopify_orders"
            / self.environment
            / self.shop_handle
            / f"{self.destination.key}-{self.dataset.lower()}"
        )


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


def settings_from_env(env: Mapping[str, str]) -> Settings:
    """Fail on missing or inconsistent configuration; never fall back to production."""
    environment = env.get("ANALYTICS_ENVIRONMENT", "")
    if environment not in ENVIRONMENTS:
        raise ConfigurationError("ANALYTICS_ENVIRONMENT must select a known target")
    selected: Environment = environment  # pyright: ignore[reportAssignmentType]
    credentials = read_credentials(Path(_required(env, "SHOPIFY_CREDENTIALS_PATH")))
    state_root = Path(env.get("SHOPIFY_STATE_ROOT", str(DEFAULT_STATE_ROOT)))
    if not state_root.is_absolute():
        raise ConfigurationError("SHOPIFY_STATE_ROOT must be absolute")
    dataset = env.get("SHOPIFY_ORDERS_DATASET", DEFAULT_DATASET)
    if not _IDENTIFIER.fullmatch(dataset):
        raise ConfigurationError("SHOPIFY_ORDERS_DATASET must be an identifier")
    destination: DuckDbTarget | SnowflakeTarget
    if selected in ("local", "ci"):
        destination = DuckDbTarget(
            state_root / "shopify_orders" / selected / "raw_shopify_orders.duckdb"
        )
    else:
        database = _required(env, "SNOWFLAKE_DATABASE")
        if not _IDENTIFIER.fullmatch(database):
            raise ConfigurationError("SNOWFLAKE_DATABASE must be an identifier")
        destination = SnowflakeTarget(
            account=_required(env, "SNOWFLAKE_ACCOUNT"),
            user=_required(env, "SNOWFLAKE_USER"),
            role=_required(env, "SNOWFLAKE_ROLE"),
            database=database,
            warehouse=_required(env, "SNOWFLAKE_WAREHOUSE"),
            private_key_path=Path(_required(env, "SNOWFLAKE_PRIVATE_KEY_PATH")),
        )
    return Settings(selected, credentials, state_root, dataset, destination)
