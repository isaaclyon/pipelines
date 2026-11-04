"""Explicit runtime configuration, parsed before any API call or write.

Production writes to Snowflake only when every Snowflake value is set. No
value falls back to a production target.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Literal, cast

from analytics_ingestion.loop_returns.errors import ConfigurationError

Environment = Literal["local", "ci", "development", "production"]
ENVIRONMENTS: tuple[Environment, ...] = ("local", "ci", "development", "production")
DEFAULT_DATASET = "LOOP_RETURN_HEADERS_RAW"
DEFAULT_STATE_ROOT = Path("/var/lib/analytics/ingestion")
DEFAULT_HISTORY_START = "2024-06-01T00:00:00+00:00"
DEFAULT_MAX_REMOVED_ROWS = 0
_SHOP_ID = re.compile(r"gid://shopify/Shop/[0-9]+")
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
class LoopCredentials:
    api_key: str = field(repr=False)


@dataclass(frozen=True)
class LoopSettings:
    environment: Environment
    api_key: str = field(repr=False)
    shop_id: str
    state_root: Path
    dataset: str
    history_start: datetime
    max_removed_rows: int
    destination: DuckDbTarget | SnowflakeTarget

    @property
    def target_root(self) -> Path:
        """Working state isolated by environment."""
        return self.state_root / "loop_returns" / self.environment


def read_credentials(path: Path) -> LoopCredentials:
    try:
        raw = path.read_bytes()
    except OSError:
        raise ConfigurationError("Loop Returns credential file is unreadable") from None
    try:
        data: object = json.loads(raw)  # pyright: ignore[reportAny]
    except ValueError:
        raise ConfigurationError("Loop Returns credential file is not JSON") from None
    if not isinstance(data, dict):
        raise ConfigurationError("Loop Returns credential file must be a JSON object")
    fields = cast(dict[str, object], data)
    api_key = fields.get("api_key")
    if not isinstance(api_key, str) or not api_key.strip():
        raise ConfigurationError("Loop Returns credential api_key is missing")
    return LoopCredentials(api_key)


def _required(env: Mapping[str, str], name: str) -> str:
    value = env.get(name, "").strip()
    if not value:
        raise ConfigurationError(f"Set {name}")
    return value


def _history_start(text: str) -> datetime:
    try:
        moment = datetime.fromisoformat(text)
    except ValueError:
        raise ConfigurationError(
            "LOOP_RETURNS_HISTORY_START must be ISO 8601"
        ) from None
    if moment.tzinfo is None or moment.microsecond:
        raise ConfigurationError(
            "LOOP_RETURNS_HISTORY_START needs a UTC offset and whole seconds"
        )
    return moment


def _max_removed(text: str) -> int:
    try:
        value = int(text)
    except ValueError:
        raise ConfigurationError(
            "LOOP_RETURNS_MAX_REMOVED_ROWS must be an integer"
        ) from None
    if value < 0:
        raise ConfigurationError("LOOP_RETURNS_MAX_REMOVED_ROWS must not be negative")
    return value


def settings_from_env(env: Mapping[str, str]) -> LoopSettings:
    """Fail on missing or inconsistent configuration; never fall back to production."""
    environment = env.get("ANALYTICS_ENVIRONMENT", "")
    if environment not in ENVIRONMENTS:
        raise ConfigurationError("ANALYTICS_ENVIRONMENT must select a known target")
    selected: Environment = environment  # pyright: ignore[reportAssignmentType]
    credentials = read_credentials(
        Path(_required(env, "LOOP_RETURNS_CREDENTIALS_PATH"))
    )
    shop_id = _required(env, "LOOP_RETURNS_SHOPIFY_SHOP_ID")
    if not _SHOP_ID.fullmatch(shop_id):
        raise ConfigurationError(
            "LOOP_RETURNS_SHOPIFY_SHOP_ID must be a Shopify Shop gid"
        )
    state_root = Path(env.get("LOOP_RETURNS_STATE_ROOT", str(DEFAULT_STATE_ROOT)))
    if not state_root.is_absolute():
        raise ConfigurationError("LOOP_RETURNS_STATE_ROOT must be absolute")
    dataset = env.get("LOOP_RETURNS_DATASET", DEFAULT_DATASET)
    if not _IDENTIFIER.fullmatch(dataset):
        raise ConfigurationError("LOOP_RETURNS_DATASET must be an identifier")
    history = _history_start(
        env.get("LOOP_RETURNS_HISTORY_START", DEFAULT_HISTORY_START)
    )
    max_removed = _max_removed(
        env.get("LOOP_RETURNS_MAX_REMOVED_ROWS", str(DEFAULT_MAX_REMOVED_ROWS))
    )
    destination: DuckDbTarget | SnowflakeTarget
    root = state_root / "loop_returns" / selected
    if selected in ("local", "ci"):
        destination = DuckDbTarget(root / "raw_loop_returns.duckdb")
    else:
        account = _required(env, "SNOWFLAKE_ACCOUNT")
        user = _required(env, "SNOWFLAKE_USER")
        role = _required(env, "SNOWFLAKE_ROLE")
        database = _required(env, "SNOWFLAKE_DATABASE")
        if not _IDENTIFIER.fullmatch(database):
            raise ConfigurationError("SNOWFLAKE_DATABASE must be an identifier")
        destination = SnowflakeTarget(
            account=account,
            user=user,
            role=role,
            database=database,
            warehouse=_required(env, "SNOWFLAKE_WAREHOUSE"),
            private_key_path=Path(_required(env, "SNOWFLAKE_PRIVATE_KEY_PATH")),
        )
    return LoopSettings(
        environment=selected,
        api_key=credentials.api_key,
        shop_id=shop_id,
        state_root=state_root,
        dataset=dataset,
        history_start=history,
        max_removed_rows=max_removed,
        destination=destination,
    )
