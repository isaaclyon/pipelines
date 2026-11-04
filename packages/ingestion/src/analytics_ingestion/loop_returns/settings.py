"""Explicit runtime configuration, parsed before any API call or write.

The destination is the shared kit target: local loads DuckDB; development and
production write Snowflake only when every Snowflake value is set. No value
falls back to a production target.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import cast

from analytics_ingestion.kit.runtime import Target, target
from analytics_ingestion.loop_returns.errors import ConfigurationError

DEFAULT_DATASET = "LOOP_RETURN_HEADERS_RAW"
DEFAULT_HISTORY_START = "2024-06-01T00:00:00+00:00"
DEFAULT_MAX_REMOVED_ROWS = 0
_SHOP_ID = re.compile(r"gid://shopify/Shop/[0-9]+")
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


@dataclass(frozen=True)
class LoopCredentials:
    api_key: str = field(repr=False)


@dataclass(frozen=True)
class LoopSettings:
    api_key: str = field(repr=False)
    shop_id: str
    dataset: str
    history_start: datetime
    max_removed_rows: int
    target: Target

    @property
    def environment(self) -> str:
        return self.target.environment


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
    try:
        selected = target(env, "loop_returns")
    except ValueError as error:
        raise ConfigurationError(str(error)) from None
    credentials = read_credentials(
        Path(_required(env, "LOOP_RETURNS_CREDENTIALS_PATH"))
    )
    shop_id = _required(env, "LOOP_RETURNS_SHOPIFY_SHOP_ID")
    if not _SHOP_ID.fullmatch(shop_id):
        raise ConfigurationError(
            "LOOP_RETURNS_SHOPIFY_SHOP_ID must be a Shopify Shop gid"
        )
    dataset = env.get("LOOP_RETURNS_DATASET", DEFAULT_DATASET)
    if not _IDENTIFIER.fullmatch(dataset):
        raise ConfigurationError("LOOP_RETURNS_DATASET must be an identifier")
    return LoopSettings(
        api_key=credentials.api_key,
        shop_id=shop_id,
        dataset=dataset,
        history_start=_history_start(
            env.get("LOOP_RETURNS_HISTORY_START", DEFAULT_HISTORY_START)
        ),
        max_removed_rows=_max_removed(
            env.get("LOOP_RETURNS_MAX_REMOVED_ROWS", str(DEFAULT_MAX_REMOVED_ROWS))
        ),
        target=selected,
    )
