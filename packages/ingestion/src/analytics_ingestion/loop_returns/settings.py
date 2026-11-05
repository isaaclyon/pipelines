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

from analytics_ingestion.client import LoopReturns
from analytics_ingestion.kit.runtime import Target, settings_path, target
from analytics_ingestion.loop_returns.errors import ConfigurationError

DEFAULT_DATASET = "LOOP_RETURN_HEADERS_RAW"
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
    """Errors name the file, never its content."""
    try:
        raw = path.read_bytes()
    except OSError:
        raise ConfigurationError(f"{path} is unreadable") from None
    try:
        data: object = json.loads(raw)  # pyright: ignore[reportAny]
    except ValueError:
        raise ConfigurationError(f"{path} is not JSON") from None
    if not isinstance(data, dict):
        raise ConfigurationError(f"{path} must contain a JSON object")
    fields = cast(dict[str, object], data)
    api_key = fields.get("api_key")
    if not isinstance(api_key, str) or not api_key.strip():
        raise ConfigurationError(f"{path} must set api_key")
    return LoopCredentials(api_key)


def settings_from_env(env: Mapping[str, str], source: LoopReturns) -> LoopSettings:
    """Fail on missing or inconsistent configuration; never fall back to production.

    ``source`` holds the client.yaml options: shop, history start and the
    removal allowance. The environment selects the target; the API key is in
    loop_returns.json in the source settings folder.
    """
    try:
        selected = target(env, "loop_returns")
        path = settings_path(env, "loop_returns")
    except ValueError as error:
        raise ConfigurationError(str(error)) from None
    credentials = read_credentials(path)
    dataset = env.get("LOOP_RETURNS_DATASET", DEFAULT_DATASET)
    if not _IDENTIFIER.fullmatch(dataset):
        raise ConfigurationError("LOOP_RETURNS_DATASET must be an identifier")
    return LoopSettings(
        api_key=credentials.api_key,
        shop_id=source.shop_id,
        dataset=dataset,
        history_start=source.history_start,
        max_removed_rows=source.max_removed_rows,
        target=selected,
    )
