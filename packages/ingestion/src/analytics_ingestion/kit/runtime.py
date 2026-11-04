"""Conventions every kit source shares: on/off list, settings file, destination.

A source named ``<source>`` is enabled by listing it in ``ANALYTICS_SOURCES``.
Its credentials and plain settings live in one JSON object at
``<secrets root>/<source>.json``. The local target loads
``<state root>/<source>/local/<source>.duckdb``, which local dbt attaches
(ADR 0029); development and production load Snowflake. CI uses fixtures only.
"""

import json
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import cast

import dlt
from dlt.pipeline.pipeline import Pipeline

STATE_ROOT = "/var/lib/analytics/ingestion"
SECRETS_ROOT = "/run/secrets/sources"
NAME = re.compile(r"[a-z][a-z0-9_]*")


def enabled(env: Mapping[str, str], source: str) -> bool:
    """Whether ``source`` is in the comma-separated ``ANALYTICS_SOURCES`` list."""
    raw = env.get("ANALYTICS_SOURCES", "").strip()
    names = [name.strip() for name in raw.split(",")] if raw else []
    if not all(NAME.fullmatch(name) for name in names):
        raise ValueError(
            "ANALYTICS_SOURCES must be comma-separated lowercase source names"
        )
    return source in names


def _absolute(env: Mapping[str, str], name: str, default: str) -> Path:
    path = Path(env.get(name, "").strip() or default)
    if not path.is_absolute():
        raise ValueError(f"{name} must be an absolute path")
    return path


def settings_path(env: Mapping[str, str], source: str) -> Path:
    return _absolute(env, "ANALYTICS_SOURCE_SECRETS_ROOT", SECRETS_ROOT) / (
        f"{source}.json"
    )


def read_settings(env: Mapping[str, str], source: str) -> dict[str, object]:
    """Parse the source's settings file; errors name the file, never its content."""
    path = settings_path(env, source)
    try:
        value: object = json.loads(path.read_text())  # pyright: ignore[reportAny]
    except OSError:
        raise ValueError(f"{path} must be a readable settings file") from None
    except ValueError:
        raise ValueError(f"{path} must contain a JSON object") from None
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return cast(dict[str, object], value)


def text(settings: Mapping[str, object], source: str, key: str) -> str:
    value = settings.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{source}.json must set {key} to a nonempty string")
    return value


def positive(settings: Mapping[str, object], source: str, key: str) -> int:
    value = settings.get(key)
    if type(value) is not int or value <= 0:
        raise ValueError(f"{source}.json must set {key} to a positive integer")
    return value


def mapping(settings: Mapping[str, object], source: str, key: str) -> dict[str, object]:
    value = settings.get(key)
    if not isinstance(value, dict):
        raise ValueError(f"{source}.json must set {key} to a JSON object")
    return cast(dict[str, object], value)


@dataclass(frozen=True)
class Target:
    """Where one source loads in this environment, and its dlt state directory."""

    source: str
    environment: str
    root: Path
    # None selects the local DuckDB file.
    snowflake: dict[str, str] | None = field(repr=False)

    @property
    def duckdb_path(self) -> Path:
        return self.root / f"{self.source}.duckdb"

    @property
    def identity(self) -> tuple[str, str]:
        """Destination identity for pipeline names that must not share state."""
        if self.snowflake is None:
            return ("duckdb", str(self.duckdb_path))
        return (self.snowflake["host"], self.snowflake["database"])

    def credential_files(self) -> list[tuple[str, Path]]:
        if self.snowflake is None:
            return []
        return [
            ("SNOWFLAKE_PRIVATE_KEY_PATH", Path(self.snowflake["private_key_path"]))
        ]

    def pipeline(self, name: str, dataset: str) -> Pipeline:
        """Callers serialize runs of this pipeline (one exclusive-resource tag)."""
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        return dlt.pipeline(
            pipeline_name=name,
            pipelines_dir=str(self.root),
            destination=(
                dlt.destinations.duckdb(str(self.duckdb_path))
                if self.snowflake is None
                else dlt.destinations.snowflake(credentials=self.snowflake)
            ),
            dataset_name=dataset,
        )


def target(env: Mapping[str, str], source: str, *, local_only: bool = False) -> Target:
    """Validate the environment and destination before any source access."""
    environment = env.get("ANALYTICS_ENVIRONMENT", "")
    if environment not in ("local", "development", "production"):
        raise ValueError(
            "ANALYTICS_ENVIRONMENT must be local, development or production; "
            "ci uses fixtures"
        )
    if local_only and environment != "local":
        raise ValueError(
            f"{source} runs only on the local target; remote loads need "
            "separate authority"
        )
    root = _absolute(env, "ANALYTICS_INGESTION_STATE_ROOT", STATE_ROOT)
    root = root / source / environment
    if environment == "local":
        return Target(source, environment, root, None)

    def required(name: str) -> str:
        value = env.get(name, "").strip()
        if not value:
            raise ValueError(f"Set {name}")
        return value

    key = Path(required("SNOWFLAKE_PRIVATE_KEY_PATH"))
    if not key.is_absolute():
        raise ValueError("SNOWFLAKE_PRIVATE_KEY_PATH must be an absolute path")
    snowflake = {
        "host": required("SNOWFLAKE_ACCOUNT"),
        "username": required("SNOWFLAKE_USER"),
        "role": required("SNOWFLAKE_ROLE"),
        "database": required("SNOWFLAKE_DATABASE"),
        "warehouse": required("SNOWFLAKE_WAREHOUSE"),
        "private_key_path": str(key),
    }
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", snowflake["database"]):
        raise ValueError("SNOWFLAKE_DATABASE must be an identifier")
    return Target(source, environment, root, snowflake)


def readable(path: Path) -> bool:
    return path.is_file() and os.access(path, os.R_OK)
