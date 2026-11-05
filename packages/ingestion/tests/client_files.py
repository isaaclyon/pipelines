"""Each test writes its own client.yaml and points ANALYTICS_CLIENT_CONFIG at it."""

from pathlib import Path
from textwrap import dedent

import pytest

from analytics_ingestion.client import ClientConfig, client_config


def write_client(
    monkeypatch: pytest.MonkeyPatch,
    folder: Path,
    body: str = "",
    zone: str = "UTC",
) -> Path:
    """Write ``body`` (YAML under the required header) and select the file."""
    path = folder / "client.yaml"
    header = (
        f"version: 1\nclient: fixture\nreporting_time_zone: {zone}\n"
        "warehouse: snowflake\n"
    )
    path.write_text(header + dedent(body))
    monkeypatch.setenv("ANALYTICS_CLIENT_CONFIG", str(path))
    return path


def client(
    monkeypatch: pytest.MonkeyPatch,
    folder: Path,
    body: str = "",
    zone: str = "UTC",
) -> ClientConfig:
    _ = write_client(monkeypatch, folder, body, zone)
    return client_config()


def listing(
    monkeypatch: pytest.MonkeyPatch, folder: Path, *sources: str
) -> ClientConfig:
    """A client.yaml that lists ``sources`` without options."""
    body = "sources:\n" + "".join(f"  {name}:\n" for name in sources)
    return client(monkeypatch, folder, body)
