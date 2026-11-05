"""The inventory capture is manually launchable and serializes its writers."""

from pathlib import Path

import pytest

from analytics_ingestion.client import ClientConfig
from analytics_ingestion.defs.goodday import build_definitions
from tests.client_files import listing
from tests.test_goodday_runtime import environment, local_environment


@pytest.fixture
def config(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> ClientConfig:
    return listing(monkeypatch, tmp_path, "goodday")


def test_unlisted_goodday_does_not_register_a_live_job(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    unlisted = listing(monkeypatch, tmp_path, "google_sheets")
    assert not build_definitions(unlisted, environment(tmp_path)).jobs


def test_goodday_job_has_no_schedule_and_exclusive_writer_tag(
    tmp_path: Path, config: ClientConfig
) -> None:
    definitions = build_definitions(config, local_environment(tmp_path))
    job = definitions.resolve_job_def("goodday_inventory_capture")
    assert job.tags["analytics/exclusive-resource"] == "goodday-inventory"
    assert definitions.schedules is None


def test_remote_activation_requires_a_readable_snowflake_key(
    tmp_path: Path, config: ClientConfig
) -> None:
    env = environment(tmp_path)
    (tmp_path / "key.pem").write_text("fixture")
    assert build_definitions(config, env).resolve_job_def("goodday_inventory_capture")
    env["SNOWFLAKE_PRIVATE_KEY_PATH"] = "/dev/null"
    with pytest.raises(ValueError, match="SNOWFLAKE_PRIVATE_KEY_PATH"):
        _ = build_definitions(config, env)


def test_activation_requires_the_settings_file(
    tmp_path: Path, config: ClientConfig
) -> None:
    env = environment(tmp_path)
    (tmp_path / "secrets" / "goodday.json").unlink()
    with pytest.raises(ValueError, match="goodday.json"):
        _ = build_definitions(config, env)


def test_local_activation_needs_no_snowflake_key(
    tmp_path: Path, config: ClientConfig
) -> None:
    """ADR 0029: the local job writes DuckDB, so only the source settings."""
    env = local_environment(tmp_path)
    assert build_definitions(config, env).resolve_job_def("goodday_inventory_capture")
