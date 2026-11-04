"""The inventory capture is manually launchable and serializes its writers."""

from pathlib import Path

import pytest

from analytics_ingestion.defs.goodday import build_definitions
from tests.test_goodday_runtime import environment, local_environment


def test_disabled_goodday_does_not_register_a_live_job(tmp_path: Path) -> None:
    assert not build_definitions(environment(tmp_path)).jobs


def test_goodday_job_has_no_schedule_and_exclusive_writer_tag(tmp_path: Path) -> None:
    env = local_environment(tmp_path) | {"ANALYTICS_SOURCES": "goodday"}
    definitions = build_definitions(env)
    job = definitions.resolve_job_def("goodday_inventory_capture")
    assert job.tags["analytics/exclusive-resource"] == "goodday-inventory"
    assert definitions.schedules is None


def test_remote_activation_requires_a_readable_snowflake_key(tmp_path: Path) -> None:
    env = environment(tmp_path) | {"ANALYTICS_SOURCES": "goodday"}
    (tmp_path / "key.pem").write_text("fixture")
    assert build_definitions(env).resolve_job_def("goodday_inventory_capture")
    env["SNOWFLAKE_PRIVATE_KEY_PATH"] = "/dev/null"
    with pytest.raises(ValueError, match="SNOWFLAKE_PRIVATE_KEY_PATH"):
        _ = build_definitions(env)


def test_activation_requires_the_settings_file(tmp_path: Path) -> None:
    env = environment(tmp_path) | {"ANALYTICS_SOURCES": "goodday"}
    (tmp_path / "secrets" / "goodday.json").unlink()
    with pytest.raises(ValueError, match="goodday.json"):
        _ = build_definitions(env)


def test_local_activation_needs_no_snowflake_key(tmp_path: Path) -> None:
    """ADR 0029: the local job writes DuckDB, so only the source settings."""
    env = local_environment(tmp_path) | {"ANALYTICS_SOURCES": "goodday"}
    assert build_definitions(env).resolve_job_def("goodday_inventory_capture")
