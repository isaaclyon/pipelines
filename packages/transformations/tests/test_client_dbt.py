"""analytics-dbt passes the client settings from client.yaml to dbt."""

import os
import subprocess
from pathlib import Path

import pytest
from analytics_client_config import ConfigError

from analytics_transformations.client_dbt import dbt_env, executable
from tests.client_files import ROOT, write_client


def test_env_derives_sources_and_zone(tmp_path: Path) -> None:
    client = write_client(
        tmp_path, "shopify_orders", "goodday", "loop_returns", zone="Europe/Paris"
    )
    env = dbt_env({"PATH": "/bin", **client})
    assert env == {
        "PATH": "/bin",
        **client,
        "ANALYTICS_ENABLED_SOURCES": "goodday,loop_returns,shopify_orders",
        "ANALYTICS_REPORTING_TIME_ZONE": "Europe/Paris",
    }


def test_no_enabled_sources_is_an_empty_list(tmp_path: Path) -> None:
    env = dbt_env(write_client(tmp_path))
    assert env["ANALYTICS_ENABLED_SOURCES"] == ""


@pytest.mark.parametrize(
    "name", ["ANALYTICS_ENABLED_SOURCES", "ANALYTICS_REPORTING_TIME_ZONE"]
)
def test_generated_variables_are_refused(tmp_path: Path, name: str) -> None:
    # Even an empty or matching value: client.yaml is the only source.
    env = {**write_client(tmp_path, "shopify_orders"), name: ""}
    with pytest.raises(ConfigError, match=f"{name} is generated from client.yaml"):
        _ = dbt_env(env)


def test_missing_client_file_fails(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="client configuration is unreadable"):
        _ = dbt_env({"ANALYTICS_CLIENT_CONFIG": str(tmp_path / "missing.yaml")})


def _wrapper(env: dict[str, str], *arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [executable("analytics-dbt"), *arguments],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )


@pytest.mark.dbt_subprocess
def test_wrapper_execs_dbt_and_returns_its_exit_code(tmp_path: Path) -> None:
    env = {**os.environ, **write_client(tmp_path, "shopify_orders")}
    for name in ("ANALYTICS_ENABLED_SOURCES", "ANALYTICS_REPORTING_TIME_ZONE"):
        _ = env.pop(name, None)
    assert _wrapper(env, "--version").returncode == 0
    # dbt's own failure code, not the wrapper's.
    failed = _wrapper(env, "parse", "--project-dir", str(tmp_path / "missing"))
    assert failed.returncode == 2, failed.stdout + failed.stderr
    assert "analytics-dbt" not in failed.stderr

    refused = _wrapper(env | {"ANALYTICS_REPORTING_TIME_ZONE": "UTC"}, "--version")
    assert refused.returncode == 2
    assert "ANALYTICS_REPORTING_TIME_ZONE is generated from client.yaml" in (
        refused.stderr
    )


# Parsing reads every project file, though it executes no node.
@pytest.mark.dbt("fqn:*", "source:*")
def test_plain_dbt_without_client_settings_fails_clearly(tmp_path: Path) -> None:
    env = {
        **os.environ,
        "DBT_TARGET": "local",
        "DBT_NAMESPACE": "plain",
        "DUCKDB_PATH": str(tmp_path / "warehouse.duckdb"),
    }
    for name in ("ANALYTICS_ENABLED_SOURCES", "ANALYTICS_REPORTING_TIME_ZONE"):
        _ = env.pop(name, None)
    arguments = [
        "parse",
        "--project-dir",
        str(ROOT / "dbt"),
        "--profiles-dir",
        str(ROOT / "dbt"),
        "--target-path",
        str(tmp_path / "target"),
        "--no-partial-parse",
    ]
    result = subprocess.run(
        ["dbt", *arguments], env=env, capture_output=True, text=True, check=False
    )
    assert result.returncode != 0
    assert "ANALYTICS_ENABLED_SOURCES" in result.stdout, result.stdout

    # The time zone has no default either.
    env["ANALYTICS_ENABLED_SOURCES"] = "shopify_orders"
    result = subprocess.run(
        ["dbt", *arguments], env=env, capture_output=True, text=True, check=False
    )
    assert result.returncode != 0
    assert "ANALYTICS_REPORTING_TIME_ZONE" in result.stdout, result.stdout
