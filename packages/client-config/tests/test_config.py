from datetime import UTC, datetime
from pathlib import Path

import pytest

from analytics_client_config import (
    ConfigError,
    LoopReturns,
    Schedule,
    ShopifyCustomers,
    ShopifyOrders,
    SlackAlerts,
    load,
    parse,
)
from analytics_client_config.__main__ import main

MINIMAL = """
version: 1
client: sample-brand
reporting_time_zone: America/New_York
warehouse: motherduck
"""

FULL = """
version: 1
client: sample-brand
reporting_time_zone: America/New_York
warehouse: snowflake
alerts:
  slack:
    dagster_url: https://dagster.example.com
sources:
  shopify_orders:
    schedule:
      start: 2024-01-01T00:00:00Z
      refresh: "*/10 * * * *"
    refresh_workers: 2
  shopify_customers:
    schedule: {start: "2024-01-01T00:00:00+00:00"}
    collect_tax_id: true
    approved_metafields:
      - {namespace: custom, key: birthday}
  loop_returns:
    shop_id: gid://shopify/Shop/123
    history_start: 2024-06-01T00:00:00Z
  goodday: {}
  impact_radius:
"""

START = datetime(2024, 1, 1, tzinfo=UTC)


def test_minimal_file_enables_nothing() -> None:
    config = parse(MINIMAL)
    assert config.client == "sample-brand"
    assert config.warehouse == "motherduck"
    assert config.sources.enabled == ()
    assert config.alerts.slack is None


def test_full_file_parses_every_setting() -> None:
    config = parse(FULL)
    assert config.sources.enabled == (
        "goodday",
        "impact_radius",
        "loop_returns",
        "shopify_customers",
        "shopify_orders",
    )
    assert config.enabled("goodday")
    assert not config.enabled("google_sheets")
    assert config.sources.shopify_orders == ShopifyOrders(
        Schedule(START, "*/10 * * * *"), refresh_workers=2
    )
    assert config.sources.shopify_customers == ShopifyCustomers(
        Schedule(START, None),
        collect_tax_id=True,
        approved_metafields=frozenset({("custom", "birthday")}),
    )
    assert config.sources.loop_returns == LoopReturns(
        "gid://shopify/Shop/123", datetime(2024, 6, 1, tzinfo=UTC), 0
    )
    assert config.alerts.slack == SlackAlerts("https://dagster.example.com")


def test_source_without_schedule_registers_none() -> None:
    config = parse(MINIMAL + "sources:\n  shopify_orders: {}\n")
    assert config.sources.shopify_orders == ShopifyOrders(None, 4)


@pytest.mark.parametrize(
    ("extra", "message"),
    [
        ("colour: blue\n", r"colour is not a setting"),
        ("sources:\n  shopify: {}\n", r"sources\.shopify is not a setting"),
        (
            "sources:\n  shopify_orders: {workers: 2}\n",
            r"sources\.shopify_orders\.workers is not a setting",
        ),
        (
            "sources:\n  shopify_orders:\n    schedule: {start: 2024-01-01}\n",
            r"sources\.shopify_orders\.schedule\.start must be an instant",
        ),
        (
            "sources:\n  shopify_orders:\n"
            "    schedule: {start: '2024-01-01T00:00:00'}\n",
            r"start must be an instant with a UTC offset",
        ),
        (
            "sources:\n  shopify_orders:\n"
            "    schedule: {start: 2024-01-01T00:00:00Z, refresh: hourly}\n",
            r"refresh must be a five-field cron",
        ),
        (
            "sources:\n  shopify_orders: {refresh_workers: 0}\n",
            r"refresh_workers must be a whole number of at least 1",
        ),
        (
            "sources:\n  loop_returns: {shop_id: '123'}\n",
            r"sources\.loop_returns\.shop_id must match",
        ),
        (
            "sources:\n  loop_returns: {shop_id: 'gid://shopify/Shop/1'}\n",
            r"history_start must be an instant",
        ),
        (
            "sources:\n  shopify_customers:\n"
            "    approved_metafields: [{namespace: custom, key: '*'}]\n",
            r"approved_metafields\[0\] must name one exact metafield",
        ),
        ("sources:\n  goodday: {path: x}\n", r"goodday\.path is not a setting"),
        ("alerts:\n  slack: {dagster_url: dagster}\n", r"dagster_url must match"),
        ("alerts:\n  email: {}\n", r"alerts\.email is not a setting"),
    ],
)
def test_invalid_settings_name_the_field(extra: str, message: str) -> None:
    with pytest.raises(ConfigError, match=message):
        _ = parse(MINIMAL + extra, "client.yaml")


@pytest.mark.parametrize(
    ("text", "message"),
    [
        (MINIMAL.replace("version: 1", "version: 2"), r"version must be 1"),
        (MINIMAL.replace("America/New_York", "Mars/Base"), r"IANA time zone"),
        (MINIMAL.replace("motherduck", "duckdb"), r"warehouse must be one of"),
        (MINIMAL.replace("sample-brand", "Sample Brand"), r"client must match"),
        ("- a\n- list\n", r"client\.yaml: file must be a mapping"),
        ("key: [unclosed\n", r"not valid YAML"),
    ],
)
def test_invalid_top_level_settings(text: str, message: str) -> None:
    with pytest.raises(ConfigError, match=message):
        _ = parse(text, "client.yaml")


def test_load_reads_the_named_file(tmp_path: Path) -> None:
    path = tmp_path / "client.yaml"
    _ = path.write_text(MINIMAL)
    assert load({"ANALYTICS_CLIENT_CONFIG": str(path)}).client == "sample-brand"


def test_load_names_a_missing_file(tmp_path: Path) -> None:
    missing = tmp_path / "absent.yaml"
    with pytest.raises(ConfigError, match=r"absent\.yaml: client configuration"):
        _ = load({"ANALYTICS_CLIENT_CONFIG": str(missing)})


def test_load_rejects_a_relative_path() -> None:
    with pytest.raises(ConfigError, match="must be an absolute path"):
        _ = load({"ANALYTICS_CLIENT_CONFIG": "client.yaml"})


def test_cli_reports_success_and_errors(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    good = tmp_path / "good.yaml"
    _ = good.write_text(FULL)
    bad = tmp_path / "bad.yaml"
    _ = bad.write_text(MINIMAL + "colour: blue\n")
    assert main(["check", str(good)]) == 0
    assert "client sample-brand is valid" in capsys.readouterr().out
    assert main(["check", str(bad)]) == 1
    assert "colour is not a setting" in capsys.readouterr().err
    assert main([]) == 2
