from pathlib import Path

import pytest
from dagster import Definitions

import analytics_ingestion
from analytics_ingestion.definitions import defs

CHECKS = Path(__file__).parent / "client.yaml"


def test_installed_definitions_load(monkeypatch: pytest.MonkeyPatch) -> None:
    """The check configuration registers every source that needs no files."""
    assert "site-packages" in Path(analytics_ingestion.__file__).parts
    monkeypatch.setenv("ANALYTICS_CLIENT_CONFIG", str(CHECKS))
    loaded = defs()
    Definitions.validate_loadable(loaded)
    jobs = {job.name for job in loaded.jobs or []}
    assert {
        "shopify_orders_dlt_load",
        "shopify_customers_dlt_load",
        "loop_returns_dlt_snapshot",
    } <= jobs
    assert {schedule.name for schedule in loaded.schedules or []} >= {
        "shopify_orders_dlt_refresh_schedule",
        "shopify_customers_dlt_refresh_schedule",
    }
