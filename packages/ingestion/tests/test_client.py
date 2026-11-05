"""client.yaml decides which sources register; its errors name the file."""

from pathlib import Path

import pytest

from analytics_ingestion.client import ConfigError, not_listed
from analytics_ingestion.definitions import defs
from analytics_ingestion.defs.shopify_orders import defs as orders
from tests.client_files import write_client


def test_only_listed_sources_register(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    loop = """\
        sources:
          loop_returns:
            shop_id: gid://shopify/Shop/1
            history_start: 2026-01-01T00:00:00Z
        """
    _ = write_client(monkeypatch, tmp_path, loop)
    loaded = defs()
    assert {job.name for job in loaded.jobs or []} == {
        "loop_returns_dlt_snapshot",
        "loop_returns_dlt_recovery_trial",
        "scaffold_check",
    }
    assert list(loaded.schedules or []) == []
    assert list(loaded.sensors or []) == []


def test_no_listed_source_leaves_only_the_scaffold_check(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _ = write_client(monkeypatch, tmp_path)
    assert {job.name for job in defs().jobs or []} == {"scaffold_check"}


def test_invalid_configuration_names_the_file_and_field(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    path = write_client(
        monkeypatch, tmp_path, "sources:\n  shopify_orders:\n    refresh_workers: 0\n"
    )
    field = "sources.shopify_orders.refresh_workers"
    with pytest.raises(ConfigError, match=field) as error:
        _ = orders()
    assert str(path) in str(error.value)


def test_the_mounted_file_is_the_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ANALYTICS_CLIENT_CONFIG", raising=False)
    assert not_listed("loop_returns") == (
        "/etc/analytics/client.yaml: sources.loop_returns is not listed"
    )
