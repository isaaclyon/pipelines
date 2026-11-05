"""Dagster assets, jobs and schedules for the customers source."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import dagster as dg
import pytest

import analytics_ingestion.defs.shopify_customers as customers_defs
from analytics_ingestion.client import ClientConfig
from analytics_ingestion.defs.shopify_customers import (
    JOB,
    JOB_NAME,
    OP_NAME,
    RECONCILE_JOB,
    RECONCILE_JOB_NAME,
    ShopifyCustomersDltResource,
    ShopifyCustomersReconcileConfig,
    access_check,
    build_schedules,
    reject_check,
    shopify_customer_source_status,
    shopify_customers_raw,
)
from analytics_ingestion.shopify.customer_reconcile import (
    CustomerReconcileReport,
    reconcile_customers,
)
from analytics_ingestion.shopify.customer_source import (
    CustomersRun,
    RunReport,
    load_customers,
)
from analytics_ingestion.shopify.errors import ConfigurationError
from tests.client_files import client, write_client
from tests.customer_fakes import FakeCustomerShop, customer, customer_settings

SHOPS: dict[str, FakeCustomerShop] = {}
CAPS: list[int] = []
CONFIG: dict[str, object] = {
    "start": "2026-10-01T00:00:00+00:00",
    "end": "2026-10-01T01:00:00+00:00",
}


class FakeDltCustomers(ShopifyCustomersDltResource):
    root: str

    def load(self, run: CustomersRun) -> RunReport:
        return load_customers(
            customer_settings(Path(self.root)),
            run,
            transport=SHOPS[self.root],
            sleeper=lambda _: None,
        )

    def reconcile(
        self, run_id: str, max_candidates: int = 500
    ) -> CustomerReconcileReport:
        CAPS.append(max_candidates)
        return reconcile_customers(
            customer_settings(Path(self.root)),
            run_id,
            SHOPS[self.root],
            sleeper=lambda _: None,
            max_candidates=max_candidates,
        )


def launch(
    root: Path, config: dict[str, object], name: str = JOB_NAME
) -> dg.ExecuteInProcessResult:
    job = dg.Definitions(
        assets=[shopify_customers_raw, shopify_customer_source_status],
        jobs=[JOB, RECONCILE_JOB],
        resources={"shopify_customers": FakeDltCustomers(root=str(root))},
    ).resolve_job_def(name)
    op = job.graph.node_names()[0]
    with dg.instance_for_test() as instance:
        return job.execute_in_process(
            instance=instance,
            run_config={"ops": {op: {"config": config}}},
            raise_on_error=False,
        )


def test_job_loads_a_window_and_reports_evidence(tmp_path: Path) -> None:
    shop = FakeCustomerShop()
    shop.denied = {"names"}
    shop.add(customer(1))
    SHOPS[str(tmp_path)] = shop

    result = launch(tmp_path, CONFIG)

    assert result.success
    [event] = result.get_asset_materialization_events()
    metadata = event.materialization.metadata
    assert metadata["customers"].value == 1
    assert metadata["access_denied_groups"].value == "names"
    checks = {c.check_name: c.passed for c in result.get_asset_check_evaluations()}
    assert checks == {"open_rejects": True, "field_group_access": False}


def test_sweep_then_reconcile_job(tmp_path: Path) -> None:
    shop = FakeCustomerShop()
    shop.add(customer(1))
    shop.add(customer(2))
    SHOPS[str(tmp_path)] = shop
    assert launch(tmp_path, {**CONFIG, "kind": "sweep"}).success
    del shop.customers["gid://shopify/Customer/2"]
    assert launch(
        tmp_path, {**CONFIG, "kind": "sweep", "end": "2026-10-01T02:00:00+00:00"}
    ).success

    result = launch(tmp_path, {}, RECONCILE_JOB_NAME)

    assert result.success
    [event] = result.get_asset_materialization_events()
    assert event.materialization.metadata["confirmed_unavailable"].value == 1


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"kind": "backfill"}, "kind"),
        ({"end": "2026-10-01T01:00:00"}, "UTC offset"),
        ({"start": "2026-10-01T00:00:00.5+00:00"}, "whole seconds"),
    ],
)
def test_bad_config_fails_without_retries(
    tmp_path: Path, change: dict[str, object], message: str
) -> None:
    SHOPS[str(tmp_path)] = FakeCustomerShop()
    result = launch(tmp_path, {**CONFIG, **change})
    assert not result.success
    failure = next(e for e in result.all_events if e.event_type_value == "STEP_FAILURE")
    assert message in str(failure.step_failure_data.error)
    assert not any(e.event_type_value == "STEP_UP_FOR_RETRY" for e in result.all_events)


def test_customers_use_their_own_exclusive_resource() -> None:
    resources = {
        str((job.tags or {})["analytics/exclusive-resource"])
        for job in [JOB, RECONCILE_JOB]
    }
    assert resources == {"shopify-customers"}


CUSTOMERS = """\
sources:
  shopify_customers:
    schedule:
      start: "2026-10-14T15:00:00+00:00"
"""
START = "2026-10-14T15:00:00+00:00"
TICK = datetime(2026, 10, 14, 16, 15, 0, 250000, tzinfo=UTC)


def tick(
    schedule: dg.ScheduleDefinition, instance: dg.DagsterInstance
) -> tuple[list[dg.RunRequest], str | None]:
    context = dg.build_schedule_context(
        instance=instance, scheduled_execution_time=TICK
    )
    data = schedule.evaluate_tick(context)  # pyright: ignore[reportAny]
    requests = cast(list[dg.RunRequest], data.run_requests or [])  # pyright: ignore[reportAny]
    return requests, data.skip_message


@pytest.fixture
def config(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> ClientConfig:
    return client(monkeypatch, tmp_path, CUSTOMERS)


def schedules(config: ClientConfig) -> dict[str, dg.ScheduleDefinition]:
    return {s.name: s for s in build_schedules(config)}


def test_schedules_are_off_without_a_schedule_block(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    listed = client(monkeypatch, tmp_path, "sources:\n  shopify_customers:\n")
    assert build_schedules(listed) == []


def test_all_schedules_start_stopped(config: ClientConfig) -> None:
    found = build_schedules(config)
    assert {s.name for s in found} == {
        "shopify_customers_dlt_refresh_schedule",
        "shopify_customers_dlt_sweep_schedule",
        f"{RECONCILE_JOB_NAME}_schedule",
    }
    assert {s.default_status for s in found} == {dg.DefaultScheduleStatus.STOPPED}


def test_refresh_starts_from_the_schedule_start(config: ClientConfig) -> None:
    schedule = schedules(config)["shopify_customers_dlt_refresh_schedule"]
    assert schedule.cron_schedule == "*/15 * * * *"
    with dg.instance_for_test() as instance:
        [request], _ = tick(schedule, instance)
    assert request.run_config["ops"][OP_NAME]["config"] == {
        "kind": "refresh",
        "start": "2026-10-14T15:00:00+00:00",
        "end": "2026-10-14T16:15:00+00:00",
    }
    assert request.tags["dagster/priority"] == "10"


def test_refresh_cron_can_be_overridden(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config = client(monkeypatch, tmp_path, CUSTOMERS + '      refresh: "0 * * * *"\n')
    schedule = schedules(config)["shopify_customers_dlt_refresh_schedule"]
    assert schedule.cron_schedule == "0 * * * *"


def test_refresh_skips_while_a_run_is_active(config: ClientConfig) -> None:
    schedule = schedules(config)["shopify_customers_dlt_refresh_schedule"]
    with dg.instance_for_test() as instance:
        instance.add_run(
            dg.DagsterRun(
                job_name=JOB_NAME, run_id="busy", status=dg.DagsterRunStatus.STARTED
            )
        )
        requests, skipped = tick(schedule, instance)
    assert not requests and "dlt" in (skipped or "")


def test_nightly_sweep(config: ClientConfig) -> None:
    schedule = schedules(config)["shopify_customers_dlt_sweep_schedule"]
    assert schedule.cron_schedule == "30 1 * * *"
    with dg.instance_for_test() as instance:
        [sweep], _ = tick(schedule, instance)
    assert sweep.run_config["ops"][OP_NAME]["config"] == {
        "kind": "sweep",
        "start": START,
        "end": "2026-10-14T16:15:00+00:00",
    }
    assert int(sweep.tags["dagster/priority"]) < 10


def test_reconcile_is_daily_in_the_background(config: ClientConfig) -> None:
    schedule = schedules(config)[f"{RECONCILE_JOB_NAME}_schedule"]
    assert schedule.job_name == RECONCILE_JOB_NAME
    with dg.instance_for_test() as instance:
        [request], _ = tick(schedule, instance)
    assert int(request.tags["dagster/priority"]) < 10


def test_an_invalid_refresh_cron_names_the_setting(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config = client(monkeypatch, tmp_path, CUSTOMERS + '      refresh: "0 25 * * *"\n')
    with pytest.raises(
        ConfigurationError, match=r"sources\.shopify_customers\.schedule\.refresh"
    ):
        _ = build_schedules(config)


def run_resource(monkeypatch: pytest.MonkeyPatch) -> dict[str, object]:
    """Load one run through the real resource; returns the collection options."""
    seen: dict[str, object] = {}

    def load(
        settings: object,
        run: CustomersRun,
        *,
        collect_tax_id: bool,
        approved_metafields: frozenset[tuple[str, str]],
    ) -> RunReport:
        seen.update(tax_id=collect_tax_id, metafields=approved_metafields)
        return RunReport(run.run_id, run.kind)

    def settings(env: object) -> None:
        return None

    monkeypatch.setattr(customers_defs, "customer_settings_from_env", settings)
    monkeypatch.setattr(customers_defs, "load_customers", load)
    start = datetime(2026, 10, 1, tzinfo=UTC)
    run = CustomersRun("run", "refresh", start, start + timedelta(hours=1))
    _ = ShopifyCustomersDltResource().load(run)
    return seen


def test_resource_collects_what_client_yaml_approves(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _ = write_client(
        monkeypatch,
        tmp_path,
        """\
        sources:
          shopify_customers:
            collect_tax_id: true
            approved_metafields:
              - {namespace: custom, key: loyalty_tier}
        """,
    )
    assert run_resource(monkeypatch) == {
        "tax_id": True,
        "metafields": frozenset({("custom", "loyalty_tier")}),
    }


def test_resource_collects_neither_by_default(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _ = write_client(monkeypatch, tmp_path, "sources:\n  shopify_customers:\n")
    assert run_resource(monkeypatch) == {"tax_id": False, "metafields": frozenset()}


def test_resource_refuses_a_source_client_yaml_no_longer_lists(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    path = write_client(monkeypatch, tmp_path, "sources:\n  shopify_orders:\n")
    with pytest.raises(ConfigurationError, match="sources.shopify_customers") as error:
        _ = run_resource(monkeypatch)
    assert str(path) in str(error.value)


def test_open_rejects_warn_then_fail_after_a_day() -> None:
    now = datetime(2026, 10, 2, tzinfo=UTC)
    report = RunReport("run", "refresh")
    assert reject_check(report, now).passed
    report.open_rejects, report.oldest_open_reject = 1, now - timedelta(hours=23)
    assert reject_check(report, now).severity == dg.AssetCheckSeverity.WARN
    report.oldest_open_reject = now - timedelta(hours=25)
    assert reject_check(report, now).severity == dg.AssetCheckSeverity.ERROR


def test_access_check_warns_on_denied_or_unsupported_groups() -> None:
    report = RunReport("run", "refresh")
    report.access = {
        "names": "access_denied",
        "tax_id": "unsupported",
        "tags": "available",
    }
    result = access_check(report)
    assert not result.passed
    assert result.metadata["groups"].value == "names,tax_id"


def reconcile_config(max_candidates: int) -> dict[str, object]:
    return {"max_candidates": max_candidates}


def sweep_twice_deleting(root: Path, deleted: int) -> None:
    shop = FakeCustomerShop()
    for number in range(1, deleted + 2):
        shop.add(customer(number))
    SHOPS[str(root)] = shop
    assert launch(root, {**CONFIG, "kind": "sweep"}).success
    for number in range(2, deleted + 2):
        del shop.customers[f"gid://shopify/Customer/{number}"]
    end = "2026-10-01T02:00:00+00:00"
    assert launch(root, {**CONFIG, "kind": "sweep", "end": end}).success


def test_reconcile_cap_defaults_to_500_and_blocks_larger_sets(tmp_path: Path) -> None:
    CAPS.clear()
    sweep_twice_deleting(tmp_path, 3)
    result = launch(tmp_path, reconcile_config(2), RECONCILE_JOB_NAME)
    assert CAPS == [2]
    assert not result.success
    failure = next(e for e in result.all_events if e.event_type_value == "STEP_FAILURE")
    assert "More than 2 deletion candidates" in str(failure.step_failure_data.error)
    assert ShopifyCustomersReconcileConfig().max_candidates == 500


def test_reconcile_cap_override_allows_a_large_deletion_set(tmp_path: Path) -> None:
    CAPS.clear()
    sweep_twice_deleting(tmp_path, 3)
    result = launch(tmp_path, reconcile_config(5000), RECONCILE_JOB_NAME)
    assert result.success
    assert CAPS == [5000]
    [event] = result.get_asset_materialization_events()
    assert event.materialization.metadata["confirmed_unavailable"].value == 3


@pytest.mark.parametrize("value", [0, -1, 5001])
def test_reconcile_cap_rejects_values_outside_bounds(
    tmp_path: Path, value: int
) -> None:
    SHOPS[str(tmp_path)] = FakeCustomerShop()
    CAPS.clear()
    result = launch(tmp_path, reconcile_config(value), RECONCILE_JOB_NAME)
    assert not result.success
    assert CAPS == []
    failure = next(e for e in result.all_events if e.event_type_value == "STEP_FAILURE")
    assert "max_candidates" in str(failure.step_failure_data.error)


def test_every_schedule_runs_in_the_reporting_time_zone(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config = client(monkeypatch, tmp_path, CUSTOMERS, zone="Europe/London")
    assert {s.execution_timezone for s in build_schedules(config)} == {"Europe/London"}
