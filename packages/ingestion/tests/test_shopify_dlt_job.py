"""Dagster jobs and schedules for the dlt-native orders source (ADR 0024)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import dagster as dg
import pytest

from analytics_ingestion.defs.shopify_orders import (
    JOB,
    JOB_NAME,
    OP_NAME,
    RECONCILE_JOB,
    RECONCILE_JOB_NAME,
    ShopifyOrdersDltResource,
    build_schedules,
    reject_check,
    shopify_order_source_status,
    shopify_orders_raw,
)
from analytics_ingestion.shopify.errors import ConfigurationError
from analytics_ingestion.shopify.reconcile import ReconcileReport, reconcile
from analytics_ingestion.shopify.source import OrdersRun, RunReport, load_orders
from tests.shopify_fakes import FakeShop, order, settings

SHOPS: dict[str, FakeShop] = {}
CONFIG: dict[str, object] = {
    "start": "2026-10-01T00:00:00+00:00",
    "end": "2026-10-01T01:00:00+00:00",
    "workers": 2,
}


class FakeDltOrders(ShopifyOrdersDltResource):
    root: str

    def load(self, run: OrdersRun) -> RunReport:
        return load_orders(
            settings(Path(self.root)),
            run,
            transport=SHOPS[self.root],
            sleeper=lambda _: None,
        )

    def reconcile(self, run_id: str) -> ReconcileReport:
        return reconcile(
            settings(Path(self.root)), run_id, SHOPS[self.root], sleeper=lambda _: None
        )


def launch(
    root: Path, config: dict[str, object], name: str = JOB_NAME
) -> dg.ExecuteInProcessResult:
    job = dg.Definitions(
        assets=[shopify_orders_raw, shopify_order_source_status],
        jobs=[JOB, RECONCILE_JOB],
        resources={"shopify_orders": FakeDltOrders(root=str(root))},
    ).resolve_job_def(name)
    op = job.graph.node_names()[0]
    with dg.instance_for_test() as instance:
        return job.execute_in_process(
            instance=instance,
            run_config={"ops": {op: {"config": config}}},
            raise_on_error=False,
        )


def test_job_loads_the_window_and_reports_counts(tmp_path: Path) -> None:
    shop = FakeShop()
    shop.add(order(1, created="2026-10-01T00:10:00Z"))
    shop.add(order(2, created="2026-10-01T00:20:00Z"))
    SHOPS[str(tmp_path)] = shop

    result = launch(tmp_path, CONFIG)

    assert result.success
    [materialization] = result.get_asset_materialization_events()
    metadata = materialization.materialization.metadata
    assert metadata["orders"].value == 2
    assert metadata["kind"].value == "reextract"
    assert metadata["workers"].value == 2
    [check] = result.get_asset_check_evaluations()
    assert (check.check_name, check.passed) == ("open_rejects", True)


def test_reconcile_job_marks_deletions_and_fails_its_check_on_gaps(
    tmp_path: Path,
) -> None:
    shop = FakeShop()
    shop.add(order(1))
    shop.add(order(2))
    SHOPS[str(tmp_path)] = shop
    assert launch(tmp_path, {**CONFIG, "kind": "refresh"}).success
    del shop.orders["gid://shopify/Order/2"]
    # Another month: a deletion and a gap in one month cancel in the counts.
    shop.add(order(3, created="2026-08-01T00:00:00Z", updated="2026-08-01T00:00:00Z"))

    result = launch(tmp_path, {}, RECONCILE_JOB_NAME)

    assert result.success
    [materialization] = result.get_asset_materialization_events()
    metadata = materialization.materialization.metadata
    assert (metadata["confirmed"].value, metadata["source_only"].value) == (1, 1)
    [check] = result.get_asset_check_evaluations()
    assert (check.check_name, check.passed) == ("counts_match", False)


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
    SHOPS[str(tmp_path)] = FakeShop()
    result = launch(tmp_path, {**CONFIG, **change})
    assert not result.success
    failure = next(
        event for event in result.all_events if event.event_type_value == "STEP_FAILURE"
    )
    assert message in str(failure.step_failure_data.error)
    assert not any(
        event.event_type_value == "STEP_UP_FOR_RETRY" for event in result.all_events
    )


def test_writers_share_one_exclusive_resource() -> None:
    resources = {
        str((job.tags or {})["analytics/exclusive-resource"])
        for job in [JOB, RECONCILE_JOB]
    }
    assert resources == {"shopify-orders"}


ENV = {
    "SHOPIFY_ORDERS_SEED": "2026-10-14T15:00:00+00:00",
    "SHOPIFY_REFRESH_CRON": "*/15 9-16 * * *",
    "SHOPIFY_REFRESH_TIMEZONE": "America/New_York",
}
TICK = datetime(2026, 10, 14, 16, 15, 0, 250000, tzinfo=UTC)


def tick(
    schedule: dg.ScheduleDefinition, instance: dg.DagsterInstance
) -> tuple[list[dg.RunRequest], str | None]:
    context = dg.build_schedule_context(
        instance=instance, scheduled_execution_time=TICK
    )
    # ScheduleExecutionData is not exported, so its fields are untyped here.
    data = schedule.evaluate_tick(context)  # pyright: ignore[reportAny]
    requests = cast(list[dg.RunRequest], data.run_requests or [])  # pyright: ignore[reportAny]
    return requests, data.skip_message


def schedules(env: dict[str, str]) -> dict[str, dg.ScheduleDefinition]:
    return {schedule.name: schedule for schedule in build_schedules(env)}


@pytest.mark.parametrize("seed", [None, ""])
def test_schedules_are_off_without_a_seed(seed: str | None) -> None:
    env = {"SHOPIFY_REFRESH_CRON": "*/5 * * * *"}
    if seed is not None:
        env["SHOPIFY_ORDERS_SEED"] = seed
    assert build_schedules(env) == []


def test_schedules_start_stopped() -> None:
    assert {schedule.default_status for schedule in build_schedules(ENV)} == {
        dg.DefaultScheduleStatus.STOPPED
    }


def test_refresh_follows_the_refresh_cadence_from_the_seed() -> None:
    schedule = schedules(ENV)["shopify_orders_dlt_refresh_schedule"]
    assert schedule.job_name == JOB_NAME
    assert schedule.cron_schedule == "*/15 9-16 * * *"
    assert schedule.execution_timezone == "America/New_York"
    with dg.instance_for_test() as instance:
        [request], _ = tick(schedule, instance)
    assert request.run_config["ops"] == {
        OP_NAME: {
            "config": {
                "kind": "refresh",
                "start": "2026-10-14T15:00:00+00:00",
                "end": "2026-10-14T16:15:00+00:00",
                "workers": 4,
            }
        }
    }
    assert request.tags["dagster/priority"] == "10"


def test_refresh_workers_can_be_set() -> None:
    schedule = schedules({**ENV, "SHOPIFY_ORDERS_REFRESH_WORKERS": "2"})[
        "shopify_orders_dlt_refresh_schedule"
    ]
    with dg.instance_for_test() as instance:
        [request], _ = tick(schedule, instance)
    assert request.run_config["ops"][OP_NAME]["config"]["workers"] == 2


def test_refresh_skips_while_a_dlt_run_is_active() -> None:
    schedule = schedules(ENV)["shopify_orders_dlt_refresh_schedule"]
    with dg.instance_for_test() as instance:
        instance.add_run(
            dg.DagsterRun(
                job_name=JOB_NAME, run_id="busy", status=dg.DagsterRunStatus.STARTED
            )
        )
        requests, skipped = tick(schedule, instance)
    assert not requests
    assert "dlt" in (skipped or "")


def test_lookback_rereads_the_last_26_hours_nightly() -> None:
    schedule = schedules(ENV)["shopify_orders_dlt_lookback_schedule"]
    assert (schedule.job_name, schedule.cron_schedule) == (JOB_NAME, "15 0 * * *")
    assert schedule.execution_timezone == "America/Los_Angeles"
    with dg.instance_for_test() as instance:
        instance.add_run(
            dg.DagsterRun(
                job_name=JOB_NAME, run_id="busy", status=dg.DagsterRunStatus.STARTED
            )
        )
        # Queued behind a refresh rather than skipped: it runs once a night.
        [request], _ = tick(schedule, instance)
    assert request.run_config["ops"][OP_NAME]["config"] == {
        "kind": "lookback",
        "start": "2026-10-13T14:15:00+00:00",
        "end": "2026-10-14T16:15:00+00:00",
        "workers": 4,
    }
    assert int(request.tags["dagster/priority"]) < 10


def test_reconcile_runs_weekly() -> None:
    schedule = schedules(ENV)["shopify_orders_reconcile_counts_schedule"]
    assert schedule.job_name == RECONCILE_JOB_NAME
    assert schedule.cron_schedule == "0 7 * * 0"
    assert schedule.execution_timezone == "America/Los_Angeles"
    zoned = ENV | {"ANALYTICS_REPORTING_TIME_ZONE": "Europe/London"}
    assert schedules(zoned)[schedule.name].execution_timezone == "Europe/London"
    with dg.instance_for_test() as instance:
        [request], _ = tick(schedule, instance)
    assert int(request.tags["dagster/priority"]) < 10


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"SHOPIFY_ORDERS_SEED": "2026-10-14T15:00:00"}, "UTC offset"),
        ({"SHOPIFY_ORDERS_SEED": "yesterday"}, "SHOPIFY_ORDERS_SEED"),
        ({"SHOPIFY_ORDERS_REFRESH_WORKERS": "0"}, "SHOPIFY_ORDERS_REFRESH_WORKERS"),
        ({"SHOPIFY_ORDERS_REFRESH_WORKERS": "four"}, "SHOPIFY_ORDERS_REFRESH_WORKERS"),
        ({"SHOPIFY_REFRESH_CRON": "never"}, "refresh schedule"),
    ],
)
def test_schedules_reject_unsafe_settings(change: dict[str, str], message: str) -> None:
    with pytest.raises(ConfigurationError, match=message):
        build_schedules({**ENV, **change})


def test_open_rejects_warn_and_fail_after_a_day() -> None:
    now = datetime(2026, 10, 2, tzinfo=UTC)
    report = RunReport("run", "refresh")
    assert reject_check(report, now).passed
    report.open_rejects, report.oldest_open_reject = 2, now - timedelta(hours=23)
    warning = reject_check(report, now)
    assert (warning.passed, warning.severity) == (False, dg.AssetCheckSeverity.WARN)
    report.oldest_open_reject = now - timedelta(hours=25)
    failure = reject_check(report, now)
    assert (failure.passed, failure.severity) == (False, dg.AssetCheckSeverity.ERROR)


def test_nightly_lookback_follows_the_reporting_time_zone() -> None:
    env = {**ENV, "ANALYTICS_REPORTING_TIME_ZONE": "Europe/London"}
    schedule = schedules(env)["shopify_orders_dlt_lookback_schedule"]
    assert schedule.execution_timezone == "Europe/London"
