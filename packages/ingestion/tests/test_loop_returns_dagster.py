"""Loop Returns Dagster wiring: manual, exclusive and stopped by default."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import dagster as dg
import pytest

from analytics_ingestion.defs.loop_returns import (
    ASSET_KEY,
    EXCLUSIVE_TAG,
    JOB,
    JOB_NAME,
    LoopReturnsDltResource,
    defs,
    loop_returns_raw,
)
from analytics_ingestion.loop_returns.errors import SourceError
from analytics_ingestion.loop_returns.source import LoadReport, Progress


def report() -> LoadReport:
    return LoadReport(
        snapshot_id="snapshot-1",
        shop_id="gid://shopify/Shop/1",
        rows=3,
        removed=1,
        windows=2,
        requests=3,
        splits=0,
        snapshot_end=datetime(2026, 7, 20, 12, tzinfo=UTC),
        completed_at=datetime(2026, 7, 20, 12, 5, tzinfo=UTC),
    )


class FakeLoop(LoopReturnsDltResource):
    error: str | None = None
    expected_restart: bool = False

    def load(
        self,
        *,
        progress: Progress | None = None,
        day: str | None = None,
        restart_candidate: bool = False,
    ) -> LoadReport:
        assert restart_candidate == self.expected_restart
        if self.error is not None:
            raise SourceError(self.error)
        return report()


def test_job_is_manual_exclusive_and_has_no_schedules() -> None:
    assert JOB.name == JOB_NAME == "loop_returns_dlt_snapshot"
    assert JOB.tags is not None
    assert JOB.tags[EXCLUSIVE_TAG] == "loop-returns"
    assert JOB.tags["dagster/retry_on_asset_or_op_failure"] == "false"
    assert list(defs.schedules or []) == []
    assert list(defs.sensors or []) == []
    assert defs.get_job_def(JOB_NAME).name == JOB_NAME


def test_asset_key_and_definitions_load() -> None:
    assert ASSET_KEY == dg.AssetKey(["raw", "loop_returns", "loop_returns_raw"])
    dg.Definitions.validate_loadable(defs)


@pytest.mark.parametrize("restart", [False, True])
def test_materialization_reports_snapshot_evidence(restart: bool) -> None:
    result = dg.materialize(
        [loop_returns_raw],
        resources={"loop_returns": FakeLoop(expected_restart=restart)},
        run_config={
            "ops": {
                "raw__loop_returns__loop_returns_raw": {
                    "config": {"restart_candidate": restart}
                }
            }
        },
    )

    assert result.success
    materialized = result.get_asset_materialization_events()[0].materialization
    assert materialized.metadata["rows"].value == 3
    assert materialized.metadata["removed"].value == 1


def test_source_failure_fails_the_run_without_dagster_retries() -> None:
    result = dg.materialize(
        [loop_returns_raw],
        resources={"loop_returns": FakeLoop(error="HTTP 401 from Loop Returns")},
        raise_on_error=False,
    )

    assert not result.success
    failure = result.get_step_failure_events()[0].step_failure_data.error
    assert failure is not None
    assert "401" in failure.message
    assert failure.cls_name is not None
    assert "Retry" not in failure.cls_name


def test_worker_limit_kills_a_blocked_step() -> None:
    import signal
    import subprocess
    import sys

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from analytics_ingestion.defs.loop_returns import worker_limit; "
            "import time;\nwith worker_limit(0.1): time.sleep(60)",
        ],
        timeout=15,
        check=False,
    )
    assert result.returncode == -signal.SIGKILL


@pytest.mark.parametrize("restart", [False, True])
def test_trial_runs_without_materializing_the_maintained_asset(restart: bool) -> None:
    from analytics_ingestion.defs.loop_returns import loop_returns_dlt_recovery_trial

    result = loop_returns_dlt_recovery_trial.execute_in_process(
        run_config={
            "ops": {
                "loop_returns_recovery_trial": {
                    "config": {"day": "2026-07-02", "restart_candidate": restart}
                }
            }
        },
        resources={"loop_returns": FakeLoop(expected_restart=restart)},
    )
    assert result.success
    assert result.get_asset_materialization_events() == []


def test_trial_settings_isolate_dataset_state_and_date(tmp_path: Path) -> None:
    from datetime import timedelta

    from analytics_ingestion.defs.loop_returns import trial_settings
    from tests.test_loop_returns_load import in_environment, settings

    original = in_environment(settings(tmp_path), "development")
    trial, end = trial_settings(original, "2026-07-02")
    assert trial.dataset != original.dataset
    assert trial.target.root != original.target.root
    assert end - trial.history_start == timedelta(days=1)
    assert trial.target.snowflake == original.target.snowflake


def test_trial_refuses_production_and_future_dates(tmp_path: Path) -> None:

    from analytics_ingestion.defs.loop_returns import trial_settings
    from analytics_ingestion.loop_returns.errors import ConfigurationError
    from tests.test_loop_returns_load import in_environment, settings

    with pytest.raises(ConfigurationError, match="development"):
        _ = trial_settings(
            in_environment(settings(tmp_path), "production"), "2026-07-02"
        )
    with pytest.raises(ConfigurationError, match="past"):
        _ = trial_settings(
            in_environment(settings(tmp_path), "development"), "2999-07-02"
        )
