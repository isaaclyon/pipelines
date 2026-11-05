"""Manual Dagster job for the Loop Returns header snapshot.

Nothing registers unless client.yaml lists ``loop_returns``; its shop, history
start and removal allowance apply to every run. The job has no schedule. It
shares the ``loop-returns`` exclusive tag so two snapshot runs never write at
once. Failed runs never retry.
"""

import json
import os
import signal
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, date, datetime, time, timedelta
from threading import Timer

import dagster as dg

from analytics_ingestion.client import ClientConfig, client_config, not_listed
from analytics_ingestion.loop_returns.errors import (
    ConfigurationError,
    LoopIngestionError,
)
from analytics_ingestion.loop_returns.settings import LoopSettings, settings_from_env
from analytics_ingestion.loop_returns.source import LoadReport, Progress, load_snapshot

JOB_NAME = "loop_returns_dlt_snapshot"
ASSET_KEY = dg.AssetKey(["raw", "loop_returns", "loop_returns_raw"])
EXCLUSIVE_TAG = "analytics/exclusive-resource"
EXCLUSIVE = {
    EXCLUSIVE_TAG: "loop-returns",
    "dagster/retry_on_asset_or_op_failure": "false",
    "dagster/max_runtime": "300",
}


@contextmanager
def worker_limit(seconds: float = 300) -> Generator[None, None, None]:
    """Bound the step process even while an SDK is blocked; native dlt resumes it."""
    if seconds <= 0:
        raise ValueError("Worker limit must be positive")
    watchdog = Timer(seconds, os.kill, args=(os.getpid(), signal.SIGKILL))
    watchdog.daemon = True
    watchdog.start()
    try:
        yield
    finally:
        watchdog.cancel()
        watchdog.join()


def trial_settings(settings: LoopSettings, day: str) -> tuple[LoopSettings, datetime]:
    """A one-day trial never shares dataset or working state with maintained data."""
    if settings.environment != "development":
        raise ConfigurationError("Loop recovery trial requires development")
    try:
        selected = date.fromisoformat(day)
    except ValueError:
        raise ConfigurationError(
            "Loop trial day must be an ISO calendar date"
        ) from None
    start = datetime.combine(selected, time.min, UTC)
    end = start + timedelta(days=1)
    if end > datetime.now(UTC):
        raise ConfigurationError("Loop trial day must be fully in the past")
    suffix = selected.strftime("%Y%m%d")
    return replace(
        settings,
        history_start=start,
        dataset=f"{settings.dataset}_trial_{suffix}",
        target=replace(
            settings.target,
            root=settings.target.root / "loop_recovery_trial" / suffix,
        ),
    ), end


class LoopReturnsDltResource(dg.ConfigurableResource):  # pyright: ignore[reportMissingTypeArgument]
    """Resumes a candidate; publishes only after its complete history validates."""

    def load(
        self,
        *,
        progress: Progress | None = None,
        day: str | None = None,
        restart_candidate: bool = False,
    ) -> LoadReport:
        with worker_limit():
            source = client_config().sources.loop_returns
            if source is None:
                raise ConfigurationError(not_listed("loop_returns"))
            settings = settings_from_env(os.environ, source)
            end = None
            if day is not None:
                settings, end = trial_settings(settings, day)
            return load_snapshot(
                settings,
                end=end,
                window_hours=6,
                progress=progress,
                restart_candidate=restart_candidate,
            )


class LoopSnapshotConfig(dg.Config):
    """Explicitly rebuild a rejected candidate; ordinary retries retain progress."""

    restart_candidate: bool = False


@dg.asset(
    name="loop_returns_raw",
    key_prefix=["raw", "loop_returns"],
    group_name="loop_returns",
    kinds={"dlt", "loop"},
    description="Loop Returns return headers: a full snapshot, replaced atomically.",
)
# MaterializeResult's value type parameter is unknown when no value is returned.
def loop_returns_raw(  # pyright: ignore[reportUnknownParameterType]
    context: dg.AssetExecutionContext,
    config: LoopSnapshotConfig,
    loop_returns: LoopReturnsDltResource,
) -> dg.MaterializeResult:  # pyright: ignore[reportMissingTypeArgument]
    try:
        report = loop_returns.load(
            progress=lambda event: context.log.info(json.dumps(event)),
            restart_candidate=config.restart_candidate,
        )
    except LoopIngestionError as error:
        raise dg.Failure(str(error), allow_retries=False) from None
    return dg.MaterializeResult(
        metadata={
            "snapshot_id": report.snapshot_id,
            "rows": dg.MetadataValue.int(report.rows),
            "removed": dg.MetadataValue.int(report.removed),
            "windows": dg.MetadataValue.int(report.windows),
            "requests": dg.MetadataValue.int(report.requests),
            "splits": dg.MetadataValue.int(report.splits),
            "snapshot_end": report.snapshot_end.isoformat(),
        }
    )


JOB = dg.define_asset_job(  # pyright: ignore[reportUnknownMemberType, reportAny]
    JOB_NAME,
    selection=[ASSET_KEY],
    tags=EXCLUSIVE,
    description="Manual full snapshot of Loop Returns headers. No schedule.",
)


class LoopRecoveryTrial(LoopSnapshotConfig):
    """Explicit past UTC day; destination and worker limit are not user-overridable."""

    day: str


@dg.op
def loop_returns_recovery_trial(
    context: dg.OpExecutionContext,
    config: LoopRecoveryTrial,
    loop_returns: LoopReturnsDltResource,
) -> None:
    try:
        report = loop_returns.load(
            day=config.day,
            restart_candidate=config.restart_candidate,
            progress=lambda event: context.log.info(json.dumps(event)),
        )
    except LoopIngestionError as error:
        raise dg.Failure(str(error), allow_retries=False) from None
    context.add_output_metadata(report.metadata())


# Dagster job metadata exposes permissive third-party types.
@dg.job(  # pyright: ignore[reportUnknownMemberType]
    tags=EXCLUSIVE,
    description="One-day development Loop recovery trial; maintained data unchanged.",
)
def loop_returns_dlt_recovery_trial() -> None:
    loop_returns_recovery_trial()


def build_definitions(config: ClientConfig) -> dg.Definitions:
    if not config.enabled("loop_returns"):
        return dg.Definitions()
    return dg.Definitions(
        assets=[loop_returns_raw],
        jobs=[JOB, loop_returns_dlt_recovery_trial],
        resources={"loop_returns": LoopReturnsDltResource()},
    )


@dg.definitions
def defs() -> dg.Definitions:
    return build_definitions(client_config())
