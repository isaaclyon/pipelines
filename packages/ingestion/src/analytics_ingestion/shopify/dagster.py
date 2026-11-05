"""Dagster pieces both Shopify sources share (ADR 0024).

Each source has a load job and a reconcile job. A ``schedule`` block for the
source in client.yaml adds three schedules, all stopped at first:

* refresh, on the toolkit cron unless ``schedule.refresh`` overrides it. It
  resumes from the dlt cursor; the first run starts at ``schedule.start``. A
  tick skips while a load run is queued or running, since the next refresh
  resumes from the cursor anyway. Priority 10.
* one nightly load run (orders look back, customers sweep). It queues rather
  than skips, since it runs once. Priority -5.
* reconciliation, which also queues. Priority -5.

Every cadence follows the client's reporting time zone, the calendar dbt and
Cube use. ``ScheduleSpec`` lists everything that differs between the sources.
"""

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import dagster as dg

from analytics_ingestion.client import Schedule
from analytics_ingestion.shopify.errors import ConfigurationError
from analytics_ingestion.shopify.load_cycle import CycleReport

REJECT_CHECK = "open_rejects"
REJECT_GRACE = timedelta(hours=24)
ACTIVE = [
    dg.DagsterRunStatus.QUEUED,
    dg.DagsterRunStatus.NOT_STARTED,
    dg.DagsterRunStatus.STARTING,
    dg.DagsterRunStatus.STARTED,
    dg.DagsterRunStatus.CANCELING,
]
REFRESH_PRIORITY = {"dagster/priority": "10"}
BACKGROUND_PRIORITY = {"dagster/priority": "-5"}


def bound(value: str) -> datetime:
    """A run window bound: an ISO instant with an offset, in whole seconds."""
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError("Run config bounds must include a UTC offset")
    if parsed.microsecond:
        raise ValueError("Run config bounds must use whole seconds")
    return parsed


def reject_check(report: CycleReport, now: datetime) -> dg.AssetCheckResult:
    """Warn while any reject is open; fail once one has been open a day."""
    oldest = report.oldest_open_reject
    if oldest is None:
        return dg.AssetCheckResult(check_name=REJECT_CHECK, passed=True)
    overdue = now - oldest > REJECT_GRACE
    return dg.AssetCheckResult(
        check_name=REJECT_CHECK,
        passed=False,
        severity=dg.AssetCheckSeverity.ERROR if overdue else dg.AssetCheckSeverity.WARN,
        metadata={
            "open_rejects": report.open_rejects,
            "oldest_hours": round((now - oldest).total_seconds() / 3600, 1),
        },
    )


@dataclass(frozen=True)
class ScheduleSpec:
    """What differs between the sources' schedules.

    Schedule names follow the jobs: ``<job without _load>_refresh_schedule``,
    ``<job without _load>_<nightly kind>_schedule`` and
    ``<reconcile job>_schedule``. A ``nightly_window`` of None starts the
    nightly run at the schedule start, which a sweep ignores."""

    # The source's name in client.yaml.
    source: str
    job: str
    op: str
    reconcile_job: str
    # The toolkit default; ``Schedule.refresh`` overrides it.
    refresh_cron: str
    nightly_kind: str
    nightly_cron: str
    nightly_window: timedelta | None
    reconcile_cron: str


def build_schedules(
    spec: ScheduleSpec,
    schedule: Schedule | None,
    time_zone: str,
    config: Mapping[str, object] | None = None,
) -> list[dg.ScheduleDefinition]:
    """The three schedules in ``time_zone``, or none without a schedule.
    ``config`` adds run config every scheduled load carries."""
    if schedule is None:
        return []
    seed = schedule.start.isoformat()
    extra = dict(config or {})

    def load(kind: str, start: datetime | None, end: datetime) -> dg.RunRequest:
        run_config: dict[str, object] = {
            "kind": kind,
            "start": seed if start is None else start.isoformat(),
            "end": end.isoformat(),
            **extra,
        }
        return dg.RunRequest(
            run_key=f"{spec.job}:{kind}:{end.isoformat()}",
            run_config={"ops": {spec.op: {"config": run_config}}},
            tags=REFRESH_PRIORITY if kind == "refresh" else BACKGROUND_PRIORITY,
        )

    def refresh(
        context: dg.ScheduleEvaluationContext,
    ) -> Iterator[dg.RunRequest | dg.SkipReason]:
        if context.instance.get_run_records(
            filters=dg.RunsFilter(job_name=spec.job, statuses=ACTIVE), limit=1
        ):
            yield dg.SkipReason(f"A dlt {spec.source} run is queued or running")
            return
        yield load("refresh", None, _tick(context))

    def nightly(context: dg.ScheduleEvaluationContext) -> dg.RunRequest:
        end = _tick(context)
        window = spec.nightly_window
        return load(spec.nightly_kind, None if window is None else end - window, end)

    def reconcile(context: dg.ScheduleEvaluationContext) -> dg.RunRequest:
        return dg.RunRequest(
            run_key=f"{spec.reconcile_job}:{_tick(context).isoformat()}",
            tags=BACKGROUND_PRIORITY,
        )

    prefix = spec.job.removesuffix("_load")
    try:
        refresh_schedule = dg.ScheduleDefinition(
            name=f"{prefix}_refresh_schedule",
            job_name=spec.job,
            cron_schedule=schedule.refresh or spec.refresh_cron,
            execution_timezone=time_zone,
            execution_fn=refresh,
            default_status=dg.DefaultScheduleStatus.STOPPED,
        )
    except dg.DagsterInvalidDefinitionError as error:
        raise ConfigurationError(
            f"Invalid refresh schedule in client.yaml "
            f"sources.{spec.source}.schedule.refresh: {error}"
        ) from None
    return [
        refresh_schedule,
        dg.ScheduleDefinition(
            name=f"{prefix}_{spec.nightly_kind}_schedule",
            job_name=spec.job,
            cron_schedule=spec.nightly_cron,
            execution_timezone=time_zone,
            execution_fn=nightly,
            default_status=dg.DefaultScheduleStatus.STOPPED,
        ),
        dg.ScheduleDefinition(
            name=f"{spec.reconcile_job}_schedule",
            job_name=spec.reconcile_job,
            cron_schedule=spec.reconcile_cron,
            execution_timezone=time_zone,
            execution_fn=reconcile,
            default_status=dg.DefaultScheduleStatus.STOPPED,
        ),
    ]


def _tick(context: dg.ScheduleEvaluationContext) -> datetime:
    tick = context.scheduled_execution_time or datetime.now(UTC)
    return tick.astimezone(UTC).replace(microsecond=0)
