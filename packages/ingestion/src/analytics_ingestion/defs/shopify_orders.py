"""Jobs and schedules for the dlt-native orders source (ADR 0024).

The load job reads an explicit ``[start, end)`` window into the environment's
raw dataset; the reconcile job compares creation-month counts and marks
confirmed deletions. Setting ``SHOPIFY_ORDERS_SEED`` adds the refresh,
nightly lookback and weekly reconcile schedules. Both jobs share one exclusive
resource, so they never write at once.
"""

import os
from collections.abc import Iterator, Mapping
from datetime import UTC, datetime, timedelta

import dagster as dg

from analytics_ingestion.shopify.errors import ConfigurationError
from analytics_ingestion.shopify.reconcile import ReconcileReport, reconcile
from analytics_ingestion.shopify.settings import settings_from_env
from analytics_ingestion.shopify.source import (
    ExtractionKind,
    OrdersRun,
    RunReport,
    load_orders,
)

JOB_NAME = "shopify_orders_dlt_load"
OP_NAME = "raw__shopify__shopify_orders_raw"
ACTIVE = [
    dg.DagsterRunStatus.QUEUED,
    dg.DagsterRunStatus.NOT_STARTED,
    dg.DagsterRunStatus.STARTING,
    dg.DagsterRunStatus.STARTED,
    dg.DagsterRunStatus.CANCELING,
]
ASSET_KEY = dg.AssetKey(["raw", "shopify", "shopify_orders_raw"])
REJECT_CHECK = "open_rejects"
REJECT_GRACE = timedelta(hours=24)
RECONCILE_JOB_NAME = "shopify_orders_reconcile_counts"
STATUS_KEY = dg.AssetKey(["raw", "shopify", "shopify_order_source_status"])
COUNTS_CHECK = "counts_match"
EXCLUSIVE = {
    "analytics/exclusive-resource": "shopify-orders",
    "analytics/publication-input": "development-marts",
    "dagster/retry_on_asset_or_op_failure": "false",
}
REFRESH_PRIORITY = {"dagster/priority": "10"}
BACKGROUND_PRIORITY = {"dagster/priority": "-5"}
LOOKBACK = timedelta(hours=26)


class ShopifyOrdersDltConfig(dg.Config):
    start: str
    end: str
    kind: str = "reextract"
    workers: int = 4


class ShopifyOrdersDltResource(dg.ConfigurableResource):  # pyright: ignore[reportMissingTypeArgument]
    """Loads one run with settings from the runtime environment."""

    def load(self, run: OrdersRun) -> RunReport:
        return load_orders(settings_from_env(os.environ), run)

    def reconcile(self, run_id: str) -> ReconcileReport:
        return reconcile(settings_from_env(os.environ), run_id)


@dg.asset(
    name="shopify_orders_raw",
    key_prefix=["raw", "shopify"],
    group_name="shopify",
    kinds={"dlt", "shopify"},
    description="Raw Shopify orders from the dlt-native source.",
    check_specs=[dg.AssetCheckSpec(REJECT_CHECK, asset=ASSET_KEY)],
)
# MaterializeResult's value type parameter is unknown when no value is returned.
def shopify_orders_raw(  # pyright: ignore[reportUnknownParameterType]
    context: dg.AssetExecutionContext,
    config: ShopifyOrdersDltConfig,
    shopify_orders: ShopifyOrdersDltResource,
) -> dg.MaterializeResult:  # pyright: ignore[reportMissingTypeArgument]
    try:
        report = shopify_orders.load(_run(context.run_id, config))
    except (ValueError, ConfigurationError) as error:
        raise dg.Failure(str(error), allow_retries=False) from None
    return dg.MaterializeResult(
        metadata=dict(report.metadata()),
        check_results=[reject_check(report, datetime.now(UTC))],
    )


def reject_check(report: RunReport, now: datetime) -> dg.AssetCheckResult:
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


@dg.asset(
    name="shopify_order_source_status",
    key_prefix=["raw", "shopify"],
    group_name="shopify",
    kinds={"dlt", "shopify"},
    deps=[ASSET_KEY],
    description="Confirmed source deletions from weekly count reconciliation.",
    check_specs=[dg.AssetCheckSpec(COUNTS_CHECK, asset=STATUS_KEY)],
)
def shopify_order_source_status(  # pyright: ignore[reportUnknownParameterType]
    context: dg.AssetExecutionContext,
    shopify_orders: ShopifyOrdersDltResource,
) -> dg.MaterializeResult:  # pyright: ignore[reportMissingTypeArgument]
    """Fails its check while any month has a gap or an unconfirmed absence."""
    try:
        report = shopify_orders.reconcile(context.run.run_id)
    except ConfigurationError as error:
        raise dg.Failure(str(error), allow_retries=False) from None
    metadata = dict(report.metadata())
    return dg.MaterializeResult(
        metadata=metadata,
        check_results=[
            dg.AssetCheckResult(
                check_name=COUNTS_CHECK,
                passed=report.unexplained == 0,
                metadata={key: metadata[key] for key in ("source_only", "unconfirmed")},
            )
        ],
    )


def _run(run_id: str, config: ShopifyOrdersDltConfig) -> OrdersRun:
    kinds: dict[str, ExtractionKind] = {
        "refresh": "refresh",
        "lookback": "lookback",
        "reextract": "reextract",
    }
    kind = kinds.get(config.kind)
    if kind is None:
        raise ValueError(f"kind must be one of {', '.join(kinds)}")
    return OrdersRun(
        run_id, kind, _bound(config.start), _bound(config.end), config.workers
    )


def _bound(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError("Run config bounds must include a UTC offset")
    if parsed.microsecond:
        raise ValueError("Run config bounds must use whole seconds")
    return parsed


# define_asset_job exposes an untyped third-party return type.
JOB = dg.define_asset_job(  # pyright: ignore[reportUnknownMemberType, reportAny]
    name=JOB_NAME,
    selection=[shopify_orders_raw],
    tags=EXCLUSIVE,
    executor_def=dg.in_process_executor,
    description="Load one window with the dlt-native orders source (manual).",
)
RECONCILE_JOB = dg.define_asset_job(  # pyright: ignore[reportUnknownMemberType, reportAny]
    name=RECONCILE_JOB_NAME,
    selection=[shopify_order_source_status],
    tags=EXCLUSIVE,
    executor_def=dg.in_process_executor,
    description="Compare creation-month counts and mark confirmed deletions.",
)


def build_schedules(env: Mapping[str, str]) -> list[dg.ScheduleDefinition]:
    """Refresh, nightly lookback and weekly reconcile, or none before cutover.

    ``SHOPIFY_ORDERS_SEED`` is the first refresh's start; later refreshes resume
    from the dlt cursor. Four readers by default keep a refresh inside its tick
    at about four times the 2026-08-18 peak hour. The schedules start stopped.
    """
    seed = env.get("SHOPIFY_ORDERS_SEED")
    if not seed:
        return []
    try:
        _bound(seed)
    except ValueError as error:
        raise ConfigurationError(
            f"SHOPIFY_ORDERS_SEED must be an ISO instant: {error}"
        ) from None
    workers = env.get("SHOPIFY_ORDERS_REFRESH_WORKERS") or "4"
    if not workers.isdigit() or int(workers) < 1:
        raise ConfigurationError(
            "SHOPIFY_ORDERS_REFRESH_WORKERS must be a whole number of at least 1"
        )

    def load(kind: str, start: datetime | None, end: datetime) -> dg.RunRequest:
        config = {
            "kind": kind,
            "start": seed if start is None else start.isoformat(),
            "end": end.isoformat(),
            "workers": int(workers),
        }
        return dg.RunRequest(
            run_key=f"{JOB_NAME}:{kind}:{end.isoformat()}",
            run_config={"ops": {OP_NAME: {"config": config}}},
            tags=REFRESH_PRIORITY if kind == "refresh" else BACKGROUND_PRIORITY,
        )

    def refresh(
        context: dg.ScheduleEvaluationContext,
    ) -> Iterator[dg.RunRequest | dg.SkipReason]:
        # Skip rather than queue: the next refresh resumes from the cursor.
        if context.instance.get_run_records(
            filters=dg.RunsFilter(job_name=JOB_NAME, statuses=ACTIVE), limit=1
        ):
            yield dg.SkipReason("A dlt orders run is queued or running")
            return
        yield load("refresh", None, _tick(context))

    def lookback(context: dg.ScheduleEvaluationContext) -> dg.RunRequest:
        end = _tick(context)
        return load("lookback", end - LOOKBACK, end)

    def reconcile_counts(context: dg.ScheduleEvaluationContext) -> dg.RunRequest:
        return dg.RunRequest(
            run_key=f"{RECONCILE_JOB_NAME}:{_tick(context).isoformat()}",
            tags=BACKGROUND_PRIORITY,
        )

    try:
        refresh_schedule = dg.ScheduleDefinition(
            name="shopify_orders_dlt_refresh_schedule",
            job_name=JOB_NAME,
            cron_schedule=env.get("SHOPIFY_REFRESH_CRON") or "*/5 * * * *",
            execution_timezone=env.get("SHOPIFY_REFRESH_TIMEZONE") or "UTC",
            execution_fn=refresh,
            default_status=dg.DefaultScheduleStatus.STOPPED,
        )
    except dg.DagsterInvalidDefinitionError as error:
        raise ConfigurationError(f"Invalid refresh schedule: {error}") from None
    # Nightly runs follow the reporting calendar used by dbt and Cube.
    reporting_zone = env.get("ANALYTICS_REPORTING_TIME_ZONE") or "America/Los_Angeles"
    return [
        refresh_schedule,
        dg.ScheduleDefinition(
            name="shopify_orders_dlt_lookback_schedule",
            job_name=JOB_NAME,
            cron_schedule="15 0 * * *",
            execution_timezone=reporting_zone,
            execution_fn=lookback,
            default_status=dg.DefaultScheduleStatus.STOPPED,
        ),
        dg.ScheduleDefinition(
            name=f"{RECONCILE_JOB_NAME}_schedule",
            job_name=RECONCILE_JOB_NAME,
            cron_schedule="0 7 * * 0",
            execution_timezone=reporting_zone,
            execution_fn=reconcile_counts,
            default_status=dg.DefaultScheduleStatus.STOPPED,
        ),
    ]


def _tick(context: dg.ScheduleEvaluationContext) -> datetime:
    tick = context.scheduled_execution_time or datetime.now(UTC)
    return tick.astimezone(UTC).replace(microsecond=0)


defs = dg.Definitions(
    assets=[shopify_orders_raw, shopify_order_source_status],
    jobs=[JOB, RECONCILE_JOB],
    schedules=build_schedules(os.environ),
    resources={"shopify_orders": ShopifyOrdersDltResource()},
)
