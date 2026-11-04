"""Jobs and schedules for the dlt-native customers source (ADR 0024).

The load job reads an explicit ``[start, end)`` window (or a full sweep) into
the customer dataset; the reconcile job confirms deletions after a sweep.
Setting ``SHOPIFY_CUSTOMERS_SEED`` adds the refresh, nightly sweep and daily
reconcile schedules. Both jobs share one exclusive resource,
independent of orders. Freshness thresholds belong to transformations; this
module only reports coverage and access evidence.
"""

import os
from collections.abc import Iterator, Mapping
from datetime import UTC, datetime, timedelta

import dagster as dg
from pydantic import Field

from analytics_ingestion.shopify.customer_reconcile import (
    MAX_CANDIDATES,
    CustomerReconcileReport,
    reconcile_customers,
)
from analytics_ingestion.shopify.customer_settings import (
    customer_metafields_from_env,
    customer_settings_from_env,
)
from analytics_ingestion.shopify.customer_source import (
    CustomerKind,
    CustomersRun,
    RunReport,
    load_customers,
)
from analytics_ingestion.shopify.errors import ConfigurationError

JOB_NAME = "shopify_customers_dlt_load"
OP_NAME = "raw__shopify__shopify_customers_raw"
ACTIVE = [
    dg.DagsterRunStatus.QUEUED,
    dg.DagsterRunStatus.NOT_STARTED,
    dg.DagsterRunStatus.STARTING,
    dg.DagsterRunStatus.STARTED,
    dg.DagsterRunStatus.CANCELING,
]
ASSET_KEY = dg.AssetKey(["raw", "shopify", "shopify_customers_raw"])
STATUS_KEY = dg.AssetKey(["raw", "shopify", "shopify_customer_source_status"])
REJECT_CHECK = "open_rejects"
ACCESS_CHECK = "field_group_access"
UNCONFIRMED_CHECK = "no_unconfirmed_absence"
REJECT_GRACE = timedelta(hours=24)
RECONCILE_JOB_NAME = "shopify_customers_reconcile"
EXCLUSIVE = {
    "analytics/exclusive-resource": "shopify-customers",
    "analytics/publication-input": "development-customers",
    "dagster/retry_on_asset_or_op_failure": "false",
}
REFRESH_PRIORITY = {"dagster/priority": "10"}
BACKGROUND_PRIORITY = {"dagster/priority": "-5"}
MAX_CANDIDATES_LIMIT = 5000


class ShopifyCustomersDltConfig(dg.Config):
    start: str
    end: str
    kind: str = "reextract"


class ShopifyCustomersReconcileConfig(dg.Config):
    """Operator override for the fail-closed deletion candidate cap."""

    max_candidates: int = Field(
        default=MAX_CANDIDATES,
        ge=1,
        le=MAX_CANDIDATES_LIMIT,
        description="Largest deletion set one run may purge; above it the run fails.",
    )


class ShopifyCustomersDltResource(dg.ConfigurableResource):  # pyright: ignore[reportMissingTypeArgument]
    """Loads one run with settings from the runtime environment."""

    def load(self, run: CustomersRun) -> RunReport:
        return load_customers(
            customer_settings_from_env(os.environ),
            run,
            collect_tax_id=os.environ.get("SHOPIFY_CUSTOMERS_TAX_ID_ENABLED") == "true",
            approved_metafields=customer_metafields_from_env(os.environ),
        )

    def reconcile(
        self, run_id: str, max_candidates: int = MAX_CANDIDATES
    ) -> CustomerReconcileReport:
        return reconcile_customers(
            customer_settings_from_env(os.environ),
            run_id,
            max_candidates=max_candidates,
        )


@dg.asset(
    name="shopify_customers_raw",
    key_prefix=["raw", "shopify"],
    group_name="shopify",
    kinds={"dlt", "shopify"},
    description="Raw Shopify customer profiles and children from the dlt source.",
    check_specs=[
        dg.AssetCheckSpec(REJECT_CHECK, asset=ASSET_KEY),
        dg.AssetCheckSpec(ACCESS_CHECK, asset=ASSET_KEY),
    ],
)
# MaterializeResult's value type parameter is unknown when no value is returned.
def shopify_customers_raw(  # pyright: ignore[reportUnknownParameterType]
    context: dg.AssetExecutionContext,
    config: ShopifyCustomersDltConfig,
    shopify_customers: ShopifyCustomersDltResource,
) -> dg.MaterializeResult:  # pyright: ignore[reportMissingTypeArgument]
    try:
        report = shopify_customers.load(_run(context.run.run_id, config))
    except (ValueError, ConfigurationError) as error:
        raise dg.Failure(str(error), allow_retries=False) from None
    return dg.MaterializeResult(
        metadata=dict(report.metadata()),
        check_results=[
            reject_check(report, datetime.now(UTC)),
            access_check(report),
        ],
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


def access_check(report: RunReport) -> dg.AssetCheckResult:
    """Warn while Shopify denies or rejects a field group; its columns are NULL."""
    missing = sorted(
        group
        for group, status in report.access.items()
        if status in ("access_denied", "unsupported")
    )
    return dg.AssetCheckResult(
        check_name=ACCESS_CHECK,
        passed=not missing,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={"groups": ",".join(missing)},
    )


@dg.asset(
    name="shopify_customer_source_status",
    key_prefix=["raw", "shopify"],
    group_name="shopify",
    kinds={"dlt", "shopify"},
    deps=[ASSET_KEY],
    description="Customer availability after a full sweep; confirmed deletions purged.",
    check_specs=[dg.AssetCheckSpec(UNCONFIRMED_CHECK, asset=STATUS_KEY)],
)
def shopify_customer_source_status(  # pyright: ignore[reportUnknownParameterType]
    context: dg.AssetExecutionContext,
    config: ShopifyCustomersReconcileConfig,
    shopify_customers: ShopifyCustomersDltResource,
) -> dg.MaterializeResult:  # pyright: ignore[reportMissingTypeArgument]
    try:
        report = shopify_customers.reconcile(context.run.run_id, config.max_candidates)
    except ConfigurationError as error:
        raise dg.Failure(str(error), allow_retries=False) from None
    return dg.MaterializeResult(
        metadata=dict(report.metadata()),
        check_results=[
            dg.AssetCheckResult(
                check_name=UNCONFIRMED_CHECK,
                passed=report.unconfirmed == 0,
                severity=dg.AssetCheckSeverity.WARN,
                metadata={"unconfirmed": report.unconfirmed},
            )
        ],
    )


def _run(run_id: str, config: ShopifyCustomersDltConfig) -> CustomersRun:
    kinds: dict[str, CustomerKind] = {
        "refresh": "refresh",
        "lookback": "lookback",
        "reextract": "reextract",
        "sweep": "sweep",
    }
    kind = kinds.get(config.kind)
    if kind is None:
        raise ValueError(f"kind must be one of {', '.join(kinds)}")
    return CustomersRun(run_id, kind, _bound(config.start), _bound(config.end))


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
    selection=[shopify_customers_raw],
    tags=EXCLUSIVE,
    executor_def=dg.in_process_executor,
    description="Load one window or a full sweep with the customers source (manual).",
)
RECONCILE_JOB = dg.define_asset_job(  # pyright: ignore[reportUnknownMemberType, reportAny]
    name=RECONCILE_JOB_NAME,
    selection=[shopify_customer_source_status],
    tags=EXCLUSIVE,
    executor_def=dg.in_process_executor,
    description="Confirm customers a full sweep missed and purge confirmed deletions.",
)


def build_schedules(env: Mapping[str, str]) -> list[dg.ScheduleDefinition]:
    """Refresh, nightly sweep and daily reconcile, or none.

    ``SHOPIFY_CUSTOMERS_SEED`` is the first refresh's start; later refreshes
    resume from the dlt cursor. All schedules start stopped. The nightly sweep
    also refreshes Shopify summaries that do not advance ``updatedAt``.
    """
    seed = env.get("SHOPIFY_CUSTOMERS_SEED")
    if not seed:
        return []
    try:
        _bound(seed)
    except ValueError as error:
        raise ConfigurationError(
            f"SHOPIFY_CUSTOMERS_SEED must be an ISO instant: {error}"
        ) from None

    def load(kind: str, start: datetime | None, end: datetime) -> dg.RunRequest:
        config = {
            "kind": kind,
            "start": seed if start is None else start.isoformat(),
            "end": end.isoformat(),
        }
        return dg.RunRequest(
            run_key=f"{JOB_NAME}:{kind}:{end.isoformat()}",
            run_config={"ops": {OP_NAME: {"config": config}}},
            tags=REFRESH_PRIORITY if kind == "refresh" else BACKGROUND_PRIORITY,
        )

    def refresh(
        context: dg.ScheduleEvaluationContext,
    ) -> Iterator[dg.RunRequest | dg.SkipReason]:
        if context.instance.get_run_records(
            filters=dg.RunsFilter(job_name=JOB_NAME, statuses=ACTIVE), limit=1
        ):
            yield dg.SkipReason("A dlt customers run is queued or running")
            return
        yield load("refresh", None, _tick(context))

    def sweep(context: dg.ScheduleEvaluationContext) -> dg.RunRequest:
        return load("sweep", None, _tick(context))

    def reconcile_customers_run(context: dg.ScheduleEvaluationContext) -> dg.RunRequest:
        return dg.RunRequest(
            run_key=f"{RECONCILE_JOB_NAME}:{_tick(context).isoformat()}",
            tags=BACKGROUND_PRIORITY,
        )

    try:
        refresh_schedule = dg.ScheduleDefinition(
            name="shopify_customers_dlt_refresh_schedule",
            job_name=JOB_NAME,
            cron_schedule=env.get("SHOPIFY_CUSTOMERS_REFRESH_CRON") or "*/15 * * * *",
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
            name="shopify_customers_dlt_sweep_schedule",
            job_name=JOB_NAME,
            cron_schedule="30 1 * * *",
            execution_timezone=reporting_zone,
            execution_fn=sweep,
            default_status=dg.DefaultScheduleStatus.STOPPED,
        ),
        dg.ScheduleDefinition(
            name=f"{RECONCILE_JOB_NAME}_schedule",
            job_name=RECONCILE_JOB_NAME,
            cron_schedule="30 4 * * *",
            execution_timezone=reporting_zone,
            execution_fn=reconcile_customers_run,
            default_status=dg.DefaultScheduleStatus.STOPPED,
        ),
    ]


def _tick(context: dg.ScheduleEvaluationContext) -> datetime:
    tick = context.scheduled_execution_time or datetime.now(UTC)
    return tick.astimezone(UTC).replace(microsecond=0)


defs = dg.Definitions(
    assets=[shopify_customers_raw, shopify_customer_source_status],
    jobs=[JOB, RECONCILE_JOB],
    schedules=build_schedules(os.environ),
    resources={"shopify_customers": ShopifyCustomersDltResource()},
)
