"""Jobs and schedules for the dlt-native customers source (ADR 0024).

The load job reads an explicit ``[start, end)`` window (or a full sweep) into
the customer dataset; the reconcile job confirms deletions after a sweep.
Setting ``SHOPIFY_CUSTOMERS_SEED`` adds the refresh, nightly sweep and daily
reconcile schedules. Both jobs share one exclusive resource,
independent of orders. Freshness thresholds belong to transformations; this
module only reports coverage and access evidence.
"""

import os
from collections.abc import Mapping
from datetime import UTC, datetime

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
from analytics_ingestion.shopify.dagster import (
    REJECT_CHECK,
    ScheduleSpec,
    bound,
    reject_check,
)
from analytics_ingestion.shopify.dagster import build_schedules as source_schedules
from analytics_ingestion.shopify.errors import ConfigurationError

JOB_NAME = "shopify_customers_dlt_load"
OP_NAME = "raw__shopify__shopify_customers_raw"
ASSET_KEY = dg.AssetKey(["raw", "shopify", "shopify_customers_raw"])
STATUS_KEY = dg.AssetKey(["raw", "shopify", "shopify_customer_source_status"])
ACCESS_CHECK = "field_group_access"
UNCONFIRMED_CHECK = "no_unconfirmed_absence"
RECONCILE_JOB_NAME = "shopify_customers_reconcile"
EXCLUSIVE = {
    "analytics/exclusive-resource": "shopify-customers",
    "analytics/publication-input": "development-customers",
    "dagster/retry_on_asset_or_op_failure": "false",
}
MAX_CANDIDATES_LIMIT = 5000
SCHEDULES = ScheduleSpec(
    source="customers",
    job=JOB_NAME,
    op=OP_NAME,
    reconcile_job=RECONCILE_JOB_NAME,
    seed_variable="SHOPIFY_CUSTOMERS_SEED",
    refresh_cron_variable="SHOPIFY_CUSTOMERS_REFRESH_CRON",
    refresh_cron="*/15 * * * *",
    # The sweep also refreshes summaries that do not advance updatedAt, and
    # covers what a 26-hour lookback would, so customers have no lookback.
    nightly_kind="sweep",
    nightly_cron="30 1 * * *",
    nightly_window=None,
    # Daily, so a confirmed deletion leaves raw within a day of the sweep.
    reconcile_cron="30 4 * * *",
)


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
    except (ValueError, ConfigurationError) as error:
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
    return CustomersRun(run_id, kind, bound(config.start), bound(config.end))


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
    """Refresh, nightly sweep and daily reconcile, or none."""
    return source_schedules(env, SCHEDULES)


defs = dg.Definitions(
    assets=[shopify_customers_raw, shopify_customer_source_status],
    jobs=[JOB, RECONCILE_JOB],
    schedules=build_schedules(os.environ),
    resources={"shopify_customers": ShopifyCustomersDltResource()},
)
