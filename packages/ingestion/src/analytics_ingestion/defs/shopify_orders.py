"""Jobs and schedules for the dlt-native orders source (ADR 0024).

The load job reads an explicit ``[start, end)`` window into the environment's
raw dataset; the reconcile job compares creation-month counts and marks
confirmed deletions. Setting ``SHOPIFY_ORDERS_SEED`` adds the refresh,
nightly lookback and weekly reconcile schedules. Both jobs share one exclusive
resource, so they never write at once.
"""

import os
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta

import dagster as dg

from analytics_ingestion.shopify.dagster import (
    REJECT_CHECK,
    ScheduleSpec,
    bound,
    reject_check,
)
from analytics_ingestion.shopify.dagster import build_schedules as source_schedules
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
ASSET_KEY = dg.AssetKey(["raw", "shopify", "shopify_orders_raw"])
RECONCILE_JOB_NAME = "shopify_orders_reconcile_counts"
STATUS_KEY = dg.AssetKey(["raw", "shopify", "shopify_order_source_status"])
COUNTS_CHECK = "counts_match"
EXCLUSIVE = {
    "analytics/exclusive-resource": "shopify-orders",
    "analytics/publication-input": "development-marts",
    "dagster/retry_on_asset_or_op_failure": "false",
}
SCHEDULES = ScheduleSpec(
    source="orders",
    job=JOB_NAME,
    op=OP_NAME,
    reconcile_job=RECONCILE_JOB_NAME,
    seed_variable="SHOPIFY_ORDERS_SEED",
    refresh_cron_variable="SHOPIFY_REFRESH_CRON",
    refresh_cron="*/5 * * * *",
    # Reread orders Shopify indexed after the refresh lag, cursor untouched.
    nightly_kind="lookback",
    nightly_cron="15 0 * * *",
    nightly_window=timedelta(hours=26),
    reconcile_cron="0 7 * * 0",
)


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
    except (ValueError, ConfigurationError) as error:
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
        run_id, kind, bound(config.start), bound(config.end), config.workers
    )


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

    Four readers by default keep a refresh inside its tick at about four times
    the 2026-08-18 peak hour.
    """
    if not env.get(SCHEDULES.seed_variable):
        return []
    workers = env.get("SHOPIFY_ORDERS_REFRESH_WORKERS") or "4"
    if not workers.isdigit() or int(workers) < 1:
        raise ConfigurationError(
            "SHOPIFY_ORDERS_REFRESH_WORKERS must be a whole number of at least 1"
        )
    return source_schedules(env, SCHEDULES, {"workers": int(workers)})


defs = dg.Definitions(
    assets=[shopify_orders_raw, shopify_order_source_status],
    jobs=[JOB, RECONCILE_JOB],
    schedules=build_schedules(os.environ),
    resources={"shopify_orders": ShopifyOrdersDltResource()},
)
