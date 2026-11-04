"""Transformation-owned customer profile and full-sweep freshness checks."""

import os

import dagster as dg

from analytics_transformations.defs.shopify_validation import (
    source_freshness,
    validation_request,
)

RAW_CUSTOMERS = dg.AssetKey(["raw", "shopify", "shopify_customers_raw"])
JOB_NAME = "shopify_customers_validation"


def result(source: str) -> dg.AssetCheckResult:
    status, age = source_freshness(f"source:shopify_customers.{source}")
    return dg.AssetCheckResult(
        passed=status == "pass",
        severity=(
            dg.AssetCheckSeverity.WARN
            if status == "warn"
            else dg.AssetCheckSeverity.ERROR
        ),
        metadata={"status": status, "age_seconds": age},
    )


@dg.asset_check(asset=RAW_CUSTOMERS, name="customer_profile_freshness")
def customer_profile_freshness() -> dg.AssetCheckResult:
    """Check complete incremental profile coverage, including empty windows."""
    return result("profile_evidence")


@dg.asset_check(asset=RAW_CUSTOMERS, name="customer_sweep_freshness")
def customer_sweep_freshness() -> dg.AssetCheckResult:
    """Check full sweeps separately: summaries may not advance updatedAt."""
    return result("sweep_evidence")


# Follow the existing raw validation job pattern; the Dagster optional metadata
# parameter includes an untyped PathLike in the installed version.
validation_job = dg.define_asset_job(  # pyright: ignore[reportUnknownMemberType]
    JOB_NAME,
    selection=dg.AssetSelection.checks(
        customer_profile_freshness, customer_sweep_freshness
    ),
    executor_def=dg.in_process_executor,
    tags={"dagster/priority": "20", "dagster/max_retries": "0"},
)
validation_schedule = dg.ScheduleDefinition(
    name="shopify_customers_validation_schedule",
    job=validation_job,
    cron_schedule="*/5 * * * *",
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.STOPPED,
    execution_fn=lambda context: validation_request(context, JOB_NAME),
)

# Expand first: existing environments have no customer raw tables. Activation
# follows initial extraction and grants, rather than breaking order checks.
defs = (
    dg.Definitions(
        asset_checks=[customer_profile_freshness, customer_sweep_freshness],
        jobs=[validation_job],
        schedules=[validation_schedule],
    )
    if os.environ.get("SHOPIFY_CUSTOMERS_ENABLED", "false") == "true"
    else dg.Definitions()
)
