"""dbt source freshness on raw Shopify orders, run as a Dagster asset check.

The policy lives in the dbt source: the latest complete refresh's
``window_end`` warns after 25 minutes and errors after 30.
"""

import os
import subprocess
import sys
import tempfile
from pathlib import Path

import dagster as dg

from analytics_transformations.mart_contracts import mapping, read_json, sequence

RAW_ORDERS = dg.AssetKey(["raw", "shopify", "shopify_orders_raw"])
SOURCE = "source:shopify_orders.ingestion_batches"
JOB_NAME = "shopify_raw_validation"
TIMEOUT_SECONDS = 120
SEVERITY = {"warn": dg.AssetCheckSeverity.WARN}


@dg.asset_check(
    asset=RAW_ORDERS,
    name="refresh_freshness",
    description="dbt source freshness on the latest complete refresh",
)
def refresh_freshness() -> dg.AssetCheckResult:
    status, age = source_freshness()
    return dg.AssetCheckResult(
        passed=status == "pass",
        severity=SEVERITY.get(status, dg.AssetCheckSeverity.ERROR),
        metadata={"status": status, "age_seconds": age},
    )


def source_freshness(source: str = SOURCE) -> tuple[str, float | None]:
    """Run ``dbt source freshness``; any failure to read is an error status."""
    project = os.environ.get("DBT_PROJECT_DIR", "dbt")
    with tempfile.TemporaryDirectory() as target:
        try:
            # A stale source exits nonzero, so the artifact decides the status.
            _ = subprocess.run(
                [
                    str(Path(sys.executable).with_name("dbt")),
                    # Skip listing the model schemas: they are never built under
                    # the fixed namespace, and Snowflake logs each listing as a
                    # failed statement.
                    "--no-populate-cache",
                    "source",
                    "freshness",
                    "--select",
                    source,
                    "--project-dir",
                    project,
                    "--profiles-dir",
                    os.environ.get("DBT_PROFILES_DIR", project),
                    "--target-path",
                    target,
                    "--no-partial-parse",
                ],
                # Parsing resolves every model's schema, which requires a
                # namespace. Freshness only reads the source, so a fixed one
                # that is never built is enough.
                env={**os.environ, "DBT_NAMESPACE": "source_freshness"},
                timeout=TIMEOUT_SECONDS,
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except subprocess.TimeoutExpired:
            return "runtime error", None
        artifact = Path(target) / "sources.json"
        # A failed query leaves no artifact or an empty result list.
        if not artifact.exists():
            return "runtime error", None
        results = sequence(mapping(read_json(artifact)).get("results"))
    if len(results) != 1:
        return "runtime error", None
    result = mapping(results[0])
    age = result.get("max_loaded_at_time_ago_in_s")
    return (
        str(result.get("status")),
        float(age) if isinstance(age, int | float) else None,
    )


# Dagster's optional metadata parameter contains an untyped PathLike.
validation_job = dg.define_asset_job(  # pyright: ignore[reportUnknownMemberType]
    JOB_NAME,
    selection=dg.AssetSelection.checks(refresh_freshness),
    executor_def=dg.in_process_executor,
    tags={"dagster/priority": "20", "dagster/max_retries": "0"},
)
# Checks raw freshness throughout the day, ahead of morning reporting.
# This is not a substitute for published-answer freshness and coverage.
validation_schedule = dg.ScheduleDefinition(
    name="shopify_raw_validation_schedule",
    job=validation_job,
    cron_schedule="*/5 * * * *",
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.STOPPED,
    execution_fn=lambda context: validation_request(context),
)


def validation_request(
    context: dg.ScheduleEvaluationContext,
    job_name: str = JOB_NAME,
) -> dg.RunRequest | dg.SkipReason:
    active = context.instance.get_runs(
        filters=dg.RunsFilter(
            job_name=job_name,
            statuses=[
                dg.DagsterRunStatus.QUEUED,
                dg.DagsterRunStatus.NOT_STARTED,
                dg.DagsterRunStatus.STARTING,
                dg.DagsterRunStatus.STARTED,
                dg.DagsterRunStatus.CANCELING,
            ],
        ),
        limit=1,
    )
    if active:
        return dg.SkipReason("A validation check is queued or running")
    return dg.RunRequest()


defs = dg.Definitions(
    asset_checks=[refresh_freshness],
    jobs=[validation_job],
    schedules=[validation_schedule],
)
