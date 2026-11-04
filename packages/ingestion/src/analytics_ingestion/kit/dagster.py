"""One manual, serialized Dagster job per kit source.

The job exists only when the source is in ``ANALYTICS_SOURCES`` and its settings
file (and, remotely, the Snowflake key) is readable. No schedule is registered.
"""

import os
from collections.abc import Callable, Mapping

import dagster as dg

from analytics_ingestion.kit.runtime import enabled, readable, settings_path, target

Metadata = dict[str, str | int | list[str]]
Run = Callable[[Mapping[str, str]], Metadata]


def manual_capture(
    env: Mapping[str, str],
    *,
    source: str,
    asset: str,
    job: str,
    kinds: set[str],
    description: str,
    run: Run,
    local_only: bool = False,
    max_runtime_seconds: int = 3600,
    tags: Mapping[str, str] | None = None,
) -> dg.Definitions:
    """``run`` receives the process environment and returns run metadata."""
    if not enabled(env, source):
        return dg.Definitions()
    files = [
        ("ANALYTICS_SOURCE_SECRETS_ROOT", settings_path(env, source)),
        *target(env, source, local_only=local_only).credential_files(),
    ]
    for name, path in files:
        if not readable(path):
            raise ValueError(f"{name}: {path} must be readable before activation")

    @dg.asset(
        name=asset,
        key_prefix=["raw", source],
        group_name=source,
        kinds=kinds,
        description=description,
    )
    def materialize() -> dg.MaterializeResult[None]:
        return dg.MaterializeResult(value=None, metadata=run(os.environ))

    # The pinned Dagster factory's return type is partially untyped.
    capture = dg.define_asset_job(  # pyright: ignore[reportUnknownMemberType, reportAny]
        name=job,
        selection=[materialize],
        tags={
            "analytics/exclusive-resource": source.replace("_", "-"),
            "dagster/retry_on_asset_or_op_failure": "false",
            # Run monitoring stops a run that outlives its bounded reads and load.
            "dagster/max_runtime": str(max_runtime_seconds),
            **(tags or {}),
        },
    )
    return dg.Definitions(assets=[materialize], jobs=[capture])
