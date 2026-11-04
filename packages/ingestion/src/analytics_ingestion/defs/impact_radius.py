"""Manual local Impact Radius Actions load.

List ``impact_radius`` in ANALYTICS_SOURCES to enable it. No schedule.
"""

import os
from collections.abc import Mapping

import dagster as dg

from analytics_ingestion.impact_radius.source import SOURCE, load_actions
from analytics_ingestion.kit.dagster import manual_capture


def build_definitions(env: Mapping[str, str]) -> dg.Definitions:
    return manual_capture(
        env,
        source=SOURCE,
        asset="impact_radius_actions_raw",
        job="impact_radius_actions_load",
        kinds={"dlt"},
        description="Impact Radius actions from a recent window, merged on Id.",
        run=load_actions,
        local_only=True,
        max_runtime_seconds=900,
    )


defs = build_definitions(os.environ)
