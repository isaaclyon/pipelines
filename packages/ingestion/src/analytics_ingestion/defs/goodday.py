"""Manual GoodDay capture, enabled by listing ``goodday`` in client.yaml.

One asset represents the completed capture, not independently materializable
rows and evidence tables. No schedule or automatic source access is registered.
"""

import os
from collections.abc import Mapping

import dagster as dg

from analytics_ingestion.client import ClientConfig, client_config
from analytics_ingestion.goodday.runtime import SOURCE, load_inventory
from analytics_ingestion.kit.dagster import Metadata, manual_capture


def run(env: Mapping[str, str]) -> Metadata:
    report = load_inventory(env)
    return {
        "capture_id": report.capture_id,
        "row_count": report.row_count,
        "load_ids": list(report.load_ids),
    }


def build_definitions(config: ClientConfig, env: Mapping[str, str]) -> dg.Definitions:
    return manual_capture(
        config,
        env,
        source=SOURCE,
        asset="goodday_inventory_raw",
        job="goodday_inventory_capture",
        kinds={"dlt", "bigquery"},
        description="Retained GoodDay inventory capture with matching load evidence.",
        run=run,
        tags={"analytics/exclusive-resource": "goodday-inventory"},
    )


@dg.definitions
def defs() -> dg.Definitions:
    return build_definitions(client_config(), os.environ)
