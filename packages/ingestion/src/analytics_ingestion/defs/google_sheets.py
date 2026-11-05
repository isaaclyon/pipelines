"""Manual live local Google Sheets captures (ADR 0029).

One asset represents a completed capture of every configured tab. No schedule
or automatic source access is registered; list ``google_sheets`` in
client.yaml to enable it.
"""

import os
from collections.abc import Mapping

import dagster as dg

from analytics_ingestion.client import ClientConfig, client_config
from analytics_ingestion.google_sheets.runtime import load_google_sheets
from analytics_ingestion.google_sheets.spec import SOURCE
from analytics_ingestion.kit.dagster import manual_capture


def build_definitions(config: ClientConfig, env: Mapping[str, str]) -> dg.Definitions:
    return manual_capture(
        config,
        env,
        source=SOURCE,
        asset="google_sheets_raw",
        job="google_sheets_capture",
        kinds={"dlt", "googlesheets"},
        description="Retained Google Sheets captures with matching load evidence.",
        run=lambda run_env: {
            f"{table}_capture_id": capture_id
            for table, capture_id in load_google_sheets(run_env).items()
        },
        local_only=True,
        max_runtime_seconds=600,
    )


@dg.definitions
def defs() -> dg.Definitions:
    return build_definitions(client_config(), os.environ)
