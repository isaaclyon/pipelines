"""Explicit live GoodDay configuration from the kit settings file ``goodday.json``.

Remote targets load Snowflake. The local target loads one raw DuckDB file under
the state root, which local dbt attaches (ADR 0029). CI uses fixtures only.
"""

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass, field

from analytics_ingestion.goodday.bigquery import (
    SourceSettings,
    engine_from_info,
    read_inventory,
)
from analytics_ingestion.goodday.source import capture_inventory
from analytics_ingestion.kit.capture import CaptureResult
from analytics_ingestion.kit.runtime import (
    Target,
    mapping,
    read_settings,
    target,
    text,
)
from analytics_ingestion.kit.writes import open_writes

SOURCE = "goodday"
DATASET = "raw_goodday"


@dataclass(frozen=True)
class Settings:
    source: SourceSettings
    target: Target
    service_account: dict[str, object] = field(repr=False)

    @property
    def pipeline_name(self) -> str:
        identity = "|".join(
            (
                self.target.environment,
                self.source.relation,
                *self.target.identity,
                DATASET,
            )
        )
        return "goodday_inventory_" + hashlib.sha256(identity.encode()).hexdigest()[:24]


def settings_from_env(env: Mapping[str, str]) -> Settings:
    """Validate all controls before credentials, source reads, or warehouse writes."""
    destination = target(env, SOURCE)
    values = read_settings(env, SOURCE)
    source = SourceSettings(
        text(values, SOURCE, "project"),
        text(values, SOURCE, "dataset"),
        text(values, SOURCE, "billing_project"),
        text(values, SOURCE, "location"),
    )
    return Settings(source, destination, mapping(values, SOURCE, "service_account"))


def load_inventory(env: Mapping[str, str]) -> CaptureResult:
    """Manual orchestrated run; serialize writers and preserve this state directory."""
    settings = settings_from_env(env)
    pipeline = settings.target.pipeline(settings.pipeline_name, DATASET)
    # Recovery does not issue another billable source query.
    writes = open_writes(pipeline)
    engine = engine_from_info(settings.source, settings.service_account)
    try:
        snapshot = read_inventory(
            engine, settings.source.schema, settings.source.relation
        )
        return capture_inventory(writes, snapshot)
    finally:
        engine.dispose()
