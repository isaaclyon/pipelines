"""dlt plumbing shared by the order and customer sources.

Column declarations, their dlt hints, the write settings every raw table uses,
and the pipeline each source builds. Each source keeps its own pipeline name,
so state and load packages stay separate.
"""

from __future__ import annotations

from collections.abc import Generator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Literal

import dlt
from dlt.common.destination.reference import TDestinationReferenceArg
from dlt.common.schema.typing import (
    TColumnSchema,
    TSchemaContract,
    TTableSchemaColumns,
    TWriteDispositionConfig,
)
from dlt.pipeline.exceptions import PipelineStepFailed
from dlt.pipeline.pipeline import Pipeline

from analytics_ingestion.shopify.errors import ShopifyIngestionError
from analytics_ingestion.shopify.jsonvalue import DECIMAL_PRECISION, DECIMAL_SCALE
from analytics_ingestion.shopify.settings import DuckDbTarget, Settings

ColumnType = Literal["text", "timestamp", "bool", "bigint", "decimal"]
Row = dict[str, object]

SCHEMA_CONTRACT: TSchemaContract = {
    "tables": "evolve",
    "columns": "evolve",
    "data_type": "freeze",
}
MERGE: TWriteDispositionConfig = {"disposition": "merge", "strategy": "delete-insert"}
LAG_SECONDS = 120


@dataclass(frozen=True)
class Column:
    """A raw column. ``description`` documents its meaning for readers."""

    name: str
    data_type: ColumnType
    nullable: bool = True
    description: str = field(kw_only=True)


def column_hints(columns: Sequence[Column]) -> TTableSchemaColumns:
    """dlt column hints; decimals use the fixed raw precision on every target."""
    hints: TTableSchemaColumns = {}
    for item in columns:
        column: TColumnSchema = {
            "name": item.name,
            "data_type": item.data_type,
            "nullable": item.nullable,
        }
        if item.data_type == "decimal":
            column["precision"] = DECIMAL_PRECISION
            column["scale"] = DECIMAL_SCALE
        hints[item.name] = column
    return hints


def pipeline(settings: Settings, name: str) -> Pipeline:
    """The ``name`` pipeline for the configured shop; its state lives in the
    destination."""
    root = settings.target_root / "dlt_source"
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    handle = settings.shop_handle.replace("-", "_")
    return dlt.pipeline(
        pipeline_name=f"shopify_{name}_dlt_{settings.environment}_{handle}",
        pipelines_dir=str(root),
        destination=_destination(settings),
        dataset_name=settings.dataset.lower(),
    )


def _destination(settings: Settings) -> TDestinationReferenceArg:
    target = settings.destination
    if isinstance(target, DuckDbTarget):
        target.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        return dlt.destinations.duckdb(str(target.path))
    return dlt.destinations.snowflake(
        credentials={
            "database": target.database,
            "username": target.user,
            "host": target.account,
            "warehouse": target.warehouse,
            "role": target.role,
            "private_key_path": str(target.private_key_path),
        }
    )


@contextmanager
def source_failures() -> Generator[None]:
    """Raise the source error behind a failed dlt step in its place, so callers
    see the ShopifyIngestionError rather than dlt's wrapper."""
    try:
        yield
    except PipelineStepFailed as error:
        cause: BaseException | None = error
        while cause is not None and not isinstance(cause, ShopifyIngestionError):
            cause = cause.__cause__ or cause.__context__
        if cause is None:
            raise
        raise cause from error
