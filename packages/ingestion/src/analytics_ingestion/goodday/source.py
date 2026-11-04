"""Complete GoodDay inventory captures through the shared kit capture.

Rows and their count evidence share one load package. Consumers must join evidence
and rows to successful ``_dlt_loads`` entries; visibility alone is not completion.
No cursor, business calculations, or implicit live access belongs here.
"""

from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Protocol

from dlt.common.schema.typing import TTableSchemaColumns

from analytics_ingestion.kit.capture import CaptureResult, Table, capture
from analytics_ingestion.kit.writes import Writes

TABLE = "inventory_by_item"
CAPTURES = "inventory_captures"
TABLE_DESCRIPTION = (
    "Source-shaped GoodDay item inventory observations retained per complete capture."
)
CAPTURE_DESCRIPTION = (
    "Inventory capture counts and source relation; "
    "complete only after dlt load success."
)
METADATA: TTableSchemaColumns = {
    "_capture_id": {
        "data_type": "text",
        "nullable": False,
        "description": "Extraction attempt identity shared by rows and evidence.",
    },
    "_row_number": {
        "data_type": "bigint",
        "nullable": False,
        "description": "One-based position in this capture, not a business key.",
    },
}
EVIDENCE: TTableSchemaColumns = {
    "capture_id": {
        "data_type": "text",
        "nullable": False,
        "description": "Unique identity of the extraction attempt.",
    },
    "captured_at": {
        "data_type": "timestamp",
        "nullable": False,
        "description": "UTC start of result extraction, not vendor freshness.",
    },
    "observed_at": {
        "data_type": "timestamp",
        "nullable": False,
        "description": "UTC time the source read started, not vendor freshness.",
    },
    "source_relation": {
        "data_type": "text",
        "nullable": False,
        "description": "Explicit BigQuery project, dataset and source view read.",
    },
    "row_count": {
        "data_type": "bigint",
        "nullable": False,
        "description": "Rows read in this capture, including zero.",
    },
}


class InventorySnapshot(Protocol):
    """One source read; rows stream from a single query."""

    @property
    def source_relation(self) -> str: ...
    @property
    def observed_at(self) -> datetime: ...
    @property
    def columns(self) -> TTableSchemaColumns: ...
    def rows(self) -> Iterator[dict[str, object]]: ...


def capture_inventory(writes: Writes, snapshot: InventorySnapshot) -> CaptureResult:
    """Retain a new observation; pending-package retries keep their original ID.

    Callers serialize this pipeline. An interrupted extract is not published;
    dlt retains normalized load packages for recovery, not source checkpoints.
    """
    if not snapshot.source_relation:
        raise ValueError("GoodDay source relation is missing")
    if snapshot.observed_at.utcoffset() is None:
        raise ValueError("GoodDay observed_at must include a time zone")
    if any(name.startswith("_") for name in snapshot.columns):
        raise ValueError("GoodDay source column conflicts with reserved metadata")

    def rows() -> Iterator[dict[str, object]]:
        for number, row in enumerate(snapshot.rows(), start=1):
            if any(name.startswith("_") for name in row):
                raise ValueError("GoodDay row conflicts with reserved metadata")
            yield {**row, "_row_number": number}

    return capture(
        writes,
        resource="inventory_capture",
        rows_table=Table(TABLE, TABLE_DESCRIPTION, {**snapshot.columns, **METADATA}),
        rows=rows(),
        evidence_table=Table(CAPTURES, CAPTURE_DESCRIPTION, EVIDENCE),
        evidence={
            "captured_at": datetime.now(UTC),
            "observed_at": snapshot.observed_at,
            "source_relation": snapshot.source_relation,
        },
    )
