"""Retain one tab's captures with native dlt pending-package recovery.

Consumers require matching capture/load identities and a successful _dlt_loads
entry. Individual visible tables are not evidence of completed loading.
"""

from datetime import UTC, datetime

from dlt.common.schema.typing import TTableSchemaColumns
from dlt.pipeline.pipeline import Pipeline

from analytics_ingestion.google_sheets.client import Clock, Read, read_capture
from analytics_ingestion.google_sheets.spec import SheetSpec
from analytics_ingestion.kit.capture import Table, capture, recover


def columns(spec: SheetSpec) -> TTableSchemaColumns:
    result: TTableSchemaColumns = {
        name: {
            "data_type": "text",
            "nullable": True,
            "description": f"Observed {header!r} source/display text; not normalized.",
        }
        for header, name in spec.fields
    }
    result.update(ROW_IDENTITY)
    return result


ROW_IDENTITY: TTableSchemaColumns = {
    "_capture_id": {
        "data_type": "text",
        "nullable": False,
        "description": "Observation identity shared with capture evidence.",
    },
    "_sheet_row": {
        "data_type": "bigint",
        "nullable": False,
        "description": "Physical sheet row (one-based), not a business key.",
    },
}
EVIDENCE: TTableSchemaColumns = {
    "capture_id": {
        "data_type": "text",
        "nullable": False,
        "description": "Immutable observation identity preserved on recovery.",
    },
    "started_at": {
        "data_type": "timestamp",
        "nullable": False,
        "description": "UTC instant before reading source headers; lower read bound.",
    },
    "observed_at": {
        "data_type": "timestamp",
        "nullable": False,
        "description": "UTC read completion, not business effective time.",
    },
    "spreadsheet_id": {
        "data_type": "text",
        "nullable": False,
        "description": "Explicit Google spreadsheet source identity.",
    },
    "sheet_id": {
        "data_type": "bigint",
        "nullable": False,
        "description": "Numeric identity of the single approved source tab.",
    },
    "row_count": {
        "data_type": "bigint",
        "nullable": False,
        "description": "Populated row count; zero means complete empty membership.",
    },
}


def capture_sheet(
    pipeline: Pipeline,
    read: Read,
    spec: SheetSpec,
    *,
    clock: Clock = lambda: datetime.now(UTC),
) -> str:
    """Recover pending loads, then read one new observation; callers serialize access.

    For recovery without a fresh observation use recover(pipeline), which also
    enforces safe dlt loader configuration. No source I/O or partial rows reach
    dlt until the entire response validates. Blank and duplicate values are retained
    for transformation-owned business checks. Tables are ``<table>_rows`` and
    ``<table>_captures``.
    """
    recover(pipeline)
    observed = read_capture(read, spec, clock=clock)
    return capture(
        pipeline,
        resource=f"{spec.table}_capture",
        rows_table=Table(
            f"{spec.table}_rows",
            "Approved values and observed row membership.",
            columns(spec),
        ),
        rows=observed.rows,
        evidence_table=Table(
            f"{spec.table}_captures",
            "Observations requiring successful dlt loads.",
            EVIDENCE,
        ),
        evidence={
            "started_at": observed.started_at,
            "observed_at": observed.observed_at,
            "spreadsheet_id": spec.spreadsheet_id,
            "sheet_id": spec.sheet_id,
        },
        freeze_columns=True,
    ).capture_id
