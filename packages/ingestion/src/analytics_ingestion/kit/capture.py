"""Complete snapshot captures: retained rows plus one evidence row per capture.

Rows and their evidence share one load package. Consumers must join evidence and
rows to successful ``_dlt_loads`` entries; visibility alone is not completion.
A failed load keeps its package; ``kit.writes`` loads it before the next read.
"""

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from uuid import uuid4

import dlt
from dlt.common.schema.typing import TTableSchemaColumns

from analytics_ingestion.kit.writes import Writes


@dataclass(frozen=True)
class Table:
    name: str
    description: str
    columns: TTableSchemaColumns


@dataclass(frozen=True)
class CaptureResult:
    capture_id: str
    row_count: int
    load_ids: tuple[str, ...]


def capture(
    writes: Writes,
    *,
    resource: str,
    rows_table: Table,
    rows: Iterable[dict[str, object]],
    evidence_table: Table,
    evidence: dict[str, object],
    expected_rows: int | None = None,
    freeze_columns: bool = False,
) -> CaptureResult:
    """Append one capture; ``_capture_id`` and ``row_count`` are added here.

    The rows table is created even when the first capture is empty, so empty
    means complete and empty.
    """
    capture_id = uuid4().hex
    count = 0

    @dlt.resource(
        name=resource,
        write_disposition="append",
        schema_contract={
            "tables": "evolve",
            "columns": "freeze" if freeze_columns else "evolve",
            "data_type": "freeze",
        },
    )
    def data() -> Iterator[object]:
        nonlocal count
        yield dlt.mark.with_hints(
            dlt.mark.materialize_table_schema(),
            dlt.mark.make_hints(
                table_name=rows_table.name,
                columns=rows_table.columns,
                additional_table_hints={"description": rows_table.description},
            ),
            create_table_variant=True,
        )
        for row in rows:
            count += 1
            yield dlt.mark.with_table_name(
                {**row, "_capture_id": capture_id}, rows_table.name
            )
        if expected_rows is not None and count != expected_rows:
            raise ValueError("Source row count differs from captured rows")
        yield dlt.mark.with_hints(
            {"capture_id": capture_id, **evidence, "row_count": count},
            dlt.mark.make_hints(
                table_name=evidence_table.name,
                columns=evidence_table.columns,
                write_disposition="append",
                additional_table_hints={"description": evidence_table.description},
            ),
            create_table_variant=True,
        )

    info = writes.run(data())
    return CaptureResult(capture_id, count, tuple(info.loads_ids))
