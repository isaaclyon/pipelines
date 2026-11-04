"""Complete snapshot captures: retained rows plus one evidence row per capture.

Rows and their evidence share one load package. Consumers must join evidence and
rows to successful ``_dlt_loads`` entries; visibility alone is not completion.
A failed load keeps its package, and the next run loads it before reading again.
"""

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from uuid import uuid4

import dlt
from dlt.common.configuration import resolve_configuration
from dlt.common.configuration.resolve import inject_section
from dlt.common.configuration.specs import ConfigSectionContext
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.load.configuration import LoaderConfiguration
from dlt.pipeline.pipeline import Pipeline


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


def recover(pipeline: Pipeline) -> None:
    """Finish durable packages before reading the source again."""
    # Resolve the same loader configuration as dlt. In 1.31.0 the loader's
    # environment configuration can override even an explicit load() argument.
    with inject_section(
        ConfigSectionContext(pipeline_name=pipeline.pipeline_name, sections=("load",))
    ):
        config = resolve_configuration(LoaderConfiguration(), accept_partial=True)
    if not config.raise_on_failed_jobs or config.auto_abort_on_terminal_error:
        raise ValueError(
            "Capture requires dlt raise_on_failed_jobs=true and "
            "auto_abort_on_terminal_error=false for safe completion and recovery"
        )
    if pipeline.has_pending_data:
        pipeline.normalize()
        info = pipeline.load(raise_on_failed_jobs=True)
        info.raise_on_failed_jobs()


def capture(
    pipeline: Pipeline,
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

    Callers recover first and serialize this pipeline. The rows table is created
    even when the first capture is empty, so empty means complete and empty.
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

    info = pipeline.run(data())
    info.raise_on_failed_jobs()
    return CaptureResult(capture_id, count, tuple(info.loads_ids))
