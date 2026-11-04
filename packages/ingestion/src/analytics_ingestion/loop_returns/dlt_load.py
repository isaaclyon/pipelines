"""dlt plumbing: one replace-disposition snapshot table and one evidence table.

``returns`` is replaced by the complete snapshot in a single load package. The
package loads only after extraction succeeds, and ``insert-from-staging`` swaps
the table in one transaction, so a failed extraction leaves the old table.
``return_snapshots`` appends load evidence. Its transaction is separate from the
header replacement; the actual header table, not evidence or restored state,
defines the next removal guard.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Sequence
from datetime import datetime
from typing import Literal, cast

import dlt
from dlt.common.schema.typing import TColumnSchema, TTableSchemaColumns
from dlt.destinations.exceptions import DatabaseUndefinedRelation
from dlt.extract.resource import DltResource
from dlt.pipeline.exceptions import PipelineStepFailed
from dlt.pipeline.pipeline import Pipeline

from analytics_ingestion.loop_returns.checkpoint import SOURCE, Checkpoint
from analytics_ingestion.loop_returns.contract import (
    DECIMAL_PRECISION,
    DECIMAL_SCALE,
    RETURN_COLUMNS,
    Column,
)
from analytics_ingestion.loop_returns.errors import GuardError, LoopIngestionError
from analytics_ingestion.loop_returns.settings import LoopSettings

RETURNS_TABLE = "returns"
SNAPSHOTS_TABLE = "return_snapshots"
SOURCE_NAME = "loop_returns"
REPLACE_STRATEGY: Literal["insert-from-staging"] = "insert-from-staging"

SNAPSHOT_COLUMNS: tuple[Column, ...] = (
    Column("snapshot_id", "text", nullable=False),
    Column("shop_id", "text", nullable=False),
    Column("row_count", "bigint", nullable=False),
    Column("removed_count", "bigint", nullable=False),
    Column("window_count", "bigint", nullable=False),
    Column("snapshot_end", "timestamp", nullable=False),
    Column("completed_at", "timestamp", nullable=False),
)


def column_hints(columns: Sequence[Column]) -> TTableSchemaColumns:
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


def pipeline(settings: LoopSettings, *, candidate: bool = False) -> Pipeline:
    """The shop's pipeline; destination rows define the removal guard."""
    suffix = "_candidate" if candidate else ""
    dataset = settings.dataset.lower() + suffix
    if len(dataset) > 255:
        raise GuardError("Loop candidate dataset exceeds the identifier limit")
    shop_number = settings.shop_id.rsplit("/", 1)[-1]
    return settings.target.pipeline(
        f"loop_returns_dlt_{settings.environment}_{shop_number}{suffix}",
        dataset,
        replace_strategy=REPLACE_STRATEGY,
    )


@dlt.resource(
    name=RETURNS_TABLE,
    table_name=RETURNS_TABLE,
    write_disposition="replace",
    columns=column_hints(RETURN_COLUMNS),
)
def returns(rows: Iterable[dict[str, object]]) -> Iterator[object]:
    yield dlt.mark.materialize_table_schema()
    yield from rows


@dlt.resource(
    name=SNAPSHOTS_TABLE,
    table_name=SNAPSHOTS_TABLE,
    write_disposition="append",
    columns=column_hints(SNAPSHOT_COLUMNS),
)
def snapshot_evidence(evidence: dict[str, object]) -> Iterator[dict[str, object]]:
    """Record load evidence without a second identity inventory in state."""
    yield evidence


@dlt.source(name=SOURCE_NAME, max_table_nesting=0)
def loop_source(
    rows: Iterable[dict[str, object]],
    evidence: dict[str, object],
) -> Sequence[DltResource]:
    return [returns(rows), snapshot_evidence(evidence)]


def run_snapshot(
    pipe: Pipeline,
    rows: Iterable[dict[str, object]],
    evidence: dict[str, object],
) -> None:
    """Load one snapshot, raising the package error behind a failed dlt step."""
    try:
        info = pipe.run(loop_source(rows, evidence))
        info.raise_on_failed_jobs()
    except PipelineStepFailed as error:
        cause: BaseException | None = error
        while cause is not None and not isinstance(cause, LoopIngestionError):
            cause = cause.__cause__ or cause.__context__
        if cause is None:
            raise
        raise cause from error


@dlt.source(name=SOURCE, max_table_nesting=0)
def candidate_window(
    rows: list[dict[str, object]],
    checkpoint: Checkpoint,
    first: bool,
) -> DltResource:
    @dlt.resource(
        name=RETURNS_TABLE,
        write_disposition="replace" if first else "merge",
        primary_key=["shop_id", "return_id"],
        columns=column_hints(RETURN_COLUMNS),
    )
    def headers() -> Iterator[object]:
        yield dlt.mark.materialize_table_schema()
        yield from rows
        state = dlt.current.source_state()
        state["checkpoint"] = checkpoint.state()

    return headers()


def stage_window(
    pipe: Pipeline,
    rows: list[dict[str, object]],
    checkpoint: Checkpoint,
    *,
    first: bool = False,
) -> None:
    info = pipe.run(candidate_window(rows, checkpoint, first))
    info.raise_on_failed_jobs()


def published_result(
    pipe: Pipeline, checkpoint: Checkpoint
) -> tuple[int, datetime] | None:
    try:
        with pipe.sql_client() as client:
            evidence = client.make_qualified_table_name(SNAPSHOTS_TABLE)
            loads = client.make_qualified_table_name("_dlt_loads")
            with client.execute_query(
                "select distinct e.removed_count, e.completed_at, "
                "e.row_count, e.window_count, e.snapshot_end "
                f"from {evidence} e join {loads} l "
                "on e._dlt_load_id = l.load_id "
                "where e.snapshot_id = %s and l.status = 0",
                checkpoint.snapshot_id,
            ) as cursor:
                rows = cast(list[tuple[object, ...]], cursor.fetchall())
    except DatabaseUndefinedRelation:
        return None
    if not rows:
        return None
    if len(rows) != 1:
        raise GuardError("Loop snapshot has conflicting publication evidence")
    removed, completed, count, windows, end = rows[0]
    if (count, windows, end) != (checkpoint.rows, checkpoint.windows, checkpoint.end):
        raise GuardError("Loop publication evidence does not match its checkpoint")
    if type(removed) is not int or removed < 0 or not isinstance(completed, datetime):
        raise GuardError("Loop publication evidence is invalid")
    return removed, completed


def candidate_count(pipe: Pipeline, shop_id: str) -> int:
    with pipe.sql_client() as client:
        table = client.make_qualified_table_name(RETURNS_TABLE)
        with client.execute_query(
            f"select count(*), count(distinct return_id), "
            f"sum(case when shop_id <> %s then 1 else 0 end) from {table}",
            shop_id,
        ) as cursor:
            row = cast(tuple[object, ...], cursor.fetchone())
    count = row[0]
    if type(count) is not int or count != row[1] or row[2] not in (None, 0):
        raise GuardError("Loop candidate has duplicate identities or another shop")
    return count


def removed_count(pipe: Pipeline, candidate: Pipeline, shop_id: str, count: int) -> int:
    with candidate.sql_client() as client:
        candidate_table = client.make_qualified_table_name(RETURNS_TABLE)
    try:
        with pipe.sql_client() as client:
            table = client.make_qualified_table_name(RETURNS_TABLE)
            with client.execute_query(
                "select count(*), sum(case when shop_id <> %s then 1 else 0 end) "
                f"from {table}",
                shop_id,
            ) as cursor:
                previous = cast(tuple[object, ...], cursor.fetchone())
            if previous[1] not in (None, 0):
                raise GuardError(
                    "destination contains another shop; nothing was published"
                )
            if previous[0] != 0 and count == 0:
                raise GuardError(
                    "snapshot is empty; previous nonempty snapshot is unchanged"
                )
            with client.execute_query(
                f"select count(*) from {table} old where not exists "
                f"(select 1 from {candidate_table} new "
                "where old.shop_id = new.shop_id and old.return_id = new.return_id)"
            ) as cursor:
                result = cast(tuple[object, ...], cursor.fetchone())[0]
    except DatabaseUndefinedRelation:
        return 0
    if type(result) is not int:
        raise GuardError("Loop removal count is invalid")
    return result


def candidate_rows(pipe: Pipeline) -> Iterator[dict[str, object]]:
    """Stream only parsed header columns; never buffer the full snapshot."""
    names = [column.name for column in RETURN_COLUMNS]
    with pipe.sql_client() as client:
        table = client.make_qualified_table_name(RETURNS_TABLE)
        with client.execute_query(f"select {', '.join(names)} from {table}") as cursor:
            while rows := cast(list[tuple[object, ...]], cursor.fetchmany(1000)):
                for row in rows:
                    yield dict(zip(names, row, strict=True))
