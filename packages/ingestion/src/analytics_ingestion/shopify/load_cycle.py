"""The load cycle every Shopify source runs (ADR 0024).

Orders and customers load the same way. Each source supplies its resources and
evidence; this module owns the order of the steps and what each one keeps:

1. A load package a crashed run left is finished first (``kit.writes``), so
   this run still loads its own window.
2. A refresh re-reads open rejects by ID in a separate load without the cursor,
   since a reject is older than the cursor.
3. The run's window loads. A reject seen again at a version already recorded
   is not appended twice.
4. Evidence is appended only after the window loads, so a row proves coverage.
   A run whose cursor had nothing left to read writes none.

A reject stays open until a version at least as new loads, or until
reconciliation confirms the record deleted at source after the reject.

``kit.writes`` owns the pending-package, loader-setting and payload-file rules.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime

from dlt.common.destination.exceptions import DestinationUndefinedEntity
from dlt.extract.resource import DltResource
from dlt.pipeline.pipeline import Pipeline

from analytics_ingestion.kit.writes import open_writes
from analytics_ingestion.shopify.errors import SourceError
from analytics_ingestion.shopify.pipeline import source_failures

# Record ID to its newest rejected version.
Recorded = Mapping[str, datetime]


@dataclass(frozen=True)
class RejectLedger:
    """Where a source records rejects and what closes one. ``key`` names the
    record identity in all three tables."""

    table: str
    records: str
    key: str
    status_table: str
    deleted_status: str
    checked_at: str


@dataclass(kw_only=True)
class CycleReport:
    """Evidence the cycle and a source's window resource share. Each source's
    run report extends it with its own counts."""

    window_start: datetime | None = None
    window_end: datetime | None = None
    observed_started_at: datetime | None = None
    observed_finished_at: datetime | None = None
    rejects: int = 0
    rereads: int = 0
    open_rejects: int = 0
    oldest_open_reject: datetime | None = None
    load_ids: list[str] = field(default_factory=list)


def run_load_cycle(
    pipeline: Pipeline,
    shop_id: str,
    ledger: RejectLedger,
    kind: str,
    report: CycleReport,
    *,
    reread: Callable[[Sequence[str], Recorded], tuple[DltResource, CycleReport]],
    window: Callable[[Recorded], DltResource],
    evidence: Callable[[], Sequence[DltResource]],
) -> None:
    """Run the four steps, filling ``report``.

    ``reread`` builds a cursorless resource for the given IDs and the report it
    fills; ``window`` builds the run's resource; both skip rejects already
    recorded at a version. ``evidence`` builds the rows that prove coverage.
    Source failures surface as ShopifyIngestionError."""
    with source_failures():
        writes = open_writes(pipeline)
        known = open_rejects(pipeline, shop_id, ledger) if kind == "refresh" else {}
        if known:
            resource, again = reread(sorted(known), _versions(known))
            report.load_ids += writes.run(resource).loads_ids
            report.rereads, report.rejects = len(known), again.rejects
            known = open_rejects(pipeline, shop_id, ledger)
        info = writes.run(window(_versions(known)))
    report.load_ids += info.loads_ids
    still_open = open_rejects(pipeline, shop_id, ledger)
    report.open_rejects = len(still_open)
    report.oldest_open_reject = min(
        (first for _, first in still_open.values()), default=None
    )
    if report.observed_started_at is not None:
        _ = writes.run(list(evidence()))


def _versions(known: Mapping[str, tuple[datetime, datetime]]) -> Recorded:
    return {key: version for key, (version, _) in known.items()}


def open_rejects(
    pipeline: Pipeline, shop_id: str, ledger: RejectLedger
) -> dict[str, tuple[datetime, datetime]]:
    """Record ID to (newest rejected version, first rejected_at) for rejects
    that no loaded version supersedes and that no later confirmed deletion
    closes. Empty before the first rejects are written."""
    with pipeline.sql_client() as client:
        rejects = client.make_qualified_table_name(ledger.table)
        records = client.make_qualified_table_name(ledger.records)
        status = client.make_qualified_table_name(ledger.status_table)
        key = ledger.key
        # The status table appears with the first reconciliation. Keep the
        # resolution in SQL so deletion history is not read into memory.
        try:
            client.execute_sql(f"select {key} from {status} where 1 = 0")
        except DestinationUndefinedEntity:
            resolved = ""
        else:
            resolved = f"""and not exists (
                select 1 from {status} s where s.shop_id = r.shop_id
                and s.{key} = r.{key} and s.status = %s
                and s.{ledger.checked_at} >= r.rejected_at)"""
        arguments = (shop_id, ledger.deleted_status) if resolved else (shop_id,)
        try:
            rows: Sequence[Sequence[object]] | None = client.execute_sql(
                f"""select r.{key}, max(r.updated_at), min(r.rejected_at)
                from {rejects} r where r.shop_id = %s and not exists (
                    select 1 from {records} c where c.shop_id = r.shop_id
                    and c.{key} = r.{key} and c.updated_at >= r.updated_at)
                {resolved}
                group by r.{key}""",
                *arguments,
            )
        except DestinationUndefinedEntity:
            return {}
    result: dict[str, tuple[datetime, datetime]] = {}
    for record_id, version, first in rows or []:
        if not isinstance(version, datetime) or not isinstance(first, datetime):
            raise SourceError("Open rejects returned a non-timestamp value")
        result[str(record_id)] = (version.astimezone(UTC), first.astimezone(UTC))
    return result
