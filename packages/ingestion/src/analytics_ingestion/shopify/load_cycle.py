"""The load cycle every Shopify source runs (ADR 0024).

Orders and customers load the same way. Each source supplies a ``Reader``, its
table declarations (``RawTables``) and its evidence rows; this module builds
the dlt resources, owns the Shopify connection and the shared run report, and
runs the steps in order:

1. A load package a crashed run left is finished first (``kit.writes``), so
   this run still loads its own window.
2. A refresh re-reads open rejects by ID in a separate load without the cursor,
   since a reject is older than the cursor.
3. The run's window loads. A reject seen again at a version already recorded
   is not appended twice.
4. Evidence is appended only after the window loads, so a row proves coverage.
   A run whose cursor had nothing left to read writes none.

A source maps each of its run types onto one read mode:

* ``refresh``: a dlt cursor on ``updated_at`` with a two-minute lag; the first
  run starts at ``start``.
* ``bounded``: exactly ``[start, end)`` on the run's date field; the refresh
  cursor is untouched.
* ``sweep``: every record updated before ``end``, with no cursor.

A reject stays open until a version at least as new loads, or until
reconciliation confirms the record deleted at source after the reject.

``kit.writes`` owns the pending-package, loader-setting and payload-file rules.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import ClassVar, Literal, Protocol

import dlt
from dlt.common.destination.exceptions import DestinationUndefinedEntity
from dlt.common.typing import TTableNames
from dlt.extract.hints import TResourceNestedHints
from dlt.extract.resource import DltResource
from dlt.pipeline.pipeline import Pipeline

from analytics_ingestion.kit.writes import open_writes
from analytics_ingestion.shopify.client import (
    Budget,
    ExecutionLimits,
    RequestStats,
    ShopifyClient,
    Transport,
)
from analytics_ingestion.shopify.errors import SourceError
from analytics_ingestion.shopify.extract import REQUIRED_SCOPES, shop_identity
from analytics_ingestion.shopify.pipeline import (
    LAG_SECONDS,
    MERGE,
    SCHEMA_CONTRACT,
    Column,
    Row,
    column_hints,
    source_failures,
)
from analytics_ingestion.shopify.queries import API_VERSION, WindowField
from analytics_ingestion.shopify.settings import Settings

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
ReadMode = Literal["refresh", "bounded", "sweep"]
MetaValue = str | int | float | None
# Record ID to its newest rejected version.
Recorded = Mapping[str, datetime]


@dataclass(frozen=True)
class Run:
    """One run as the cycle sees it. ``kind`` is the source's own run type and
    is written to ``_extraction_kind``. Refresh uses ``start`` only as its first
    seed; a sweep ignores it."""

    run_id: str
    kind: str
    mode: ReadMode
    field: WindowField
    start: datetime
    end: datetime


@dataclass(frozen=True)
class Window:
    """What a reader is asked to read: ``field`` in ``[start, end)``, where a
    ``start`` of None means all time."""

    field: WindowField
    start: datetime | None
    end: datetime


@dataclass(frozen=True)
class Reject:
    """A record version that failed parsing at ingestion."""

    record_id: str
    version: datetime
    reason: str


@dataclass(frozen=True)
class Tally:
    """A reader's counts for one finished read. ``rejects`` are final: a
    reader leaves out any it saw superseded by a version it kept."""

    records: int
    pages: int
    moved: int
    outside_window: int
    rejects: Sequence[Reject]


# Parsed rows from one source page, each with the time its page was read.
Page = Sequence[tuple[Row, datetime]]


class Reader(Protocol):
    """Reads one source. The cycle asks a fresh reader for each read."""

    def read(self, window: Window) -> Iterator[Page]: ...

    def read_ids(self, ids: Sequence[str]) -> Iterator[Page]:
        """Current versions of these records; a deleted record is absent."""
        ...

    def tally(self) -> Tally:
        """Counts for the read; valid once reading has finished."""
        ...


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


@dataclass(frozen=True)
class RawTables:
    """A source's raw tables. The record table is ``rejects.records``, merged on
    (shop_id, ``rejects.key``); ``null_columns`` are declared metadata
    columns the source never fills."""

    rejects: RejectLedger
    columns: Sequence[Column]
    nested: Mapping[tuple[str, ...], Sequence[Column]]
    reject_columns: Sequence[Column]
    query_version: str
    extraction_revision: int
    null_columns: Sequence[str] = ()


@dataclass
class Connection:
    """The shop a run reads. Each client's request costs reach ``stats``."""

    shop_id: str
    stats: list[RequestStats]
    _make: Callable[[], ShopifyClient]

    def client(self) -> ShopifyClient:
        """A new client; clients are not thread-safe, so one per reader."""
        made = self._make()
        self.stats.append(made.stats)
        return made


def connect(
    settings: Settings,
    scopes: frozenset[str] = REQUIRED_SCOPES,
    *,
    limits: ExecutionLimits,
    transport: Transport | None = None,
    sleeper: Callable[[float], None] = time.sleep,
) -> Connection:
    """Check the credential reaches the configured shop with ``scopes``."""

    def make() -> ShopifyClient:
        return ShopifyClient(
            settings.credentials.shop_domain,
            settings.credentials.access_token,
            Budget(limits, sleeper=sleeper),
            transport,
        )

    stats: list[RequestStats] = []
    first = make()
    stats.append(first.stats)
    return Connection(shop_identity(first, scopes), stats, make)


@dataclass
class CycleReport:
    """Throughput and outcome evidence; counts and costs only, no payloads.
    Each source's run report extends it with its own fields and keys."""

    RECORDS_KEY: ClassVar[str] = "records"

    run_id: str
    kind: str
    window_start: datetime | None = None
    window_end: datetime | None = None
    observed_started_at: datetime | None = None
    observed_finished_at: datetime | None = None
    records: int = 0
    pages: int = 0
    moved: int = 0
    outside_window: int = 0
    rejects: int = 0
    rereads: int = 0
    open_rejects: int = 0
    oldest_open_reject: datetime | None = None
    extract_seconds: float = 0.0
    stats: list[RequestStats] = field(default_factory=list)
    load_ids: list[str] = field(default_factory=list)

    def extra_metadata(self) -> Mapping[str, MetaValue]:
        """A source's own metadata keys, placed before ``load_ids``."""
        return {}

    def metadata(self) -> Mapping[str, MetaValue]:
        floors = [s.min_available for s in self.stats if s.min_available is not None]
        return {
            "kind": self.kind,
            "window_start": _iso(self.window_start),
            "window_end": _iso(self.window_end),
            self.RECORDS_KEY: self.records,
            "rejects": self.rejects,
            "rereads": self.rereads,
            "open_rejects": self.open_rejects,
            "moved": self.moved,
            "outside_window": self.outside_window,
            "pages": self.pages,
            "requests": sum(sum(s.requests.values()) for s in self.stats),
            "throttled": sum(s.throttled for s in self.stats),
            "requested_cost": sum(s.requested_cost for s in self.stats),
            "actual_cost": sum(s.actual_cost for s in self.stats),
            "min_available": min(floors) if floors else None,
            "extract_seconds": round(self.extract_seconds, 3),
            **self.extra_metadata(),
            "load_ids": ",".join(self.load_ids),
        }


def _iso(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()


def run_load_cycle(
    pipeline: Pipeline,
    connection: Connection,
    run: Run,
    tables: RawTables,
    report: CycleReport,
    *,
    reader: Callable[[], Reader],
    evidence: Callable[[], Sequence[DltResource]],
) -> None:
    """Run the four steps, filling ``report``. ``reader`` makes a fresh reader
    for each read; ``evidence`` builds the rows that prove coverage. Source
    failures surface as ShopifyIngestionError."""
    report.stats = connection.stats
    ledger, shop_id = tables.rejects, connection.shop_id
    with source_failures():
        writes = open_writes(pipeline)
        known = open_rejects(pipeline, shop_id, ledger) if run.mode == "refresh" else {}
        if known:
            report.rereads = len(known)
            resource = _resource(
                run, tables, shop_id, reader(), _versions(known), report, sorted(known)
            )
            report.load_ids += writes.run(resource).loads_ids
            known = open_rejects(pipeline, shop_id, ledger)
        window = _resource(run, tables, shop_id, reader(), _versions(known), report)
        report.load_ids += writes.run(window).loads_ids
    still_open = open_rejects(pipeline, shop_id, ledger)
    report.open_rejects = len(still_open)
    report.oldest_open_reject = min(
        (first for _, first in still_open.values()), default=None
    )
    if report.observed_started_at is not None:
        _ = writes.run(list(evidence()))


def _versions(known: Mapping[str, tuple[datetime, datetime]]) -> Recorded:
    return {key: version for key, (version, _) in known.items()}


def _cursor(run: Run) -> dlt.sources.incremental[datetime] | None:
    if run.mode == "refresh":
        return dlt.sources.incremental[datetime](
            "updated_at", initial_value=run.start, lag=LAG_SECONDS
        )
    if run.mode == "bounded":
        # An explicit end makes the run stateless: the refresh cursor is untouched.
        return dlt.sources.incremental[datetime](
            run.field,
            initial_value=run.start,
            end_value=run.end,
            on_cursor_value_missing="include",
        )
    return None


def _resource(
    run: Run,
    tables: RawTables,
    shop_id: str,
    reader: Reader,
    recorded: Recorded,
    report: CycleReport,
    ids: Sequence[str] = (),
) -> DltResource:
    """Merge resource on (shop_id, key); rejects go to an append table.

    Without ``ids`` it reads the run's window and fills ``report``. With
    ``ids`` it re-reads those records without a cursor, so dlt keeps rows older
    than the refresh cursor, and only counts rejects. A record read twice keeps
    its newest ``updated_at`` through dlt's dedup. ``recorded`` maps open
    rejects to their newest rejected version, so a record that fails again at
    that version is not appended twice."""
    ledger = tables.rejects
    nulls: Mapping[str, None] = dict.fromkeys(tables.null_columns, None)
    cursor = None if ids else _cursor(run)

    def rows(
        window: dlt.sources.incremental[datetime] | None = cursor,
    ) -> Iterator[object]:
        began = time.monotonic()
        if ids:
            pages = reader.read_ids(ids)
        else:
            start = (window.start_value if window else None) or (
                EPOCH if run.mode == "sweep" else run.start
            )
            report.window_start, report.window_end = start, run.end
            if start >= run.end:
                return
            report.observed_started_at = datetime.now(UTC)
            sweep = run.mode == "sweep"
            pages = reader.read(Window(run.field, None if sweep else start, run.end))
        for page in pages:
            if page:
                yield [
                    {
                        **row,
                        "_batch_id": run.run_id,
                        **nulls,
                        "_observed_at": observed_at,
                        "_extraction_kind": run.kind,
                        "_query_version": tables.query_version,
                        "_extraction_revision": tables.extraction_revision,
                        "_api_version": API_VERSION,
                    }
                    for row, observed_at in page
                ]
        tally = reader.tally()
        if not ids:
            report.observed_finished_at = datetime.now(UTC)
            report.extract_seconds = time.monotonic() - began
            report.records, report.pages = tally.records, tally.pages
            report.moved, report.outside_window = tally.moved, tally.outside_window
        if not tally.records:
            yield dlt.mark.materialize_table_schema()
        fresh = [
            reject
            for reject in tally.rejects
            if reject.record_id not in recorded
            or recorded[reject.record_id] < reject.version
        ]
        report.rejects += len(fresh)
        rejected_at = datetime.now(UTC)
        for reject in fresh:
            yield dlt.mark.with_table_name(
                {
                    "shop_id": shop_id,
                    ledger.key: reject.record_id,
                    "updated_at": reject.version,
                    "run_id": run.run_id,
                    "error": reject.reason,
                    "rejected_at": rejected_at,
                },
                ledger.table,
            )

    columns = column_hints(tables.columns)
    columns["updated_at"]["dedup_sort"] = "desc"
    nested: dict[TTableNames, TResourceNestedHints] = {
        path: {"columns": column_hints(declared)}
        for path, declared in tables.nested.items()
    }
    resource = dlt.resource(
        rows,
        name=ledger.records,
        write_disposition=MERGE,
        primary_key=("shop_id", ledger.key),
        columns=columns,
        nested_hints=nested,
        schema_contract=SCHEMA_CONTRACT,
    )
    # A variant starts from the resource hints; clear the record columns and
    # keys first, then declare the append-only rejects table.
    for declared in ({}, column_hints(tables.reject_columns)):
        resource.apply_hints(
            table_name=ledger.table,
            write_disposition="append",
            columns=declared,
            primary_key=[],
            nested_hints={},
            create_table_variant=True,
        )
    return resource


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
