"""dlt-native Shopify customers source (ADR 0024).

An independent pipeline, dataset and state from orders. It pages the Admin
GraphQL ``customers`` connection sorted by ``updated_at`` and follows every
nested connection (addresses, metafields, role assignments) to its final page
before a customer is kept, so a merge replaces a customer's children as a
whole. Incomplete source data raises and dlt discards the extract.

Run kinds:

* ``refresh``: dlt cursor on ``updated_at`` with a two-minute lag; the first
  run starts at the seed.
* ``lookback`` / ``reextract``: an explicit ``[start, end)`` window by
  ``updated_at``; the refresh cursor is untouched.
* ``sweep``: every customer updated before ``end``. Shopify summaries such as
  the order count may not advance ``updatedAt``, so a sweep refreshes them, and
  the customers it misses are the deletion candidates for
  ``customer_reconcile``.

Field groups Shopify denies are probed once per run and left out of the query;
``customer_field_access`` records the result so a denied group is never read
as a source NULL. Log and error text never contains customer values.
"""

from __future__ import annotations

import os
import time
from collections.abc import Callable, Generator, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Literal

import dlt
from dlt.common.destination.exceptions import DestinationUndefinedEntity
from dlt.common.typing import TTableNames
from dlt.extract.hints import TResourceNestedHints
from dlt.extract.resource import DltResource
from dlt.pipeline.pipeline import Pipeline

from analytics_ingestion.shopify.client import (
    Budget,
    ExecutionLimits,
    RequestStats,
    ShopifyClient,
    Transport,
)
from analytics_ingestion.shopify.customer_model import (
    ACCESS_COLUMNS,
    ACCESS_TABLE,
    CUSTOMER_COLUMNS,
    EVIDENCE_COLUMNS,
    EVIDENCE_TABLE,
    EXCLUDED_GROUPS,
    INVENTORY_COLUMNS,
    INVENTORY_TABLE,
    NESTED_TABLES,
    REJECT_COLUMNS,
    REJECTS_TABLE,
    ROOT_TABLE,
    STATUS_TABLE,
    ApprovedMetafields,
    metafield_is_collected,
)
from analytics_ingestion.shopify.customer_parse import ParsedCustomer, parse_customer
from analytics_ingestion.shopify.customer_queries import (
    ALL_RECORDED_GROUPS,
    CUSTOMER_PAGE_SIZE,
    CUSTOMER_QUERY_VERSION,
    IDS_PER_READ,
    PROBED_GROUPS,
    address_page_query,
    customer_fragment,
    customers_page_query,
    metafield_page_query,
    probe_query,
    role_page_query,
)
from analytics_ingestion.shopify.errors import (
    AccessDenied,
    ContractError,
    QueryRejected,
    SourceError,
)
from analytics_ingestion.shopify.extract import next_cursor, shop_identity
from analytics_ingestion.shopify.jsonvalue import (
    DECIMAL_PRECISION,
    DECIMAL_SCALE,
    JsonObject,
    as_list,
    as_object,
    gid,
    objects,
    parse_instant,
    required_str,
)
from analytics_ingestion.shopify.pipeline import (
    LAG_SECONDS,
    MERGE,
    SCHEMA_CONTRACT,
    Column,
    Row,
    column_hints,
    pipeline,
    source_failures,
)
from analytics_ingestion.shopify.queries import API_VERSION, format_instant
from analytics_ingestion.shopify.settings import DuckDbTarget, Settings

EXTRACTION_REVISION = 1
REQUIRED_SCOPES = frozenset({"read_customers"})
# Shopify cannot page past about 25,000 nodes of one connection.
MAX_WINDOW_CUSTOMERS = 25_000
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
READER_LIMITS = ExecutionLimits(
    max_requests=200_000, max_source_bytes=2 * 1024**3, max_active_seconds=4 * 3600
)
CustomerKind = Literal["refresh", "lookback", "reextract", "sweep"]


@dataclass(frozen=True)
class CustomersRun:
    """One run. Refresh resumes from the dlt cursor and uses ``start`` only as
    its first seed. A sweep reads everything updated before ``end``."""

    run_id: str
    kind: CustomerKind
    start: datetime
    end: datetime

    @property
    def window_start(self) -> datetime:
        return EPOCH if self.kind == "sweep" else self.start


Page = list[tuple[ParsedCustomer, datetime]]


@dataclass
class Observed:
    rejects: list[tuple[str, datetime, str]] = field(default_factory=list)
    customers: int = 0
    pages: int = 0
    moved: int = 0
    outside_window: int = 0
    # (namespace, key, type) -> [customers, collected]
    inventory: dict[tuple[str, str, str], list[int]] = field(default_factory=dict)


def probe_groups(client: ShopifyClient, collect_tax_id: bool) -> dict[str, str]:
    """Access status per field group; only ``available`` groups are queried."""
    status: dict[str, str] = {"profile": "available"}
    for group in PROBED_GROUPS:
        if group == "tax_id" and not collect_tax_id:
            status[group] = "excluded"
            continue
        try:
            client.query(probe_query(group), path=f"probe:{group}")
            status[group] = "available"
        except AccessDenied:
            status[group] = "access_denied"
        except QueryRejected:
            status[group] = "unsupported"
    for group in EXCLUDED_GROUPS:
        status[group] = "excluded"
    return status


def _updated(node: JsonObject) -> datetime:
    return parse_instant(required_str(node, "updatedAt"), "updatedAt")


class CustomerReader:
    """Reads customers page by page with complete children. One per client."""

    def __init__(
        self,
        client: ShopifyClient,
        shop_id: str,
        groups: frozenset[str],
        window_cap: int = MAX_WINDOW_CUSTOMERS,
        approved_metafields: ApprovedMetafields = frozenset(),
    ) -> None:
        self.approved_metafields = approved_metafields
        self.window_cap = window_cap
        self.client = client
        self.shop_id = shop_id
        self.groups = groups
        self.observed = Observed()

    def read(
        self,
        start: datetime | None,
        end: datetime,
        accept: Callable[[JsonObject], bool],
    ) -> Iterator[Page]:
        """Customers updated in ``[start, end)`` (``start`` None means all time).

        Shopify stops a connection near 25,000 nodes, sometimes without an
        error. Each read is therefore capped at ``window_cap`` roots sorted by
        ``updated_at``. A capped read keeps only the roots strictly before the
        second of its last root, and the next read starts at that second, so
        every customer is read once and no read relies on a truncated cursor.
        If one second holds a whole cap of customers the window cannot be
        split and the run fails."""
        while True:
            roots, capped = self._fetch(start, end)
            floor = None
            if capped:
                floor = _updated(roots[-1]).replace(microsecond=0)
                roots = [r for r in roots if _updated(r) < floor]
                if not roots:
                    raise SourceError(
                        f"At least {self.window_cap} customers share one updated_at "
                        "second; the window cannot be split"
                    )
            kept: list[JsonObject] = []
            for node in roots:
                if accept(node):
                    kept.append(node)
                else:
                    self.observed.outside_window += 1
            for offset in range(0, len(kept), CUSTOMER_PAGE_SIZE):
                yield self._complete(
                    kept[offset : offset + CUSTOMER_PAGE_SIZE], datetime.now(UTC)
                )
            if floor is None:
                return
            start = floor

    def _fetch(
        self, start: datetime | None, end: datetime
    ) -> tuple[list[JsonObject], bool]:
        """Root nodes of one window in ascending ``updated_at`` order. The flag
        is true when the cap was reached, so the window may hold more."""
        search = f'updated_at:<"{format_instant(end)}"'
        if start is not None:
            search = f'updated_at:>="{format_instant(start)}" {search}'
        cursors: set[str] = set()
        cursor: str | None = None
        roots: list[JsonObject] = []
        previous: datetime | None = None
        while True:
            data = self.client.query(
                customers_page_query(self.groups),
                {"search": search, "after": cursor},
                path="customers_page",
            )
            page = as_object(data.get("customers"), "customers")
            self.observed.pages += 1
            for node in objects(page.get("nodes"), "customers"):
                updated = _updated(node)
                if previous is not None and updated < previous:
                    raise SourceError("Customers page is not sorted by updated_at")
                previous = updated
                roots.append(node)
            if len(roots) >= self.window_cap:
                return roots, True
            cursor = next_cursor(page, cursors, "customers")
            if cursor is None:
                return roots, False

    def read_ids(self, ids: Sequence[str]) -> Iterator[Page]:
        """Current versions of these customers; a deleted customer is absent."""
        query = (
            "query($ids: [ID!]!) { nodes(ids: $ids) { ... on Customer { "
            + customer_fragment(self.groups)
            + " } } }"
        )
        for offset in range(0, len(ids), IDS_PER_READ):
            batch = list(ids[offset : offset + IDS_PER_READ])
            data = self.client.query(query, {"ids": batch}, path="customers_by_id")
            nodes = as_list(data.get("nodes"), "nodes")
            if len(nodes) != len(batch):
                raise SourceError("Direct read returned a different node count")
            found = [as_object(n, "customer") for n in nodes if n]
            self.observed.pages += 1
            yield self._complete(found, datetime.now(UTC))

    def _complete(self, roots: Sequence[JsonObject], observed_at: datetime) -> Page:
        complete: Page = []
        for root in roots:
            customer_id = gid(root.get("id"), "Customer", "customer.id")
            addresses: list[JsonObject] = []
            metafields: list[JsonObject] = []
            contacts: list[tuple[JsonObject, list[JsonObject]]] = []
            if "addresses" in self.groups:
                found = self._follow(
                    customer_id, "addressesV2", root, address_page_query()
                )
                if found is None:
                    self.observed.moved += 1
                    continue
                addresses = found
            if "metafields" in self.groups:
                found = self._follow(
                    customer_id, "metafields", root, metafield_page_query()
                )
                if found is None:
                    self.observed.moved += 1
                    continue
                metafields = found
            if "company_contacts" in self.groups:
                result = self._contacts(root)
                if result is None:
                    self.observed.moved += 1
                    continue
                contacts = result
            try:
                parsed = parse_customer(
                    self.shop_id,
                    root,
                    self.groups,
                    addresses,
                    metafields,
                    contacts,
                    approved_metafields=self.approved_metafields,
                )
            except ContractError as error:
                updated = parse_instant(required_str(root, "updatedAt"), "updatedAt")
                self.observed.rejects.append(
                    (customer_id, updated, f"contract: {error}")
                )
                continue
            self.observed.customers += 1
            self._inventory(metafields)
            complete.append((parsed, observed_at))
        return complete

    def _inventory(self, metafields: Sequence[JsonObject]) -> None:
        seen: set[tuple[str, str, str]] = set()
        for item in metafields:
            key = (
                required_str(item, "namespace"),
                required_str(item, "key"),
                required_str(item, "type"),
            )
            if key in seen:
                continue
            seen.add(key)
            entry = self.observed.inventory.setdefault(
                key,
                [
                    0,
                    int(
                        metafield_is_collected(key[0], key[1], self.approved_metafields)
                    ),
                ],
            )
            entry[0] += 1

    def _follow(
        self, parent_id: str, name: str, root: JsonObject, document: str
    ) -> list[JsonObject] | None:
        """Every node of one connection, or None if the customer vanished."""
        page = as_object(root.get(name), name)
        nodes = objects(page.get("nodes"), name)
        cursors: set[str] = set()
        while (cursor := next_cursor(page, cursors, name)) is not None:
            data = self.client.query(
                document,
                {"id": parent_id, "after": cursor},
                path=f"customer_page:{name}",
            )
            if data.get("node") is None:
                return None
            parent = as_object(data.get("node"), "customer page")
            if parent.get("id") != parent_id:
                raise SourceError("Customer page returned another identity")
            page = as_object(parent.get(name), name)
            nodes.extend(objects(page.get("nodes"), name))
        return nodes

    def _contacts(
        self, root: JsonObject
    ) -> list[tuple[JsonObject, list[JsonObject]]] | None:
        result: list[tuple[JsonObject, list[JsonObject]]] = []
        for contact in objects(root.get("companyContactProfiles"), "contacts"):
            contact_id = required_str(contact, "id")
            roles = self._follow(
                contact_id, "roleAssignments", contact, role_page_query()
            )
            if roles is None:
                return None
            result.append((contact, roles))
        return result


@dataclass
class RunReport:
    """Throughput and outcome evidence; counts and costs only, no payloads."""

    run_id: str
    kind: CustomerKind
    window_start: datetime | None = None
    window_end: datetime | None = None
    observed_started_at: datetime | None = None
    observed_finished_at: datetime | None = None
    customers: int = 0
    rejects: int = 0
    rereads: int = 0
    open_rejects: int = 0
    oldest_open_reject: datetime | None = None
    moved: int = 0
    outside_window: int = 0
    pages: int = 0
    extract_seconds: float = 0.0
    access: dict[str, str] = field(default_factory=dict)
    inventory: dict[tuple[str, str, str], list[int]] = field(default_factory=dict)
    stats: list[RequestStats] = field(default_factory=list)
    load_ids: list[str] = field(default_factory=list)

    def metadata(self) -> Mapping[str, str | int | float | None]:
        floors = [s.min_available for s in self.stats if s.min_available is not None]
        denied = sorted(g for g, s in self.access.items() if s == "access_denied")
        unsupported = sorted(g for g, s in self.access.items() if s == "unsupported")
        return {
            "kind": self.kind,
            "window_start": _iso(self.window_start),
            "window_end": _iso(self.window_end),
            "customers": self.customers,
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
            "access_denied_groups": ",".join(denied),
            "unsupported_groups": ",".join(unsupported),
            "load_ids": ",".join(self.load_ids),
        }


def _iso(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()


def customers_resource(
    run: CustomersRun,
    shop_id: str,
    read: Callable[
        [datetime | None, datetime, Callable[[JsonObject], bool], Observed],
        Iterator[Page],
    ],
    report: RunReport,
    recorded: Mapping[str, datetime] | None = None,
    stateless: bool = False,
    by_id: bool = False,
) -> DltResource:
    """Merge resource on (shop_id, customer_id); rejects go to an append table.

    ``recorded`` maps open rejects to their newest rejected version so a
    customer that fails again at that version is not appended twice. A
    ``stateless`` resource has no cursor (used for reject re-reads)."""
    recorded = recorded or {}
    cursor: dlt.sources.incremental[datetime] | None
    if stateless or run.kind == "sweep":
        cursor = None
    elif run.kind == "refresh":
        cursor = dlt.sources.incremental[datetime](
            "updated_at", initial_value=run.start, lag=LAG_SECONDS
        )
    else:
        cursor = dlt.sources.incremental[datetime](
            "updated_at",
            initial_value=run.start,
            end_value=run.end,
            on_cursor_value_missing="include",
        )

    def rows(
        window: dlt.sources.incremental[datetime] | None = cursor,
    ) -> Iterator[object]:
        start = (window.start_value if window else None) or run.window_start
        report.window_start, report.window_end = start, run.end
        if start >= run.end:
            return
        began = time.monotonic()
        report.observed_started_at = datetime.now(UTC)
        observed = Observed()

        def accept(node: JsonObject) -> bool:
            updated = parse_instant(required_str(node, "updatedAt"), "updatedAt")
            return (run.kind == "sweep" or start <= updated) and updated < run.end

        for page in read(
            None if by_id or run.kind == "sweep" else start, run.end, accept, observed
        ):
            if page:
                yield [
                    {
                        **customer.content,
                        "_batch_id": run.run_id,
                        "_observed_at": observed_at,
                        "_extraction_kind": run.kind,
                        "_query_version": CUSTOMER_QUERY_VERSION,
                        "_extraction_revision": EXTRACTION_REVISION,
                        "_api_version": API_VERSION,
                    }
                    for customer, observed_at in page
                ]
        report.observed_finished_at = datetime.now(UTC)
        report.extract_seconds = time.monotonic() - began
        report.pages, report.moved = observed.pages, observed.moved
        report.outside_window = observed.outside_window
        report.customers = observed.customers
        report.inventory = observed.inventory
        if not observed.customers:
            yield dlt.mark.materialize_table_schema()
        fresh = [
            item
            for item in observed.rejects
            if item[0] not in recorded or recorded[item[0]] < item[1]
        ]
        report.rejects += len(fresh)
        rejected_at = datetime.now(UTC)
        for customer_id, updated_at, error in fresh:
            yield dlt.mark.with_table_name(
                {
                    "shop_id": shop_id,
                    "customer_id": customer_id,
                    "updated_at": updated_at,
                    "run_id": run.run_id,
                    "error": error,
                    "rejected_at": rejected_at,
                },
                REJECTS_TABLE,
            )

    columns = column_hints(CUSTOMER_COLUMNS)
    columns["updated_at"]["dedup_sort"] = "desc"
    nested: dict[TTableNames, TResourceNestedHints] = {
        path: {"columns": column_hints(cols)} for path, cols in NESTED_TABLES.items()
    }
    resource = dlt.resource(
        rows,
        name=ROOT_TABLE,
        write_disposition=MERGE,
        primary_key=("shop_id", "customer_id"),
        columns=columns,
        nested_hints=nested,
        schema_contract=SCHEMA_CONTRACT,
    )
    for cols in ({}, column_hints(REJECT_COLUMNS)):
        resource.apply_hints(
            table_name=REJECTS_TABLE,
            write_disposition="append",
            columns=cols,
            primary_key=[],
            nested_hints={},
            create_table_variant=True,
        )
    return resource


def build_pipeline(settings: Settings) -> Pipeline:
    """The customers pipeline; its state lives in the destination."""
    return pipeline(settings, "customers")


DELETE_COMPLETED_JOBS_ENV = "LOAD__DELETE_COMPLETED_JOBS"


@contextmanager
def delete_completed_jobs(pipeline: Pipeline) -> Generator[None]:
    """Keep no customer payload files of a loaded package.

    dlt reads ``load.delete_completed_jobs`` when its load step starts and
    exposes no per-pipeline argument, so the setting applies for the scope of
    this customers pipeline only and the previous value is restored. dlt keeps
    the files of a package with a failed job and never touches pending
    packages, so recovery still works. Completed packages that earlier runs
    left behind are cleaned once on entry."""
    previous = os.environ.get(DELETE_COMPLETED_JOBS_ENV)
    os.environ[DELETE_COMPLETED_JOBS_ENV] = "true"
    try:
        purge_completed_jobs(pipeline)
        yield
    finally:
        if previous is None:
            _ = os.environ.pop(DELETE_COMPLETED_JOBS_ENV, None)
        else:
            os.environ[DELETE_COMPLETED_JOBS_ENV] = previous


def purge_completed_jobs(pipeline: Pipeline) -> None:
    """Remove job files of loaded packages in this pipeline's own working
    directory. A package with a failed job keeps its files."""
    storage = pipeline._get_load_storage()  # pyright: ignore[reportPrivateUsage]
    for load_id in pipeline.list_completed_load_packages():
        try:
            storage.loaded_packages.remove_completed_jobs(load_id)
        except NotADirectoryError:
            continue  # already cleaned


def load_customers(
    settings: Settings,
    run: CustomersRun,
    transport: Transport | None = None,
    limits: ExecutionLimits | None = None,
    sleeper: Callable[[float], None] = time.sleep,
    collect_tax_id: bool = False,
    approved_metafields: ApprovedMetafields = frozenset(),
) -> RunReport:
    """Read and load one run; source failures surface as ShopifyIngestionError."""
    report = RunReport(run.run_id, run.kind)

    def client() -> ShopifyClient:
        made = ShopifyClient(
            settings.credentials.shop_domain,
            settings.credentials.access_token,
            Budget(limits or READER_LIMITS, sleeper=sleeper),
            transport,
        )
        report.stats.append(made.stats)
        return made

    probe = client()
    shop_id = shop_identity(probe, REQUIRED_SCOPES)
    report.access = probe_groups(probe, collect_tax_id)
    groups = frozenset(
        g for g, status in report.access.items() if status == "available"
    )

    def read(
        start: datetime | None,
        end: datetime,
        accept: Callable[[JsonObject], bool],
        observed: Observed,
    ) -> Iterator[Page]:
        reader = CustomerReader(
            client(), shop_id, groups, approved_metafields=approved_metafields
        )
        yield from reader.read(start, end, accept)
        _absorb(observed, reader.observed)

    pipeline = build_pipeline(settings)
    with delete_completed_jobs(pipeline):
        with source_failures():
            # dlt loads a package left by a crashed run and then exits without
            # extracting, so finish it first and keep this run's window.
            if pipeline.has_pending_data:
                pipeline.normalize()
                pipeline.load()
            known = open_rejects(pipeline, shop_id) if run.kind == "refresh" else {}
            if known:
                ids, again = sorted(known), RunReport(run.run_id, run.kind)

                def read_ids(
                    start: datetime | None,
                    end: datetime,
                    accept: Callable[[JsonObject], bool],
                    observed: Observed,
                ) -> Iterator[Page]:
                    del start, end, accept
                    reader = CustomerReader(
                        client(),
                        shop_id,
                        groups,
                        approved_metafields=approved_metafields,
                    )
                    yield from reader.read_ids(ids)
                    _absorb(observed, reader.observed)

                reread = customers_resource(
                    run,
                    shop_id,
                    read_ids,
                    again,
                    {cid: version for cid, (version, _) in known.items()},
                    stateless=True,
                    by_id=True,
                )
                report.load_ids += pipeline.run(reread).loads_ids
                report.rereads, report.rejects = len(ids), again.rejects
                known = open_rejects(pipeline, shop_id)
            recorded = {cid: version for cid, (version, _) in known.items()}
            info = pipeline.run(
                customers_resource(run, shop_id, read, report, recorded)
            )
        report.load_ids += info.loads_ids
        still_open = open_rejects(pipeline, shop_id)
        report.open_rejects = len(still_open)
        report.oldest_open_reject = min(
            (first for _, first in still_open.values()), default=None
        )
        if report.observed_started_at is not None:
            ensure_child_tables(pipeline, settings)
            pipeline.run(
                [
                    evidence_resource(run, shop_id, report),
                    access_resource(run, shop_id, report),
                    inventory_resource(run, shop_id, report),
                ]
            )
        return report


_DUCKDB_TYPES = {
    "text": "varchar",
    "timestamp": "timestamp with time zone",
    "bool": "boolean",
    "bigint": "bigint",
    "decimal": f"decimal({DECIMAL_PRECISION}, {DECIMAL_SCALE})",
}
_SNOWFLAKE_TYPES = {
    "text": "varchar",
    "timestamp": "timestamp_tz",
    "bool": "boolean",
    "bigint": "number(19, 0)",
    "decimal": f"number({DECIMAL_PRECISION}, {DECIMAL_SCALE})",
}
_DLT_CHILD_COLUMNS = ("_dlt_root_id", "_dlt_parent_id", "_dlt_list_idx", "_dlt_id")


def ensure_child_tables(pipeline: Pipeline, settings: Settings) -> None:
    """Create declared child tables that no row has needed yet, so consumers can
    query a store with no addresses, metafields or company contacts. dlt
    creates a nested table only when a row first needs it; a later load finds
    the table, with the same columns dlt would have created, and fills it."""
    types = (
        _DUCKDB_TYPES
        if isinstance(settings.destination, DuckDbTarget)
        else _SNOWFLAKE_TYPES
    )
    with pipeline.sql_client() as client:
        for path, columns in NESTED_TABLES.items():
            name = client.make_qualified_table_name(f"{ROOT_TABLE}__{'__'.join(path)}")
            declared = [
                f"{client.escape_column_name(c.name)} {types[c.data_type]}"
                for c in columns
            ]
            dlt_columns = [
                f"{client.escape_column_name(c)} "
                f"{types['bigint'] if c == '_dlt_list_idx' else types['text']}"
                for c in _DLT_CHILD_COLUMNS
            ]
            client.execute_sql(
                f"create table if not exists {name} "
                f"({', '.join([*declared, *dlt_columns])})"
            )


def _absorb(into: Observed, other: Observed) -> None:
    into.rejects.extend(other.rejects)
    into.customers += other.customers
    into.pages += other.pages
    into.moved += other.moved
    into.outside_window += other.outside_window
    for key, (count, collected) in other.inventory.items():
        entry = into.inventory.setdefault(key, [0, collected])
        entry[0] += count


def open_rejects(
    pipeline: Pipeline, shop_id: str
) -> dict[str, tuple[datetime, datetime]]:
    """Customer ID to (newest rejected version, first rejected_at) for rejects
    that no loaded version supersedes and that no later
    ``confirmed_unavailable`` status closes (a deletion proven by two direct
    reads)."""
    with pipeline.sql_client() as client:
        rejects = client.make_qualified_table_name(REJECTS_TABLE)
        customers = client.make_qualified_table_name(ROOT_TABLE)
        status = client.make_qualified_table_name(STATUS_TABLE)
        # Legacy pipelines may not have a status table before reconciliation.
        # Keep resolution in SQL so deleted-customer history is not loaded into
        # memory on every refresh.
        try:
            client.execute_sql(f"select customer_id from {status} where 1 = 0")
        except DestinationUndefinedEntity:
            resolved = ""
        else:
            resolved = f"""and not exists (
                select 1 from {status} s where s.shop_id = r.shop_id
                and s.customer_id = r.customer_id
                and s.status = 'confirmed_unavailable'
                and s.checked_at >= r.rejected_at)"""
        try:
            rows: Sequence[Sequence[object]] | None = client.execute_sql(
                f"""select r.customer_id, max(r.updated_at), min(r.rejected_at)
                from {rejects} r where r.shop_id = %s and not exists (
                    select 1 from {customers} c where c.shop_id = r.shop_id
                    and c.customer_id = r.customer_id and c.updated_at >= r.updated_at)
                {resolved}
                group by r.customer_id""",
                shop_id,
            )
        except DestinationUndefinedEntity:
            return {}
    result: dict[str, tuple[datetime, datetime]] = {}
    for customer_id, version, first in rows or []:
        if not isinstance(version, datetime) or not isinstance(first, datetime):
            raise SourceError("Open rejects returned a non-timestamp value")
        result[str(customer_id)] = (version.astimezone(UTC), first.astimezone(UTC))
    return result


def _append(name: str, rows: list[Row], columns: Sequence[Column]) -> DltResource:
    return dlt.resource(
        rows,
        name=name,
        write_disposition="append",
        columns=column_hints(columns),
        schema_contract=SCHEMA_CONTRACT,
    )


def evidence_resource(
    run: CustomersRun, shop_id: str, report: RunReport
) -> DltResource:
    """Loaded only after the customers load succeeds, so a row proves coverage."""
    row: Row = {
        "shop_id": shop_id,
        "batch_id": run.run_id,
        "kind": run.kind,
        "window_field": "updated_at",
        "window_start": report.window_start,
        "window_end": report.window_end,
        "status": "complete",
        "completeness": "partial" if report.moved else "complete",
        "is_full_sweep": run.kind == "sweep",
        "observed_started_at": report.observed_started_at,
        "observed_finished_at": report.observed_finished_at,
        "completed_at": datetime.now(UTC),
        "customers_observed": report.customers,
        # Suppressed duplicate reject events still leave unresolved profiles.
        "rejects": max(report.rejects, report.open_rejects),
        "pages": report.pages,
        "load_ids": ",".join(report.load_ids),
        "api_version": API_VERSION,
        "query_version": CUSTOMER_QUERY_VERSION,
        "extraction_revision": EXTRACTION_REVISION,
    }
    return _append(EVIDENCE_TABLE, [row], EVIDENCE_COLUMNS)


def access_resource(run: CustomersRun, shop_id: str, report: RunReport) -> DltResource:
    checked = datetime.now(UTC)
    rows: list[Row] = [
        {
            "shop_id": shop_id,
            "batch_id": run.run_id,
            "field_group": group,
            "status": report.access.get(group, "unsupported"),
            "checked_at": checked,
        }
        for group in ALL_RECORDED_GROUPS
    ]
    return _append(ACCESS_TABLE, rows, ACCESS_COLUMNS)


def inventory_resource(
    run: CustomersRun, shop_id: str, report: RunReport
) -> DltResource:
    rows: list[Row] = [
        {
            "shop_id": shop_id,
            "batch_id": run.run_id,
            "namespace": namespace,
            "key": key,
            "type": kind,
            "customers": count,
            "collected": bool(collected),
        }
        for (namespace, key, kind), (count, collected) in sorted(
            report.inventory.items()
        )
    ]
    resource = _append(INVENTORY_TABLE, rows, INVENTORY_COLUMNS)
    if not rows:
        resource = dlt.resource(
            [dlt.mark.materialize_table_schema()],
            name=INVENTORY_TABLE,
            write_disposition="append",
            columns=column_hints(INVENTORY_COLUMNS),
            schema_contract=SCHEMA_CONTRACT,
        )
    return resource
