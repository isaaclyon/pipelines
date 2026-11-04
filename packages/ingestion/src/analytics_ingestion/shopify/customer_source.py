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
``load_cycle`` owns the steps around the read and the dlt resources: reject
re-reads, reject closure and evidence; ``kit.writes`` deletes payload files.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import ClassVar, Literal

import dlt
from dlt.extract.resource import DltResource
from dlt.pipeline.pipeline import Pipeline

from analytics_ingestion.shopify.client import (
    ExecutionLimits,
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
from analytics_ingestion.shopify.customer_parse import parse_customer
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
from analytics_ingestion.shopify.extract import next_cursor
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
from analytics_ingestion.shopify.load_cycle import (
    CycleReport,
    MetaValue,
    Page,
    RawTables,
    ReadMode,
    Reject,
    RejectLedger,
    Run,
    Tally,
    Window,
    connect,
    run_load_cycle,
)
from analytics_ingestion.shopify.pipeline import (
    SCHEMA_CONTRACT,
    Column,
    Row,
    column_hints,
    pipeline,
)
from analytics_ingestion.shopify.queries import API_VERSION, format_instant
from analytics_ingestion.shopify.settings import Settings

EXTRACTION_REVISION = 1
REQUIRED_SCOPES = frozenset({"read_customers"})
# Shopify cannot page past about 25,000 nodes of one connection.
MAX_WINDOW_CUSTOMERS = 25_000
READER_LIMITS = ExecutionLimits(
    max_requests=200_000, max_source_bytes=2 * 1024**3, max_active_seconds=4 * 3600
)
CustomerKind = Literal["refresh", "lookback", "reextract", "sweep"]
# A reject closes once a version at least as new loads, or once two direct
# reads after a sweep confirm the customer deleted after the reject.
REJECTS = RejectLedger(
    REJECTS_TABLE,
    ROOT_TABLE,
    "customer_id",
    STATUS_TABLE,
    "confirmed_unavailable",
    "checked_at",
)
CUSTOMER_TABLES = RawTables(
    REJECTS,
    CUSTOMER_COLUMNS,
    NESTED_TABLES,
    REJECT_COLUMNS,
    CUSTOMER_QUERY_VERSION,
    EXTRACTION_REVISION,
)
# How each run type reads; see ``load_cycle``.
READ_MODES: dict[CustomerKind, ReadMode] = {
    "refresh": "refresh",
    "lookback": "bounded",
    "reextract": "bounded",
    "sweep": "sweep",
}


@dataclass(frozen=True)
class CustomersRun:
    """One run. Refresh resumes from the dlt cursor and uses ``start`` only as
    its first seed. A sweep reads everything updated before ``end``."""

    run_id: str
    kind: CustomerKind
    start: datetime
    end: datetime


# (namespace, key, type) -> [customers, collected]
Inventory = dict[tuple[str, str, str], list[int]]


@dataclass
class Observed:
    rejects: list[Reject] = field(default_factory=list)
    customers: int = 0
    pages: int = 0
    moved: int = 0
    outside_window: int = 0
    inventory: Inventory = field(default_factory=dict)


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
    """Reads customers page by page with complete children. One per client.
    A window read stores its metafield inventory in ``inventory``; a read by
    ID does not."""

    def __init__(
        self,
        client: ShopifyClient,
        shop_id: str,
        groups: frozenset[str],
        window_cap: int = MAX_WINDOW_CUSTOMERS,
        approved_metafields: ApprovedMetafields = frozenset(),
        inventory: Inventory | None = None,
    ) -> None:
        self.approved_metafields = approved_metafields
        self.inventory = inventory
        self.window_cap = window_cap
        self.client = client
        self.shop_id = shop_id
        self.groups = groups
        self.observed = Observed()

    def read(self, window: Window) -> Iterator[Page]:
        """Customers updated in the window (a ``start`` of None means all time).

        Shopify stops a connection near 25,000 nodes, sometimes without an
        error. Each read is therefore capped at ``window_cap`` roots sorted by
        ``updated_at``. A capped read keeps only the roots strictly before the
        second of its last root, and the next read starts at that second, so
        every customer is read once and no read relies on a truncated cursor.
        If one second holds a whole cap of customers the window cannot be
        split and the run fails."""
        start, end = window.start, window.end

        def accept(node: JsonObject) -> bool:
            updated = _updated(node)
            return (window.start is None or window.start <= updated) and updated < end

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
                break
            start = floor
        if self.inventory is not None:
            self.inventory.update(self.observed.inventory)

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

    def tally(self) -> Tally:
        observed = self.observed
        return Tally(
            records=observed.customers,
            pages=observed.pages,
            moved=observed.moved,
            outside_window=observed.outside_window,
            rejects=observed.rejects,
        )

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
                    Reject(customer_id, updated, f"contract: {error}")
                )
                continue
            self.observed.customers += 1
            self._inventory(metafields)
            complete.append((parsed.content, observed_at))
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
class RunReport(CycleReport):
    """The customers run report: the shared counts plus field access and the
    metafield inventory."""

    RECORDS_KEY: ClassVar[str] = "customers"

    access: dict[str, str] = field(default_factory=dict)
    inventory: Inventory = field(default_factory=dict)

    @property
    def customers(self) -> int:
        return self.records

    def extra_metadata(self) -> Mapping[str, MetaValue]:
        denied = sorted(g for g, s in self.access.items() if s == "access_denied")
        unsupported = sorted(g for g, s in self.access.items() if s == "unsupported")
        return {
            "access_denied_groups": ",".join(denied),
            "unsupported_groups": ",".join(unsupported),
        }


def build_pipeline(settings: Settings) -> Pipeline:
    """The customers pipeline; its state lives in the destination."""
    return pipeline(settings, "customers")


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
    connection = connect(
        settings,
        REQUIRED_SCOPES,
        limits=limits or READER_LIMITS,
        transport=transport,
        sleeper=sleeper,
    )
    shop_id = connection.shop_id
    report = RunReport(run.run_id, run.kind)
    report.access = probe_groups(connection.client(), collect_tax_id)
    groups = frozenset(
        g for g, status in report.access.items() if status == "available"
    )
    pipeline = build_pipeline(settings)

    def evidence() -> list[DltResource]:
        ensure_child_tables(pipeline, settings)
        return [
            evidence_resource(run, shop_id, report),
            access_resource(run, shop_id, report),
            inventory_resource(run, shop_id, report),
        ]

    run_load_cycle(
        pipeline,
        connection,
        Run(
            run.run_id, run.kind, READ_MODES[run.kind], "updated_at", run.start, run.end
        ),
        CUSTOMER_TABLES,
        report,
        reader=lambda: CustomerReader(
            connection.client(),
            shop_id,
            groups,
            approved_metafields=approved_metafields,
            inventory=report.inventory,
        ),
        evidence=evidence,
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
    types = _DUCKDB_TYPES if settings.target.snowflake is None else _SNOWFLAKE_TYPES
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
