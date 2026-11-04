"""dlt-native Shopify orders source (ADR 0024).

Pages the Admin GraphQL ``orders`` connection sorted by ``updated_at`` and
follows every nested connection to its final page before an order is kept.
Each page is handed to dlt as it is read, so memory stays flat as runs grow;
dlt keeps the newest version of an order seen twice. Incomplete source data
raises, and dlt discards the run's extract, so a failed run loads nothing.
An order whose content breaks the row contract is appended to the rejects
table while the rest of the run loads. ``load_cycle`` owns the steps around
the read and the dlt resources: reject re-reads, reject closure and evidence.

Windows are closed at the start and open at the end. Shopify's ``<`` matches
the whole end second, so rows at or after the end are dropped here. Several
workers can read contiguous slices of one window, each with its own client.

It writes the environment's raw dataset.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from queue import SimpleQueue
from threading import Event
from typing import ClassVar, Literal

import dlt
from dlt.extract.resource import DltResource
from dlt.pipeline.pipeline import Pipeline

from analytics_ingestion.shopify.client import (
    ExecutionLimits,
    ShopifyClient,
    Transport,
)
from analytics_ingestion.shopify.errors import (
    ConfigurationError,
    SourceError,
)
from analytics_ingestion.shopify.extract import next_cursor
from analytics_ingestion.shopify.jsonvalue import (
    JsonObject,
    as_list,
    as_object,
    gid,
    objects,
    parse_instant,
    required_str,
)
from analytics_ingestion.shopify.load_cycle import (
    EPOCH,
    Connection,
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
from analytics_ingestion.shopify.model import (
    ANCESTOR_DESCRIPTIONS,
    BATCH_EVIDENCE_COLUMNS,
    BATCH_EVIDENCE_TABLE,
    NESTED_TABLES,
    ORDER_COLUMNS,
    ROOT_TABLE,
    SOURCE_STATUS_TABLE,
    OrderException,
    ParsedOrder,
    RefundChildren,
    parse_order,
)
from analytics_ingestion.shopify.pipeline import (
    SCHEMA_CONTRACT,
    Column,
    Row,
    column_hints,
    pipeline,
)
from analytics_ingestion.shopify.queries import (
    API_VERSION,
    ORDER_CHILD_FIELDS,
    PAGED_QUERY_VERSION,
    REFUND_CHILD_FIELDS,
    REFUNDS_PER_HYDRATION_REQUEST,
    WindowField,
    agreement_sales_query,
    order_child_page_query,
    order_lines_page_query,
    orders_page_query,
    refund_child_page_query,
    refund_children_query,
    search_window,
)
from analytics_ingestion.shopify.settings import (
    Settings,
)

EXTRACTION_REVISION = 4
REJECTS_TABLE = "shopify_order_rejects"
REREAD_IDS_PER_SEARCH = 50
# Each reader holds one page, so these stop a runaway reader rather than bound
# memory; the request cap is the main guard.
READER_LIMITS = ExecutionLimits(
    max_source_bytes=1024 * 1024 * 1024, max_active_seconds=60 * 60
)
ExtractionKind = Literal["refresh", "lookback", "reextract"]
# A reject closes once a version at least as new loads, or once count
# reconciliation marks the order unavailable after the reject.
REJECTS = RejectLedger(
    REJECTS_TABLE,
    ROOT_TABLE,
    "order_id",
    SOURCE_STATUS_TABLE,
    "unavailable",
    "detected_at",
)
REJECT_COLUMNS = [
    Column("shop_id", "text", False, description=ANCESTOR_DESCRIPTIONS["shop_id"]),
    Column("order_id", "text", False, description=ANCESTOR_DESCRIPTIONS["order_id"]),
    Column(
        "updated_at",
        "timestamp",
        False,
        description="Shopify updated_at of the rejected order version.",
    ),
    Column(
        "run_id",
        "text",
        False,
        description="Dagster run ID of the run that rejected the version.",
    ),
    Column(
        "error",
        "text",
        False,
        description="Why the version failed, such as the broken field contract.",
    ),
    Column(
        "rejected_at",
        "timestamp",
        False,
        description="UTC instant the run recorded the reject.",
    ),
]

ORDER_TABLES = RawTables(
    REJECTS,
    ORDER_COLUMNS,
    NESTED_TABLES,
    REJECT_COLUMNS,
    PAGED_QUERY_VERSION,
    EXTRACTION_REVISION,
    # Kept from the earlier observation model; the paged source never fills them.
    null_columns=("_observation_sequence", "_content_digest"),
)


@dataclass(frozen=True)
class OrdersRun:
    """One run. Refresh resumes from the dlt cursor and uses ``start`` only as
    the first seed; lookback and reextract read exactly ``[start, end)``."""

    run_id: str
    kind: ExtractionKind
    start: datetime
    end: datetime
    workers: int = 1

    @property
    def field(self) -> WindowField:
        return "created_at" if self.kind == "reextract" else "updated_at"


# Complete orders from one source page, each with the time its page was read.
OrderPage = list[tuple[ParsedOrder, datetime]]


@dataclass
class Observed:
    """Newest version seen per order, plus rejects and evidence. Orders
    themselves go to dlt page by page and are not held here."""

    versions: dict[str, datetime] = field(default_factory=dict)
    rejects: list[OrderException] = field(default_factory=list)
    pages: int = 0
    moved: int = 0
    outside_window: int = 0

    def keep(self, order_id: str, updated_at: datetime) -> None:
        prior = self.versions.get(order_id)
        if prior is None or prior < updated_at:
            self.versions[order_id] = updated_at

    def absorb(self, other: Observed) -> None:
        for order_id, updated_at in other.versions.items():
            self.keep(order_id, updated_at)
        self.rejects.extend(other.rejects)
        self.pages += other.pages
        self.moved += other.moved
        self.outside_window += other.outside_window

    def open_rejects(self) -> list[OrderException]:
        """Rejects not superseded by a loaded observation of the same order."""
        return [
            reject
            for reject in self.rejects
            if reject.order_id not in self.versions
            or self.versions[reject.order_id] < reject.updated_at
        ]


class PageReader:
    """Reads one window slice page by page, recording evidence in ``observed``.
    Clients are not thread-safe; one per reader."""

    def __init__(self, client: ShopifyClient, shop_id: str) -> None:
        self.client = client
        self.shop_id = shop_id
        self.observed = Observed()

    def read(
        self, field: WindowField, start: datetime, end: datetime
    ) -> Iterator[OrderPage]:
        key = "updatedAt" if field == "updated_at" else "createdAt"
        return self._search(
            search_window(field, start, end),
            lambda node: start <= parse_instant(required_str(node, key), key) < end,
        )

    def read_ids(self, order_ids: Sequence[str]) -> Iterator[OrderPage]:
        """Current versions of these orders; a deleted order is simply absent."""
        for offset in range(0, len(order_ids), REREAD_IDS_PER_SEARCH):
            batch = order_ids[offset : offset + REREAD_IDS_PER_SEARCH]
            yield from self._read_batch(set(batch))

    def _read_batch(self, wanted: set[str]) -> Iterator[OrderPage]:
        numbers = [order_id.rsplit("/", 1)[-1] for order_id in sorted(wanted)]
        if not all(number.isdigit() for number in numbers):
            raise SourceError("Rejected order identity is not numeric")
        search = " OR ".join(f"id:{number}" for number in numbers)
        return self._search(search, lambda node: node.get("id") in wanted)

    def _search(
        self, search: str, accept: Callable[[JsonObject], bool]
    ) -> Iterator[OrderPage]:
        result = self.observed
        cursors: set[str] = set()
        cursor: str | None = None
        while True:
            observed_at = datetime.now(UTC)
            data = self.client.query(
                orders_page_query(),
                {"search": search, "after": cursor},
                path="orders_page",
            )
            page = as_object(data.get("orders"), "orders")
            result.pages += 1
            roots: list[JsonObject] = []
            for node in objects(page.get("nodes"), "orders"):
                if accept(node):
                    roots.append(node)
                else:
                    result.outside_window += 1
            yield self._complete(roots, observed_at)
            cursor = next_cursor(page, cursors, "orders")
            if cursor is None:
                return

    def _complete(
        self, roots: Sequence[JsonObject], observed_at: datetime
    ) -> OrderPage:
        result = self.observed
        complete: OrderPage = []
        followed: list[tuple[JsonObject, list[JsonObject]]] = []
        for root in roots:
            order_id = gid(root.get("id"), "Order", "order.id")
            lines = self._follow(order_id, "lineItems", root, order_lines_page_query())
            children: JsonObject = {}
            for name in ORDER_CHILD_FIELDS:
                nodes = self._follow(order_id, name, root, order_child_page_query(name))
                if nodes is not None and name == "agreements":
                    nodes = self._agreements(order_id, nodes)
                if nodes is None:
                    break
                children[name] = nodes
            if lines is None or len(children) != len(ORDER_CHILD_FIELDS):
                # The order changed between pages; a later read sees the new
                # version (re-extraction sort order or the next refresh).
                result.moved += 1
                continue
            followed.append(({**root, **children}, lines))
        refunds = self._refunds(
            [
                gid(item.get("id"), "Refund", "refund.id")
                for data, _ in followed
                for item in objects(data.get("refunds"), "refunds")
            ]
        )
        for data, lines in followed:
            parsed = parse_order(self.shop_id, data, lines, refunds)
            if isinstance(parsed, ParsedOrder):
                result.keep(parsed.order_id, parsed.updated_at)
                complete.append((parsed, observed_at))
            elif parsed.reason == "refund_children_missing":
                result.moved += 1
            elif parsed.reason.startswith("contract:"):
                result.rejects.append(parsed)
            else:
                raise SourceError(
                    f"Incomplete order {parsed.order_id}: {parsed.reason}"
                )
        return complete

    def _follow(
        self, order_id: str, name: str, root: JsonObject, document: str
    ) -> list[JsonObject] | None:
        """Every node of one order connection, or None if the order vanished."""
        page = as_object(root.get(name), name)
        nodes = objects(page.get("nodes"), name)
        cursors: set[str] = set()
        while (cursor := next_cursor(page, cursors, name)) is not None:
            data = self.client.query(
                document, {"id": order_id, "after": cursor}, path=f"order_page:{name}"
            )
            if data.get("order") is None:
                return None
            parent = as_object(data.get("order"), "order page")
            if parent.get("id") != order_id:
                raise SourceError("Order page returned another identity")
            page = as_object(parent.get(name), name)
            nodes.extend(objects(page.get("nodes"), name))
        _unique(nodes, "index" if name == "discountApplications" else "id", name)
        return nodes

    def _agreements(
        self, order_id: str, agreements: Sequence[JsonObject]
    ) -> list[JsonObject] | None:
        result: list[JsonObject] = []
        for agreement in agreements:
            sales = self._sales(order_id, agreement)
            if sales is None:
                return None
            result.append({**agreement, "sales": sales})
        return result

    def _sales(self, order_id: str, agreement: JsonObject) -> list[JsonObject] | None:
        identity = required_str(agreement, "id")
        suffix = identity.rsplit("/", 1)[-1]
        if not suffix.isdigit():
            raise SourceError("Invalid sales agreement identity")
        page = as_object(agreement.get("sales"), "sales")
        sales = objects(page.get("nodes"), "sales")
        cursors: set[str] = set()
        while (cursor := next_cursor(page, cursors, "sales")) is not None:
            data = self.client.query(
                agreement_sales_query(),
                {"id": order_id, "search": f"id:{suffix}", "after": cursor},
                path="sales_page",
            )
            if data.get("order") is None:
                return None
            parent = as_object(data.get("order"), "sales order")
            if parent.get("id") != order_id:
                raise SourceError("Sales page returned another order")
            matches = objects(
                as_object(parent.get("agreements"), "agreements").get("nodes"),
                "agreements",
            )
            if not matches:
                return None
            if len(matches) != 1 or matches[0].get("id") != identity:
                raise SourceError("Sales page did not resolve its agreement")
            page = as_object(matches[0].get("sales"), "sales")
            sales.extend(objects(page.get("nodes"), "sales"))
        _unique(sales, "id", "sales")
        return sales

    def _refunds(self, refund_ids: Sequence[str]) -> dict[str, RefundChildren]:
        """Complete child connections; a refund that vanished is left out."""
        result: dict[str, RefundChildren] = {}
        for offset in range(0, len(refund_ids), REFUNDS_PER_HYDRATION_REQUEST):
            batch = list(refund_ids[offset : offset + REFUNDS_PER_HYDRATION_REQUEST])
            data = self.client.query(
                refund_children_query(), {"ids": batch}, path="refund_children"
            )
            nodes = as_list(data.get("nodes"), "nodes")
            if len(nodes) != len(batch):
                raise SourceError("Refund hydration returned a different node count")
            for refund_id, node in zip(batch, nodes, strict=True):
                if node is None:
                    continue
                refund = as_object(node, "refund")
                if refund.get("id") != refund_id:
                    raise SourceError("Refund hydration returned another identity")
                lines, adjustments, shipping, transactions = (
                    self._refund_pages(refund_id, name, refund)
                    for name in REFUND_CHILD_FIELDS
                )
                result[refund_id] = RefundChildren(
                    lines, adjustments, shipping, transactions
                )
        return result

    def _refund_pages(
        self, refund_id: str, name: str, refund: JsonObject
    ) -> list[JsonObject]:
        page = as_object(refund.get(name), name)
        nodes = objects(page.get("nodes"), name)
        cursors: set[str] = set()
        while (cursor := next_cursor(page, cursors, "refund child")) is not None:
            data = self.client.query(
                refund_child_page_query(name),
                {"id": refund_id, "after": cursor},
                path=f"refund_page:{name}",
            )
            page = as_object(as_object(data.get("node"), "refund").get(name), name)
            nodes.extend(objects(page.get("nodes"), name))
        identities = [str(node["id"]) for node in nodes if node.get("id") is not None]
        if len(set(identities)) != len(identities):
            raise SourceError("Duplicate refund child identity")
        return nodes


def _unique(nodes: Sequence[JsonObject], key: str, label: str) -> None:
    identities = [node.get(key) for node in nodes]
    if None in identities or len(set(map(str, identities))) != len(identities):
        raise SourceError(f"Missing or duplicate {label} identity")


def slices(
    start: datetime, end: datetime, parts: int
) -> list[tuple[datetime, datetime]]:
    """Contiguous whole-second slices covering ``[start, end)``."""
    seconds = int((end - start).total_seconds())
    parts = max(1, min(parts, seconds))
    bounds = [start + timedelta(seconds=seconds * i // parts) for i in range(parts)]
    return list(zip(bounds, [*bounds[1:], end], strict=True))


class OrdersReader:
    """The cycle's reader for orders. A window is split into ``workers``
    contiguous slices, each read by its own ``PageReader`` and client; pages
    reach dlt as any reader hands them over."""

    def __init__(self, connection: Connection, workers: int, report: RunReport) -> None:
        self.connection = connection
        self.workers = workers
        self.report = report
        self.observed = Observed()

    def read(self, window: Window) -> Iterator[Page]:
        parts = slices(window.start or EPOCH, window.end, self.workers)
        self.report.workers = len(parts)
        readers = [self._page_reader() for _ in parts]
        # Readers hand over pages as they read them, then their evidence when
        # done; the first failure stops every reader at its next page.
        handed: SimpleQueue[OrderPage | Observed | Exception] = SimpleQueue()
        stop = Event()

        def work(reader: PageReader, part: tuple[datetime, datetime]) -> None:
            try:
                for page in reader.read(window.field, *part):
                    if stop.is_set():
                        return
                    handed.put(page)
                handed.put(reader.observed)
            except Exception as error:
                handed.put(error)

        with ThreadPoolExecutor(max_workers=len(parts)) as pool:
            for reader, part in zip(readers, parts, strict=True):
                pool.submit(work, reader, part)
            try:
                running = len(parts)
                while running:
                    item = handed.get()
                    if isinstance(item, Exception):
                        raise item
                    if isinstance(item, Observed):
                        self.observed.absorb(item)
                        running -= 1
                    else:
                        yield _rows(item)
            finally:
                stop.set()

    def read_ids(self, ids: Sequence[str]) -> Iterator[Page]:
        reader = self._page_reader()
        for page in reader.read_ids(ids):
            yield _rows(page)
        self.observed.absorb(reader.observed)

    def tally(self) -> Tally:
        observed = self.observed
        return Tally(
            records=len(observed.versions),
            pages=observed.pages,
            moved=observed.moved,
            outside_window=observed.outside_window,
            rejects=[
                Reject(reject.order_id, reject.updated_at, reject.reason)
                for reject in observed.open_rejects()
            ],
        )

    def _page_reader(self) -> PageReader:
        return PageReader(self.connection.client(), self.connection.shop_id)


def _rows(page: OrderPage) -> Page:
    return [(order.content, observed_at) for order, observed_at in page]


@dataclass
class RunReport(CycleReport):
    """The orders run report: the shared counts plus the readers used."""

    RECORDS_KEY: ClassVar[str] = "orders"

    workers: int = 0

    @property
    def orders(self) -> int:
        return self.records

    def extra_metadata(self) -> Mapping[str, MetaValue]:
        return {"workers": self.workers}


def build_pipeline(settings: Settings) -> Pipeline:
    """The orders pipeline; its state lives in the destination."""
    return pipeline(settings, "orders")


def load_orders(
    settings: Settings,
    run: OrdersRun,
    transport: Transport | None = None,
    limits: ExecutionLimits | None = None,
    sleeper: Callable[[float], None] = time.sleep,
) -> RunReport:
    """Read and load one run; source failures surface as ShopifyIngestionError."""
    if run.workers < 1:
        raise ConfigurationError("workers must be at least 1")
    connection = connect(
        settings,
        limits=limits or READER_LIMITS,
        transport=transport,
        sleeper=sleeper,
    )
    report = RunReport(run.run_id, run.kind)
    mode: ReadMode = "refresh" if run.kind == "refresh" else "bounded"
    run_load_cycle(
        build_pipeline(settings),
        connection,
        Run(run.run_id, run.kind, mode, run.field, run.start, run.end),
        ORDER_TABLES,
        report,
        reader=lambda: OrdersReader(connection, run.workers, report),
        evidence=lambda: [evidence_resource(run, connection.shop_id, report)],
    )
    return report


def evidence_resource(run: OrdersRun, shop_id: str, report: RunReport) -> DltResource:
    """Loaded only after the orders load succeeds, so a row proves coverage."""
    row: Row = {
        "shop_id": shop_id,
        "batch_id": run.run_id,
        "kind": run.kind,
        "window_field": run.field,
        "window_start": report.window_start,
        "window_end": report.window_end,
        "status": "complete",
        "completeness": "complete",
        "observation_sequence": None,
        "observed_started_at": report.observed_started_at,
        "observed_finished_at": report.observed_finished_at,
        "completed_at": datetime.now(UTC),
        "orders_observed": report.orders,
        "orders_applied": None,
        "exceptions": report.rejects,
        "anomalies": None,
        "load_ids": ",".join(report.load_ids),
        "api_version": API_VERSION,
        "query_version": PAGED_QUERY_VERSION,
        "extraction_revision": EXTRACTION_REVISION,
    }
    return dlt.resource(
        [row],
        name=BATCH_EVIDENCE_TABLE,
        write_disposition="append",
        columns=column_hints(BATCH_EVIDENCE_COLUMNS),
        schema_contract=SCHEMA_CONTRACT,
    )
