"""Weekly count reconciliation for the dlt-native orders source (ADR 0024).

Each UTC creation month up to the latest complete refresh compares Shopify's
exact ``ordersCount`` with raw orders not marked unavailable. Every order
created before that refresh's window end is loaded, so equal months need
nothing more. Differing months read their raw orders directly by ID, so the
cost follows what raw holds, not what Shopify holds. A raw order both reads
miss is marked unavailable; raw rows are never removed. Shopify's count minus
the raw orders it still returns is the gap a reextract must fill.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import dlt
from dlt.common.destination.exceptions import DestinationUndefinedEntity
from dlt.pipeline.pipeline import Pipeline

from analytics_ingestion.shopify.client import (
    Budget,
    ExecutionLimits,
    ShopifyClient,
    Transport,
)
from analytics_ingestion.shopify.errors import ConfigurationError, SourceError
from analytics_ingestion.shopify.extract import count_orders, shop_identity
from analytics_ingestion.shopify.jsonvalue import as_list, as_object, objects
from analytics_ingestion.shopify.model import (
    BATCH_EVIDENCE_TABLE,
    ROOT_TABLE,
    SOURCE_STATUS_COLUMNS,
    SOURCE_STATUS_TABLE,
)
from analytics_ingestion.shopify.pipeline import (
    MERGE,
    SCHEMA_CONTRACT,
    Row,
    column_hints,
)
from analytics_ingestion.shopify.queries import (
    EARLIEST_ORDER,
    ORDER_VERSIONS,
    VERSION_CHECK_IDS_PER_REQUEST,
    search_window,
)
from analytics_ingestion.shopify.settings import Settings
from analytics_ingestion.shopify.source import build_pipeline

# More absences than this suggests lost access, not deletions; nothing is marked.
MAX_CANDIDATES = 500
COUNT_LIMIT = 10_000_000
ABSENT = "absent from two direct reads"
BACK = "returned by a direct read"


@dataclass
class ReconcileReport:
    """Counts only. ``unexplained`` is what a reextract or a person must resolve."""

    run_id: str
    end: datetime | None = None
    months: int = 0
    differing_months: int = 0
    source_only: int = 0
    confirmed: int = 0
    unconfirmed: int = 0
    reappeared: int = 0

    @property
    def unexplained(self) -> int:
        return self.source_only + self.unconfirmed

    def metadata(self) -> Mapping[str, str | int | None]:
        return {
            "end": None if self.end is None else self.end.isoformat(),
            "months": self.months,
            "differing_months": self.differing_months,
            "source_only": self.source_only,
            "confirmed": self.confirmed,
            "unconfirmed": self.unconfirmed,
            "reappeared": self.reappeared,
            "unexplained": self.unexplained,
        }


def reconcile(
    settings: Settings,
    run_id: str,
    transport: Transport | None = None,
    limits: ExecutionLimits | None = None,
    sleeper: Callable[[float], None] = time.sleep,
    max_candidates: int = MAX_CANDIDATES,
) -> ReconcileReport:
    client = ShopifyClient(
        settings.credentials.shop_domain,
        settings.credentials.access_token,
        Budget(limits or ExecutionLimits(), sleeper=sleeper),
        transport,
    )
    shop_id = shop_identity(client)
    pipeline = build_pipeline(settings)
    report = ReconcileReport(run_id)
    [(end, raw_first)] = _sql(
        pipeline,
        "select (select max(window_end) from {batches} where shop_id = %s and kind ="
        " 'refresh' and status = 'complete'), (select min(created_at) from {orders}"
        " where shop_id = %s)",
        shop_id,
        shop_id,
    ) or [(None, None)]
    if not isinstance(end, datetime):
        raise ConfigurationError("Reconciliation needs a complete refresh first")
    report.end = end = end.astimezone(UTC)
    marks: dict[str, tuple[str, datetime | None]] = {
        str(order_id): (str(status), _instant(created))
        for order_id, status, created in _sql(
            pipeline,
            "select order_id, status, created_at from {status} where shop_id = %s",
            shop_id,
        )
    }
    gone = {
        key: made for key, (status, made) in marks.items() if status == "unavailable"
    }
    candidates: dict[str, datetime] = {}
    reappeared: dict[str, datetime] = {}
    for start, stop in _months(_first(client, _instant(raw_first)), end):
        report.months += 1
        between = (
            "from {orders} where shop_id = %s and created_at >= %s and created_at < %s"
        )
        [(raw,)] = _sql(pipeline, f"select count(*) {between}", shop_id, start, stop)
        removed = sum(1 for made in gone.values() if made and start <= made < stop)
        search = search_window("created_at", start, stop)
        source, exact = count_orders(client, search, COUNT_LIMIT)
        if not exact:
            raise SourceError("ordersCount was not exact for a creation month")
        if source == int(str(raw)) - removed:
            continue
        report.differing_months += 1
        loaded = {
            str(key): _instant(made) or start
            for key, made in _sql(
                pipeline, f"select order_id, created_at {between}", shop_id, start, stop
            )
        }
        found = _present(client, sorted(loaded))
        report.source_only += max(0, source - len(found))
        reappeared.update({key: loaded[key] for key in found & gone.keys()})
        candidates.update(
            {key: loaded[key] for key in loaded.keys() - found - gone.keys()}
        )
    if len(candidates) > max_candidates:
        raise SourceError(
            f"{len(candidates)} deletion candidates; access loss suspected, none marked"
        )
    shop_identity(client)  # Re-read scopes: a grant revoked mid-run blocks marks.
    present = _present(client, sorted(candidates))
    report.confirmed = len(candidates) - len(present)
    report.unconfirmed = len(present)
    report.reappeared = len(reappeared)
    now = datetime.now(UTC)
    marked = [
        *(
            (key, made, "unavailable", ABSENT)
            for key, made in candidates.items()
            if key not in present
        ),
        *((key, made, "available", BACK) for key, made in reappeared.items()),
    ]
    status_rows: list[Row] = [
        {
            "shop_id": shop_id,
            "order_id": key,
            "status": status,
            "created_at": made,
            "detected_at": now,
            "evidence_batch_id": run_id,
            "detail": detail,
        }
        for key, made, status, detail in marked
    ]
    if status_rows:
        pipeline.run(
            dlt.resource(
                status_rows,
                name=SOURCE_STATUS_TABLE,
                write_disposition=MERGE,
                primary_key=("shop_id", "order_id"),
                columns=column_hints(SOURCE_STATUS_COLUMNS),
                schema_contract=SCHEMA_CONTRACT,
            )
        )
    return report


def _sql(pipeline: Pipeline, sql: str, *args: object) -> list[Sequence[object]]:
    """Run ``sql`` with qualified table names; a table not yet created is empty."""
    tables = {"batches": BATCH_EVIDENCE_TABLE, "orders": ROOT_TABLE}
    tables["status"] = SOURCE_STATUS_TABLE
    with pipeline.sql_client() as client:
        names = {key: client.make_qualified_table_name(t) for key, t in tables.items()}
        try:
            result: Sequence[Sequence[object]] | None = client.execute_sql(
                sql.format(**names), *args
            )
        except DestinationUndefinedEntity:
            return []
    return list(result or [])


def _instant(value: object) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, datetime):
        raise SourceError("Reconciliation read a non-timestamp value")
    return value.astimezone(UTC)


def _first(client: ShopifyClient, raw_first: datetime | None) -> datetime | None:
    nodes = objects(
        as_object(
            client.query(EARLIEST_ORDER, path="earliest").get("orders"), "orders"
        ).get("nodes"),
        "orders",
    )
    found = [datetime.fromisoformat(str(node["createdAt"])) for node in nodes]
    return min([*found, *([raw_first] if raw_first else [])], default=None)


def _months(
    first: datetime | None, end: datetime
) -> Iterator[tuple[datetime, datetime]]:
    if first is None:
        return
    start = first.astimezone(UTC).replace(
        day=1, hour=0, minute=0, second=0, microsecond=0
    )
    while start < end:
        stop = (start + timedelta(days=32)).replace(day=1)
        yield start, min(stop, end)
        start = stop


def _present(client: ShopifyClient, order_ids: Sequence[str]) -> set[str]:
    """IDs a direct read still returns; a deleted order reads as null."""
    present: set[str] = set()
    for offset in range(0, len(order_ids), VERSION_CHECK_IDS_PER_REQUEST):
        batch = list(order_ids[offset : offset + VERSION_CHECK_IDS_PER_REQUEST])
        data = client.query(ORDER_VERSIONS, {"ids": batch}, path="reconcile_read")
        nodes = as_list(data.get("nodes"), "nodes")
        if len(nodes) != len(batch):
            raise SourceError("Direct read returned a different node count")
        present.update(key for key, node in zip(batch, nodes, strict=True) if node)
    return present
