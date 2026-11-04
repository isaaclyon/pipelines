"""Deletion and unavailability handling for the customer source.

Candidates are raw rows the newest complete full sweep did not load (their
``_batch_id`` is another batch and they predate the sweep), plus open profile
rejects. Profile rejects do not make a completed traversal incomplete; direct
presence reads protect those customers. Candidates are found in the destination,
so no customer population is held in memory. Access denial, a partial sweep,
a request limit or a single missing read never confirm a deletion:

1. The newest ``customer_batch_evidence`` row with ``is_full_sweep`` selects
   the sweep; without one nothing is marked.
2. More than ``MAX_CANDIDATES`` candidates mark nothing and fail the run, since
   that suggests lost access, not deletions.
3. Shop identity and the ``read_customers`` scope are checked again, then each
   candidate is read directly by ID. A customer a read returns is marked
   ``unknown`` (absent from the sweep, present directly) and is kept.
4. A candidate that is null in two direct reads is ``confirmed_unavailable``:
   its root and child rows are deleted (personal-data purge) and only the ID
   and status remain in ``customer_source_status``.
5. A customer marked unavailable that a later sweep reads again is marked
   ``available``.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime

import dlt
from dlt.common.destination.exceptions import DestinationUndefinedEntity
from dlt.pipeline.pipeline import Pipeline

from analytics_ingestion.shopify.client import (
    Budget,
    ExecutionLimits,
    ShopifyClient,
    Transport,
)
from analytics_ingestion.shopify.customer_model import (
    EVIDENCE_TABLE,
    NESTED_TABLES,
    ROOT_TABLE,
    STATUS_COLUMNS,
    STATUS_TABLE,
)
from analytics_ingestion.shopify.customer_queries import (
    CUSTOMER_PRESENCE,
    IDS_PER_READ,
)
from analytics_ingestion.shopify.customer_source import (
    REQUIRED_SCOPES,
    build_pipeline,
    open_rejects,
    purge_completed_jobs,
)
from analytics_ingestion.shopify.errors import ConfigurationError, SourceError
from analytics_ingestion.shopify.extract import shop_identity
from analytics_ingestion.shopify.jsonvalue import as_list, as_object
from analytics_ingestion.shopify.pipeline import (
    MERGE,
    SCHEMA_CONTRACT,
    Row,
    column_hints,
)
from analytics_ingestion.shopify.settings import Settings

MAX_CANDIDATES = 500
ABSENT = "unavailable in two direct reads after a complete sweep"
KEPT = "deletion candidate returned by a direct read"
BACK = "read by a later sweep"


@dataclass
class CustomerReconcileReport:
    run_id: str
    sweep_batch: str | None = None
    candidates: int = 0
    confirmed: int = 0
    unconfirmed: int = 0
    reappeared: int = 0

    def metadata(self) -> Mapping[str, str | int | None]:
        return {
            "sweep_batch": self.sweep_batch,
            "candidates": self.candidates,
            "confirmed_unavailable": self.confirmed,
            "unconfirmed": self.unconfirmed,
            "reappeared": self.reappeared,
        }


def reconcile_customers(
    settings: Settings,
    run_id: str,
    transport: Transport | None = None,
    limits: ExecutionLimits | None = None,
    sleeper: Callable[[float], None] = time.sleep,
    max_candidates: int = MAX_CANDIDATES,
) -> CustomerReconcileReport:
    client = ShopifyClient(
        settings.credentials.shop_domain,
        settings.credentials.access_token,
        Budget(limits or ExecutionLimits(), sleeper=sleeper),
        transport,
    )
    shop_id = shop_identity(client, REQUIRED_SCOPES)
    pipeline = build_pipeline(settings)
    if pipeline.has_pending_data:
        raise ConfigurationError(
            "Reconciliation requires recovery of pending customer packages"
        )
    if any(
        pipeline.list_failed_jobs_in_package(load_id)
        for load_id in pipeline.list_completed_load_packages()
    ):
        raise ConfigurationError(
            "Reconciliation requires recovery of failed customer packages"
        )
    purge_completed_jobs(pipeline)
    report = CustomerReconcileReport(run_id)
    open_ids = sorted(open_rejects(pipeline, shop_id))
    sweeps = _sql(
        pipeline,
        "select batch_id, observed_started_at from {evidence} where shop_id = %s "
        "and is_full_sweep and status = 'complete' and completeness = 'complete' "
        "order by completed_at desc limit 1",
        shop_id,
    )
    if not sweeps:
        raise ConfigurationError("Reconciliation needs a complete full sweep first")
    # Profile rejects do not imply a traversal gap: each rejected node has a
    # validated ID. They still fail freshness, but must not block deletion of
    # unrelated customers. Every candidate, including a reject never loaded,
    # needs two direct null reads before any personal data can be purged.
    sweep_id, started = str(sweeps[0][0]), sweeps[0][1]
    if not isinstance(started, datetime):
        raise SourceError("Sweep evidence has no start time")
    report.sweep_batch = sweep_id
    loaded = [
        str(row[0])
        for row in _sql(
            pipeline,
            "select customer_id from {customers} where shop_id = %s and "
            "_batch_id <> %s and _observed_at < %s order by customer_id limit %s",
            shop_id,
            sweep_id,
            started,
            max_candidates + 1,
        )
    ]
    # A status can commit before its purge fails. Include the remaining root
    # even when it was loaded by this sweep and its reject is already closed.
    pending_purges = [
        str(row[0])
        for row in _sql(
            pipeline,
            "select c.customer_id from {customers} c join {status} s on "
            "c.shop_id = s.shop_id and c.customer_id = s.customer_id where "
            "c.shop_id = %s and s.status = 'confirmed_unavailable' and "
            "c._observed_at <= s.checked_at order by c.customer_id limit %s",
            shop_id,
            max_candidates + 1,
        )
    ]
    candidates = sorted(set(loaded) | set(open_ids) | set(pending_purges))
    if len(candidates) > max_candidates:
        raise SourceError(
            f"More than {max_candidates} deletion candidates; access loss suspected,"
            " none marked"
        )
    report.candidates = len(candidates)
    reappeared = [
        str(row[0])
        for row in _sql(
            pipeline,
            "select s.customer_id from {status} s join {customers} c on "
            "c.shop_id = s.shop_id and c.customer_id = s.customer_id where "
            "s.shop_id = %s and s.status = 'confirmed_unavailable' and "
            "c._batch_id = %s and c._observed_at > s.checked_at",
            shop_id,
            sweep_id,
        )
    ]
    report.reappeared = len(reappeared)
    first = _present(client, candidates)
    shop_identity(client, REQUIRED_SCOPES)  # A grant revoked mid-run blocks the marks.
    second = _present(client, sorted(set(candidates) - first))
    present = first | second
    gone = [cid for cid in candidates if cid not in present]
    report.confirmed, report.unconfirmed = len(gone), len(present)
    now = datetime.now(UTC)
    rows: list[Row] = [
        *(
            _row(shop_id, cid, "confirmed_unavailable", ABSENT, run_id, now)
            for cid in gone
        ),
        *(_row(shop_id, cid, "unknown", KEPT, run_id, now) for cid in sorted(present)),
        *(_row(shop_id, cid, "available", BACK, run_id, now) for cid in reappeared),
    ]
    if rows:
        pipeline.run(
            dlt.resource(
                rows,
                name=STATUS_TABLE,
                write_disposition=MERGE,
                primary_key=("shop_id", "customer_id"),
                columns=column_hints(STATUS_COLUMNS),
                schema_contract=SCHEMA_CONTRACT,
            )
        )
    if gone:
        _purge(pipeline, shop_id, gone)
    return report


def _row(
    shop_id: str, customer_id: str, status: str, detail: str, run_id: str, now: datetime
) -> Row:
    return {
        "shop_id": shop_id,
        "customer_id": customer_id,
        "status": status,
        "checked_at": now,
        "batch_id": run_id,
        "detail": detail,
    }


def _purge(pipeline: Pipeline, shop_id: str, customer_ids: Sequence[str]) -> None:
    """Delete personal data of confirmed-unavailable customers. The status
    rows are written first, so an interrupted purge repeats on the next run."""
    # Children first: the root row finds a customer as a candidate, so it goes
    # last and an interrupted purge stays retryable.
    tables = [*(f"{ROOT_TABLE}__{'__'.join(p)}" for p in NESTED_TABLES), ROOT_TABLE]
    with pipeline.sql_client() as client:
        for offset in range(0, len(customer_ids), IDS_PER_READ):
            batch = list(customer_ids[offset : offset + IDS_PER_READ])
            marks = ", ".join(["%s"] * len(batch))
            with client.begin_transaction():
                for table in tables:
                    name = client.make_qualified_table_name(table)
                    try:
                        client.execute_sql(
                            f"delete from {name} where shop_id = %s and "
                            f"customer_id in ({marks})",
                            shop_id,
                            *batch,
                        )
                    except DestinationUndefinedEntity:
                        continue


def _sql(pipeline: Pipeline, sql: str, *args: object) -> list[Sequence[object]]:
    with pipeline.sql_client() as client:
        names = {
            "evidence": client.make_qualified_table_name(EVIDENCE_TABLE),
            "customers": client.make_qualified_table_name(ROOT_TABLE),
            "status": client.make_qualified_table_name(STATUS_TABLE),
        }
        try:
            result: Sequence[Sequence[object]] | None = client.execute_sql(
                sql.format(**names), *args
            )
        except DestinationUndefinedEntity:
            return []
    return list(result or [])


def _present(client: ShopifyClient, ids: Sequence[str]) -> set[str]:
    """IDs a direct read still returns; a deleted customer reads as null."""
    present: set[str] = set()
    for offset in range(0, len(ids), IDS_PER_READ):
        batch = list(ids[offset : offset + IDS_PER_READ])
        data = client.query(CUSTOMER_PRESENCE, {"ids": batch}, path="reconcile_read")
        nodes = as_list(data.get("nodes"), "nodes")
        if len(nodes) != len(batch):
            raise SourceError("Direct read returned a different node count")
        for cid, node in zip(batch, nodes, strict=True):
            if node is None:
                continue
            if as_object(node, "customer").get("id") != cid:
                raise SourceError("Direct read returned a mismatched customer ID")
            present.add(cid)
    return present
