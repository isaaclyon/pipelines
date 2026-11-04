"""Customer deletion handling: only confirmed absence marks and purges."""

from __future__ import annotations

from pathlib import Path
from typing import cast

import pytest
from dlt.destinations.impl.duckdb.sql_client import DuckDbSqlClient

from analytics_ingestion.shopify.customer_reconcile import (
    CustomerReconcileReport,
    reconcile_customers,
)
from analytics_ingestion.shopify.customer_source import build_pipeline
from analytics_ingestion.shopify.errors import (
    AccessDenied,
    ConfigurationError,
    SourceError,
)
from analytics_ingestion.shopify.jsonvalue import JsonObject
from tests.customer_fakes import (
    FakeCustomerShop,
    address,
    customer,
    customer_settings,
    metafield,
)
from tests.test_customer_source import END, START, query, run

GONE = "gid://shopify/Customer/2"


def seed(tmp_path: Path) -> FakeCustomerShop:
    shop = FakeCustomerShop()
    shop.add(customer(1))
    shop.add(customer(2, addresses=[address(2)], metafields=[metafield(1)]))
    run(tmp_path, shop, "sweep", "sweep-1", end=END)
    return shop


def reconcile(
    tmp_path: Path, shop: FakeCustomerShop, limit: int = 500
) -> CustomerReconcileReport:
    return reconcile_customers(
        customer_settings(tmp_path),
        "rec-1",
        shop,
        sleeper=lambda _: None,
        max_candidates=limit,
    )


def sweep_again(tmp_path: Path, shop: FakeCustomerShop) -> None:
    run(
        tmp_path,
        shop,
        "sweep",
        "sweep-2",
        start=START,
        end=END.replace(hour=2),
    )


def test_confirmed_deletion_purges_personal_data_and_keeps_status(
    tmp_path: Path,
) -> None:
    shop = seed(tmp_path)
    del shop.customers[GONE]
    sweep_again(tmp_path, shop)

    report = reconcile(tmp_path, shop)

    assert (report.candidates, report.confirmed) == (1, 1)
    for table in (
        "customers",
        "customers__addresses",
        "customers__metafields",
        "customers__tags",
    ):
        assert query(
            tmp_path,
            f"select count(*) from shopify_customers_raw.{table}"
            " where customer_id = %s",
            GONE,
        ) == [(0,)], table
    assert query(
        tmp_path,
        "select customer_id, status from shopify_customers_raw.customer_source_status",
    ) == [(GONE, "confirmed_unavailable")]
    assert query(tmp_path, "select count(*) from shopify_customers_raw.customers") == [
        (1,)
    ]


def test_customer_missing_from_sweep_but_readable_is_kept(tmp_path: Path) -> None:
    shop = seed(tmp_path)
    shop.hidden.add(GONE)
    sweep_again(tmp_path, shop)

    report = reconcile(tmp_path, shop)

    assert (report.confirmed, report.unconfirmed) == (0, 1)
    assert query(tmp_path, "select count(*) from shopify_customers_raw.customers") == [
        (2,)
    ]
    assert query(
        tmp_path, "select status from shopify_customers_raw.customer_source_status"
    ) == [("unknown",)]


def test_access_denial_never_marks_a_deletion(tmp_path: Path) -> None:
    shop = seed(tmp_path)
    del shop.customers[GONE]
    sweep_again(tmp_path, shop)
    shop.deny_reads = True

    with pytest.raises(AccessDenied):
        reconcile(tmp_path, shop)

    assert query(tmp_path, "select count(*) from shopify_customers_raw.customers") == [
        (2,)
    ]


def test_lost_scope_blocks_marks(tmp_path: Path) -> None:
    shop = seed(tmp_path)
    del shop.customers[GONE]
    sweep_again(tmp_path, shop)
    shop.scopes = []

    with pytest.raises(SourceError, match="read_customers"):
        reconcile(tmp_path, shop)


def test_too_many_candidates_mark_nothing(tmp_path: Path) -> None:
    shop = seed(tmp_path)
    shop.customers.clear()
    sweep_again(tmp_path, shop)

    with pytest.raises(SourceError, match="access loss"):
        reconcile(tmp_path, shop, limit=1)

    assert query(tmp_path, "select count(*) from shopify_customers_raw.customers") == [
        (2,)
    ]


def test_window_runs_are_not_sweeps(tmp_path: Path) -> None:
    shop = FakeCustomerShop()
    shop.add(customer(1))
    run(tmp_path, shop)
    with pytest.raises(ConfigurationError, match="full sweep"):
        reconcile(tmp_path, shop)


def test_customer_read_by_a_later_sweep_is_available_again(tmp_path: Path) -> None:
    shop = seed(tmp_path)
    saved = shop.customers.pop(GONE)
    sweep_again(tmp_path, shop)
    reconcile(tmp_path, shop)

    shop.customers[GONE] = saved
    run(tmp_path, shop, "sweep", "sweep-3", end=END.replace(hour=3))
    report = reconcile(tmp_path, shop)

    assert report.reappeared == 1
    assert query(
        tmp_path,
        "select status from shopify_customers_raw.customer_source_status"
        " where customer_id = %s",
        GONE,
    ) == [("available",)]


def count(tmp_path: Path, table: str) -> int:
    [(n,)] = query(
        tmp_path,
        f"select count(*) from shopify_customers_raw.{table} where customer_id = %s",
        GONE,
    )
    return int(str(n))


@pytest.mark.parametrize("rejected_after_sweep", [False, True])
def test_interrupted_purge_keeps_root_and_retry_recovers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, rejected_after_sweep: bool
) -> None:
    shop = seed(tmp_path)
    if rejected_after_sweep:
        shop.customers[GONE]["numberOfOrders"] = "invalid"
        shop.customers[GONE]["updatedAt"] = "2026-10-01T00:45:00Z"
        run(tmp_path, shop, "reextract", "rejected-after-sweep")
    del shop.customers[GONE]
    if not rejected_after_sweep:
        sweep_again(tmp_path, shop)
    deletes: list[str] = []
    real = DuckDbSqlClient.execute_sql

    def flaky(self: DuckDbSqlClient, sql: str, *args: object, **kw: object) -> object:
        if sql.startswith("delete from"):
            table = sql.split()[2].split(".")[-1].strip('"')
            deletes.append(table)
            if len(deletes) == 2:
                raise RuntimeError("interrupted")
        return real(self, sql, *args, **kw)  # pyright: ignore[reportArgumentType]

    monkeypatch.setattr(DuckDbSqlClient, "execute_sql", flaky)
    with pytest.raises(RuntimeError, match="interrupted"):
        reconcile(tmp_path, shop)

    assert "customers" not in deletes[:1]
    assert count(tmp_path, "customers") == 1  # still a candidate for retry

    deletes.clear()
    monkeypatch.setattr(DuckDbSqlClient, "execute_sql", real)
    report = reconcile(tmp_path, shop)

    assert report.confirmed == 1
    assert report.reappeared == 0
    for table in ("customers", "customers__addresses", "customers__metafields"):
        assert count(tmp_path, table) == 0, table


class MalformedShop(FakeCustomerShop):
    node: object = None

    def _route(self, query: str, variables: JsonObject) -> JsonObject:
        if "... on Customer { id updatedAt }" in query and not self.deny_reads:
            ids = variables["ids"]
            assert isinstance(ids, list)
            count_ = len(cast("list[object]", ids))
            return {"data": {"nodes": [self.node] * count_}}
        return super()._route(query, variables)


@pytest.mark.parametrize(
    "node", [{}, {"id": "gid://shopify/Customer/99"}, "x", 0, [], {"id": None}]
)
def test_malformed_direct_read_raises_and_purges_nothing(
    tmp_path: Path, node: object
) -> None:
    shop = seed(tmp_path)
    del shop.customers[GONE]
    sweep_again(tmp_path, shop)
    bad = MalformedShop()
    bad.customers = shop.customers
    bad.scopes = shop.scopes
    bad.node = node

    with pytest.raises(SourceError):
        reconcile(tmp_path, bad)

    assert count(tmp_path, "customers") == 1
    assert count(tmp_path, "customers__addresses") == 1
    assert query(
        tmp_path,
        "select count(*) from information_schema.tables"
        " where table_name = 'customer_source_status'",
    ) == [(0,)]


@pytest.mark.parametrize(
    "assignment", ["completeness = 'partial'", "status = 'failed'"]
)
def test_sweep_with_incomplete_evidence_is_not_used(
    tmp_path: Path, assignment: str
) -> None:
    shop = seed(tmp_path)
    del shop.customers[GONE]
    sweep_again(tmp_path, shop)
    pipeline = build_pipeline(customer_settings(tmp_path))
    with pipeline.sql_client() as client:
        client.execute_sql(
            f"update shopify_customers_raw.customer_batch_evidence set {assignment}"
        )

    with pytest.raises(ConfigurationError, match="full sweep"):
        reconcile(tmp_path, shop)

    assert count(tmp_path, "customers") == 1


def test_reconcile_blocks_pending_customer_packages(tmp_path: Path) -> None:
    import dlt

    shop = seed(tmp_path)
    pipeline = build_pipeline(customer_settings(tmp_path))
    pipeline.extract(
        dlt.resource([{"email": "pending@example.invalid"}], name="pending")
    )
    with pytest.raises(ConfigurationError, match="pending"):
        reconcile(tmp_path, shop)
    assert pipeline.has_pending_data


def test_reconcile_removes_legacy_completed_payload_jobs(tmp_path: Path) -> None:
    import dlt

    from tests.test_customer_package_retention import holders

    shop = seed(tmp_path)
    pipeline = build_pipeline(customer_settings(tmp_path))
    needle = b"legacy@example.invalid"
    pipeline.run(dlt.resource([{"email": needle.decode()}], name="legacy"))
    assert holders(Path(pipeline.working_dir), needle)
    reconcile(tmp_path, shop)
    assert holders(Path(pipeline.working_dir), needle) == []
