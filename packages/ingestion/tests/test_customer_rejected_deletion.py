"""A rejected customer that Shopify later deletes must be purged and closed."""

from __future__ import annotations

from pathlib import Path

from analytics_ingestion.shopify.customer_reconcile import (
    CustomerReconcileReport,
    reconcile_customers,
)
from analytics_ingestion.shopify.customer_source import REJECTS, build_pipeline
from analytics_ingestion.shopify.load_cycle import open_rejects
from tests.customer_fakes import (
    FakeCustomerShop,
    address,
    customer,
    customer_settings,
)
from tests.test_customer_source import END, START, query, run

GONE = "gid://shopify/Customer/2"
RAW = "shopify_customers_raw"


def _reconcile(
    tmp_path: Path, shop: FakeCustomerShop, run_id: str = "rec-1"
) -> CustomerReconcileReport:
    return reconcile_customers(
        customer_settings(tmp_path), run_id, shop, sleeper=lambda _: None
    )


def _open(tmp_path: Path) -> dict[str, object]:
    pipeline = build_pipeline(customer_settings(tmp_path))
    return dict(open_rejects(pipeline, "gid://shopify/Shop/1", REJECTS))


def _sweep(tmp_path: Path, shop: FakeCustomerShop, name: str, hour: int) -> None:
    run(tmp_path, shop, "sweep", name, start=START, end=END.replace(hour=hour))


def _shop_with_loaded_then_rejected(tmp_path: Path) -> FakeCustomerShop:
    shop = FakeCustomerShop()
    shop.add(customer(1))
    shop.add(customer(2, addresses=[address(2)]))
    run(tmp_path, shop, "sweep", "sweep-1", end=END)
    shop.customers[GONE]["numberOfOrders"] = "many"
    shop.customers[GONE]["updatedAt"] = "2026-10-01T01:30:00Z"
    _sweep(tmp_path, shop, "sweep-2", 2)
    return shop


def test_loaded_then_rejected_then_deleted_is_purged(tmp_path: Path) -> None:
    shop = _shop_with_loaded_then_rejected(tmp_path)
    assert list(_open(tmp_path)) == [GONE]
    del shop.customers[GONE]
    _sweep(tmp_path, shop, "sweep-3", 3)

    report = _reconcile(tmp_path, shop)

    assert report.confirmed == 1
    assert query(tmp_path, f"select count(*) from {RAW}.customers") == [(1,)]
    assert query(
        tmp_path,
        f"select count(*) from {RAW}.customers__addresses where customer_id = %s",
        GONE,
    ) == [(0,)]
    assert _open(tmp_path) == {}


def test_never_loaded_rejected_then_deleted_closes_reject(tmp_path: Path) -> None:
    shop = FakeCustomerShop()
    shop.add(customer(1))
    bad = customer(2)
    bad["numberOfOrders"] = "many"
    shop.add(bad)
    run(tmp_path, shop, "sweep", "sweep-1", end=END)
    del shop.customers[GONE]
    _sweep(tmp_path, shop, "sweep-2", 2)

    report = _reconcile(tmp_path, shop)

    assert report.confirmed == 1
    assert _open(tmp_path) == {}
    assert query(tmp_path, f"select status from {RAW}.customer_source_status") == [
        ("confirmed_unavailable",)
    ]


def test_rejected_customer_still_readable_stays_open_and_is_kept(
    tmp_path: Path,
) -> None:
    shop = _shop_with_loaded_then_rejected(tmp_path)

    report = _reconcile(tmp_path, shop)
    assert report.confirmed == 0
    assert report.unconfirmed == 1

    assert list(_open(tmp_path)) == [GONE]
    assert query(tmp_path, f"select count(*) from {RAW}.customers") == [(2,)]
