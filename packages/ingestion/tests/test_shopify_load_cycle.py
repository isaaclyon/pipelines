"""The load cycle both Shopify sources share, exercised through orders.

The customer tests cover the same cycle through the customer source.
"""

from __future__ import annotations

import os
from pathlib import Path

from analytics_ingestion.shopify.load_cycle import DELETE_COMPLETED_JOBS_ENV
from analytics_ingestion.shopify.reconcile import reconcile
from analytics_ingestion.shopify.source import build_pipeline
from tests.shopify_fakes import FakeShop, at, money, order
from tests.test_customer_package_retention import holders
from tests.test_shopify_source import isolated, run

EMAIL = "ada@example.invalid"
DELETED = "gid://shopify/Order/2"


def test_loaded_order_package_keeps_no_payload_files(tmp_path: Path) -> None:
    shop = FakeShop()
    contact = order(1)
    contact["email"] = EMAIL
    shop.add(contact)

    _ = run(tmp_path, shop)

    working = Path(build_pipeline(isolated(tmp_path)).working_dir)
    assert holders(working, EMAIL.encode()) == []
    assert DELETE_COMPLETED_JOBS_ENV not in os.environ


def test_reject_of_an_order_confirmed_deleted_is_closed(tmp_path: Path) -> None:
    shop = FakeShop()
    for number in (1, 2, 3):
        shop.add(order(number, created=f"2026-09-0{number}T00:00:00Z"))
    _ = run(tmp_path, shop, kind="refresh")
    # A newer version of a loaded order breaks the field contract.
    bad = order(2, created="2026-09-02T00:00:00Z", updated="2026-10-01T01:30:00Z")
    bad["totalPriceSet"] = money("not-a-number")
    shop.add(bad)
    rejected = run(tmp_path, shop, kind="refresh", end=at("2026-10-01T02:00:00Z"))
    assert rejected.open_rejects == 1
    del shop.orders[DELETED]

    marked = reconcile(isolated(tmp_path), "reconcile-1", shop, sleeper=lambda _: None)
    later = run(tmp_path, shop, kind="refresh", end=at("2026-10-01T03:00:00Z"))

    assert marked.confirmed == 1
    assert (later.rereads, later.open_rejects) == (0, 0)
