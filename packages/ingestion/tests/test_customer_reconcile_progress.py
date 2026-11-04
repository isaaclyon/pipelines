"""A malformed existing customer must not block unrelated deletion purges."""

import json
from collections.abc import Mapping
from pathlib import Path

import pytest

from analytics_ingestion.shopify.client import HttpResponse
from analytics_ingestion.shopify.customer_reconcile import reconcile_customers
from analytics_ingestion.shopify.customer_source import build_pipeline, open_rejects
from analytics_ingestion.shopify.errors import SourceError
from analytics_ingestion.shopify.jsonvalue import JsonObject, load_object
from tests.customer_fakes import FakeCustomerShop, customer, customer_settings
from tests.test_customer_source import END, query, run


def test_persistent_reject_does_not_block_an_unrelated_deletion(tmp_path: Path) -> None:
    shop = FakeCustomerShop()
    for number in (1, 2, 3):
        shop.add(customer(number))
    run(tmp_path, shop, "sweep", "clean-sweep", end=END)
    rejected = "gid://shopify/Customer/2"
    deleted = "gid://shopify/Customer/3"
    shop.customers[rejected]["numberOfOrders"] = "invalid"
    shop.customers[rejected]["updatedAt"] = "2026-10-01T01:30:00Z"
    del shop.customers[deleted]
    run(tmp_path, shop, "sweep", "rejectful-sweep", end=END.replace(hour=2))

    settings = customer_settings(tmp_path)
    report = reconcile_customers(settings, "reconcile", shop, sleeper=lambda _: None)

    assert report.sweep_batch == "rejectful-sweep"
    assert report.confirmed == 1
    assert report.unconfirmed == 1
    assert query(
        tmp_path,
        "select customer_id from shopify_customers_raw.customers order by customer_id",
    ) == [("gid://shopify/Customer/1",), (rejected,)]
    assert query(
        tmp_path,
        "select customer_id, status from "
        "shopify_customers_raw.customer_source_status order by customer_id",
    ) == [(rejected, "unknown"), (deleted, "confirmed_unavailable")]
    assert list(open_rejects(build_pipeline(settings), "gid://shopify/Shop/1")) == [
        rejected
    ]


def rejected_then_absent(tmp_path: Path) -> tuple[FakeCustomerShop, JsonObject]:
    shop = FakeCustomerShop()
    shop.add(customer(1))
    shop.add(customer(2))
    run(tmp_path, shop, "sweep", "clean", end=END)
    rejected = "gid://shopify/Customer/2"
    shop.customers[rejected]["numberOfOrders"] = "invalid"
    shop.customers[rejected]["updatedAt"] = "2026-10-01T01:30:00Z"
    run(tmp_path, shop, "sweep", "rejected", end=END.replace(hour=2))
    saved = shop.customers.pop(rejected)
    run(tmp_path, shop, "sweep", "absent", end=END.replace(hour=3))
    return shop, saved


@pytest.mark.parametrize("change", ["reappears", "scope_lost", "malformed"])
def test_rejected_candidate_still_needs_two_safe_absence_reads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    shop, saved = rejected_then_absent(tmp_path)
    original = shop.post
    reads = 0

    def post(
        url: str, body: bytes, headers: Mapping[str, str], timeout: float
    ) -> HttpResponse:
        nonlocal reads
        request = load_object(body, "test request")
        if "... on Customer { id updatedAt }" in str(request["query"]):
            reads += 1
            if change == "reappears" and reads == 2:
                shop.add(saved)
            if change == "scope_lost" and reads == 1:
                shop.scopes = []
            if change == "malformed":
                return HttpResponse(
                    200,
                    shop.served_version,
                    None,
                    json.dumps({"data": {"nodes": [{}]}}).encode(),
                )
        return original(url, body, headers, timeout)

    monkeypatch.setattr(shop, "post", post)
    settings = customer_settings(tmp_path)
    if change == "reappears":
        report = reconcile_customers(
            settings, "reconcile", shop, sleeper=lambda _: None
        )
        assert (report.confirmed, report.unconfirmed) == (0, 1)
        assert reads == 2
    else:
        with pytest.raises(SourceError):
            reconcile_customers(settings, "reconcile", shop, sleeper=lambda _: None)
    assert query(
        tmp_path,
        "select count(*) from shopify_customers_raw.customers "
        "where customer_id = 'gid://shopify/Customer/2'",
    ) == [(1,)]
    assert list(open_rejects(build_pipeline(settings), "gid://shopify/Shop/1")) == [
        "gid://shopify/Customer/2"
    ]


def test_never_loaded_rejects_count_toward_the_deletion_limit(tmp_path: Path) -> None:
    shop = FakeCustomerShop()
    for number in (1, 2):
        row = customer(number)
        row["numberOfOrders"] = "invalid"
        shop.add(row)
    run(tmp_path, shop, "sweep", "rejected", end=END)
    shop.customers.clear()
    run(tmp_path, shop, "sweep", "absent", end=END.replace(hour=2))
    with pytest.raises(SourceError, match="More than 1"):
        reconcile_customers(
            customer_settings(tmp_path),
            "reconcile",
            shop,
            sleeper=lambda _: None,
            max_candidates=1,
        )
    assert query(
        tmp_path,
        "select count(*) from information_schema.tables "
        "where table_name = 'customer_source_status'",
    ) == [(0,)]
    assert (
        len(
            open_rejects(
                build_pipeline(customer_settings(tmp_path)), "gid://shopify/Shop/1"
            )
        )
        == 2
    )


def test_a_later_rejection_reopens_a_confirmed_absence(tmp_path: Path) -> None:
    shop, saved = rejected_then_absent(tmp_path)
    settings = customer_settings(tmp_path)
    assert (
        reconcile_customers(
            settings, "reconcile", shop, sleeper=lambda _: None
        ).confirmed
        == 1
    )
    assert open_rejects(build_pipeline(settings), "gid://shopify/Shop/1") == {}
    shop.add(saved)
    run(tmp_path, shop, "sweep", "new-reject", end=END.replace(hour=4))
    assert list(open_rejects(build_pipeline(settings), "gid://shopify/Shop/1")) == [
        "gid://shopify/Customer/2"
    ]
