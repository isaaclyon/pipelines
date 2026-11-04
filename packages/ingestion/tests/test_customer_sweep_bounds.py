"""Customer reads stay under Shopify's connection cap on large stores."""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from typing import cast

import pytest

from analytics_ingestion.shopify.client import ShopifyClient
from analytics_ingestion.shopify.customer_source import (
    MAX_WINDOW_CUSTOMERS,
    CustomerReader,
)
from analytics_ingestion.shopify.errors import SourceError
from analytics_ingestion.shopify.jsonvalue import JsonObject
from tests.customer_fakes import customer
from tests.shopify_fakes import SHOP

BASE = datetime(2026, 1, 1, tzinfo=UTC)
END = datetime(2027, 1, 1, tzinfo=UTC)


def stamp(value: datetime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


class CappedShop:
    """Stands in for ShopifyClient. Like Shopify it serves at most 25,000 nodes
    of one search and then reports the connection as finished."""

    def __init__(self, updated: list[datetime]) -> None:
        self.nodes: list[JsonObject] = [
            customer(number, stamp(value), addresses=[], tags=[])
            for number, value in enumerate(updated, 1)
        ]
        self.nodes.sort(key=lambda n: (str(n["updatedAt"]), str(n["id"])))
        self.searches: list[str] = []
        self.size = 250

    def query(
        self, document: str, variables: JsonObject | None = None, *, path: str
    ) -> JsonObject:
        del document
        assert path == "customers_page"
        assert variables is not None
        search = str(variables["search"])
        lower = re.search(r'updated_at:>="([^"]+)"', search)
        upper = re.search(r'updated_at:<"([^"]+)"', search)
        if variables.get("after") is None:
            self.searches.append(search)
        found = [
            n
            for n in self.nodes
            if (not lower or str(n["updatedAt"]) >= lower[1])
            and (not upper or str(n["updatedAt"]) < upper[1])
        ][:25_000]
        start = int(str(variables["after"] or 0))
        page = found[start : start + self.size]
        more = start + self.size < len(found)
        return {
            "customers": {
                "nodes": page,
                "pageInfo": {
                    "hasNextPage": more,
                    "endCursor": str(start + self.size) if more else None,
                },
            }
        }


def read_all(
    shop: CappedShop, start: datetime | None, cap: int | None = None
) -> list[str]:
    reader = CustomerReader(
        cast(ShopifyClient, shop),
        SHOP,
        frozenset({"names"}),
        window_cap=MAX_WINDOW_CUSTOMERS if cap is None else cap,
    )
    ids: list[str] = []
    for page in reader.read(start, END, lambda _node: True):
        ids.extend(str(parsed.content["customer_id"]) for parsed, _ in page)
    assert reader.observed.customers == len(ids)
    return ids


def test_sweep_reads_more_customers_than_one_connection_serves() -> None:
    total = MAX_WINDOW_CUSTOMERS + 3_000
    shop = CappedShop([BASE + timedelta(seconds=n // 2) for n in range(total)])

    ids = read_all(shop, None)

    assert len(ids) == total
    assert len(set(ids)) == total
    assert len(shop.searches) > 1


def test_incremental_window_reads_past_the_cap_in_updated_order() -> None:
    total = MAX_WINDOW_CUSTOMERS + 10
    shop = CappedShop([BASE + timedelta(seconds=n) for n in range(total)])

    ids = read_all(shop, BASE + timedelta(seconds=5))

    assert len(ids) == total - 5
    assert len(set(ids)) == total - 5


def test_a_cap_of_customers_in_one_second_fails_instead_of_truncating() -> None:
    shop = CappedShop([BASE] * 40)
    with pytest.raises(SourceError, match="cannot be split"):
        _ = read_all(shop, None, cap=40)


def test_small_cap_still_covers_every_customer_once() -> None:
    shop = CappedShop([BASE + timedelta(seconds=n // 3) for n in range(500)])
    ids = read_all(shop, None, cap=60)
    assert len(ids) == len(set(ids)) == 500
