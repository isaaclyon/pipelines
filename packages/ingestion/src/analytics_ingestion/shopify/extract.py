"""Shop identity, order counts and cursor paging shared by source and reconcile."""

from __future__ import annotations

from analytics_ingestion.shopify.client import ShopifyClient
from analytics_ingestion.shopify.errors import SourceError
from analytics_ingestion.shopify.jsonvalue import (
    JsonObject,
    as_object,
    gid,
    objects,
    required_str,
)
from analytics_ingestion.shopify.queries import ORDERS_COUNT, SHOP_IDENTITY

REQUIRED_SCOPES = frozenset({"read_orders", "read_all_orders"})


def shop_identity(
    client: ShopifyClient, required: frozenset[str] = REQUIRED_SCOPES
) -> str:
    """Shop ID after checking the credential reaches the configured shop and
    holds the ``required`` scopes."""
    data = client.query(SHOP_IDENTITY, path="identity")
    shop = as_object(data.get("shop"), "shop")
    installation = as_object(data.get("currentAppInstallation"), "installation")
    scopes = frozenset(
        required_str(scope, "handle")
        for scope in objects(installation.get("accessScopes"), "accessScopes")
    )
    domain = required_str(shop, "myshopifyDomain")
    if domain != client.shop_domain:
        raise SourceError("Credential reached a different shop than configured")
    missing = required - scopes
    if missing:
        raise SourceError(f"Missing required scopes: {', '.join(sorted(missing))}")
    return gid(shop.get("id"), "Shop", "shop.id")


def count_orders(client: ShopifyClient, search: str, limit: int) -> tuple[int, bool]:
    """Return ``(count, exact)``; ``exact`` is false when the limit was reached."""
    data = client.query(
        ORDERS_COUNT, {"search": search, "limit": limit}, path="orders_count"
    )
    count = as_object(data.get("ordersCount"), "ordersCount")
    value = count.get("count")
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise SourceError("Invalid ordersCount")
    return value, count.get("precision") == "EXACT"


def next_cursor(page: JsonObject, seen: set[str], label: str) -> str | None:
    """Cursor for the following page, or None after an explicit final page.

    Only a boolean ``hasNextPage`` is accepted: a missing or malformed value
    could otherwise make a truncated child set look authoritative.
    """
    info = as_object(page.get("pageInfo"), "pageInfo")
    more = info.get("hasNextPage")
    if not isinstance(more, bool):
        raise SourceError(f"Invalid {label} pageInfo")
    if not more:
        return None
    cursor = required_str(info, "endCursor")
    if cursor in seen:
        raise SourceError(f"Nonadvancing {label} cursor")
    seen.add(cursor)
    return cursor
