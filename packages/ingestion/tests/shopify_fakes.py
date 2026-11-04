"""Deterministic Admin GraphQL fakes for offline Shopify tests."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from analytics_ingestion.kit.runtime import Target
from analytics_ingestion.shopify.client import HttpResponse
from analytics_ingestion.shopify.jsonvalue import (
    JsonObject,
    as_object,
    load_object,
    objects,
)
from analytics_ingestion.shopify.queries import (
    API_VERSION,
    LINE_ITEM_PAGE,
    ORDER_CHILD_FIELDS,
    ORDER_CHILD_PAGE,
    PAGE_LINE_ITEMS,
    PAGE_SIZE,
    REFUND_CHILD_PAGE,
)
from analytics_ingestion.shopify.settings import (
    Settings,
    ShopifyCredentials,
)

SHOP = "gid://shopify/Shop/1"
DOMAIN = "fixture.myshopify.com"
CONNECTIONS = (
    "refundLineItems",
    "orderAdjustments",
    "refundShippingLines",
    "transactions",
)


def money(amount: str, currency: str = "USD") -> JsonObject:
    return {
        "shopMoney": {"amount": amount, "currencyCode": currency},
        "presentmentMoney": {"amount": amount, "currencyCode": currency},
    }


def line(number: int, quantity: int = 1, amount: str = "10.00") -> JsonObject:
    return {
        "id": f"gid://shopify/LineItem/{number}",
        "sku": f"SKU-{number}",
        "name": f"Item {number}",
        "title": f"Item {number}",
        "variantTitle": None,
        "isGiftCard": False,
        "discountAllocations": [],
        "taxLines": [],
        "quantity": quantity,
        "currentQuantity": quantity,
        "unfulfilledQuantity": quantity,
        "nonFulfillableQuantity": 0,
        "requiresShipping": True,
        "originalUnitPriceSet": money(amount),
        "discountedTotalSet": money(amount),
        "product": {"id": "gid://shopify/Product/9"},
        "variant": None,
    }


def transaction(number: int, kind: str = "SALE", status: str = "SUCCESS") -> JsonObject:
    return {
        "id": f"gid://shopify/OrderTransaction/{number}",
        "parentTransaction": None,
        "createdAt": "2026-10-01T00:00:00Z",
        "processedAt": None,
        "kind": kind,
        "status": status,
        "gateway": "manual",
        "test": False,
        "errorCode": None,
        "amountSet": money("10.00"),
    }


def refund(number: int) -> JsonObject:
    return {
        "id": f"gid://shopify/Refund/{number}",
        "createdAt": "2026-10-01T00:00:00Z",
        "updatedAt": "2026-10-01T00:00:00Z",
        "totalRefundedSet": money("5.00"),
        "note": None,
        "processedAt": None,
        "return": None,
        "duties": [],
    }


def order(
    number: int,
    updated: str = "2026-10-01T00:30:00Z",
    created: str = "2026-09-30T00:00:00Z",
    transactions: list[JsonObject] | None = None,
    refunds: list[JsonObject] | None = None,
    total: str = "10.00",
) -> JsonObject:
    txs = transactions if transactions is not None else [transaction(number)]
    return {
        "id": f"gid://shopify/Order/{number}",
        "name": f"#{number}",
        "tags": [],
        "discountCodes": [],
        "email": None,
        "phone": None,
        "billingAddress": None,
        "shippingAddress": None,
        "taxesIncluded": False,
        "originalTotalPriceSet": money(total),
        "totalTaxSet": money("0.00"),
        "totalShippingPriceSet": money("0.00"),
        "taxLines": [],
        "discountApplications": [],
        "shippingLines": [],
        "agreements": [],
        "createdAt": created,
        "updatedAt": updated,
        "processedAt": created,
        "test": False,
        "sourceName": "web",
        "cancelReason": None,
        "cancelledAt": None,
        "closedAt": None,
        "customer": {"id": "gid://shopify/Customer/3"},
        "displayFinancialStatus": "PAID",
        "displayFulfillmentStatus": "UNFULFILLED",
        "currencyCode": "USD",
        "presentmentCurrencyCode": "USD",
        "totalPriceSet": money(total),
        "currentTotalPriceSet": money(total),
        "subtotalPriceSet": money(total),
        "currentSubtotalPriceSet": money(total),
        "totalDiscountsSet": None,
        "currentTotalTaxSet": money("0.00"),
        "totalRefundedSet": money("0.00"),
        "transactionsCount": {"count": len(txs), "precision": "EXACT"},
        "transactions": txs,
        "refunds": refunds or [],
    }


@dataclass
class FakeShop:
    """In-memory shop. Tests mutate fields to inject source behavior."""

    orders: dict[str, JsonObject] = field(default_factory=dict)
    lines: dict[str, list[JsonObject]] = field(default_factory=dict)
    refund_children: dict[str, dict[str, list[JsonObject]]] = field(
        default_factory=dict
    )
    count: int | None = None
    scopes: list[str] = field(
        default_factory=lambda: ["read_orders", "read_all_orders"]
    )
    served_version: str = API_VERSION
    # Called after hydration requests; lets a test move an order mid-extraction.
    on_refund_hydration: Callable[[FakeShop], None] | None = None
    on_orders_page: Callable[[FakeShop], None] | None = None
    version_overrides: dict[str, str] = field(default_factory=dict)
    # Replaces pageInfo on order pages.
    page_info: JsonObject | None = None
    calls: list[str] = field(default_factory=list)

    def add(self, data: JsonObject, lines: list[JsonObject] | None = None) -> None:
        identity = str(data["id"])
        self.orders[identity] = data
        self.lines[identity] = (
            lines if lines is not None else [line(len(self.lines) + 1)]
        )

    # -- transport protocol ----------------------------------------------------
    def post(
        self, url: str, body: bytes, headers: Mapping[str, str], timeout: float
    ) -> HttpResponse:
        del url, headers, timeout
        request = load_object(body, "request")
        query = str(request["query"])
        variables = as_object(request.get("variables", {}), "variables")
        data = self._route(query, variables)
        payload = json.dumps({"data": data}).encode()
        return HttpResponse(200, self.served_version, None, payload)

    # -- routing -------------------------------------------------------------------
    def _route(self, query: str, variables: JsonObject) -> JsonObject:
        if "ordersCount" in query:
            self.calls.append("count")
            matching = len(self._matching(str(variables["search"])))
            value = matching if self.count is None else self.count
            return {"ordersCount": {"count": value, "precision": "EXACT"}}
        if "shop {" in query:
            return {
                "shop": {"id": SHOP, "myshopifyDomain": DOMAIN},
                "currentAppInstallation": {
                    "accessScopes": [{"handle": scope} for scope in self.scopes]
                },
            }
        if "... on Refund { id" in query:
            self.calls.append("refund_children")
            nodes: list[object] = [
                self._refund_node(str(item)) for item in _ids(variables)
            ]
            result: JsonObject = {"nodes": nodes}
            if self.on_refund_hydration is not None:
                hook = self.on_refund_hydration
                self.on_refund_hydration = None
                hook(self)
            return result
        if "node(id: $id) { ... on Refund" in query:
            self.calls.append("refund_page")
            return {"node": self._refund_page(query, variables)}
        if "... on Order { id updatedAt" in query:
            self.calls.append("versions")
            return {"nodes": [self._version(str(item)) for item in _ids(variables)]}
        if "order(id: $id) { id agreements(first: 2, query:" in query:
            self.calls.append("sales_page")
            identity = str(variables["id"])
            agreements = objects(self.orders[identity]["agreements"], "agreements")
            found = [
                item
                for item in agreements
                if str(item["id"]).rsplit("/", 1)[1]
                == str(variables["search"]).split(":")[1]
            ]
            return {
                "order": {
                    "id": identity,
                    "agreements": {
                        "nodes": [
                            {
                                "id": item["id"],
                                "sales": self._page(
                                    objects(item["sales"], "sales"),
                                    int(str(variables["after"])),
                                    100,
                                ),
                            }
                            for item in found
                        ]
                    },
                }
            }
        if f"orders(first: {PAGE_SIZE}, after: $after" in query:
            self.calls.append("orders_page")
            return {"orders": self._orders_page(variables)}
        if "order(id: $id) { id lineItems(" in query:
            self.calls.append("lines_page")
            identity = str(variables["id"])
            if identity not in self.orders:
                return {"order": None}
            page = self._page(
                self.lines[identity], int(str(variables["after"])), LINE_ITEM_PAGE
            )
            return {"order": {"id": identity, "lineItems": page}}
        if "order(id: $id) { id " in query and "order(id: $id) { id name" not in query:
            name = next(name for name in ORDER_CHILD_FIELDS if f"{name}(" in query)
            self.calls.append(f"order_page:{name}")
            identity = str(variables["id"])
            return {
                "order": {
                    "id": identity,
                    name: self._commercial_page(
                        identity, name, int(str(variables["after"])), 100
                    ),
                }
            }
        if "orders(first: 1, sortKey: CREATED_AT)" in query:
            if not self.orders:
                return {"orders": {"nodes": []}}
            first = min(self.orders.values(), key=lambda item: str(item["createdAt"]))
            return {
                "orders": {
                    "nodes": [{"id": first["id"], "createdAt": first["createdAt"]}]
                }
            }
        raise AssertionError(f"Unrouted fixture query: {query[:60]}")

    def _orders_page(self, variables: JsonObject) -> JsonObject:
        """Keyset pages sorted by (updatedAt, id), like Shopify's opaque cursors.

        An order edited mid-pagination moves behind the cursor without
        shifting the orders that follow it.
        """
        after = str(variables.get("after") or "")
        start = tuple(after.split("|", 1)) if "|" in after else ("", "")
        matching = sorted(
            (
                (str(data["updatedAt"]), identity, data)
                for identity, data in self._matching(str(variables["search"]))
            ),
            key=lambda item: (item[0], item[1]),
        )
        remaining = [item for item in matching if (item[0], item[1]) > start]
        page = remaining[:PAGE_SIZE]
        nodes: list[JsonObject] = [
            {
                **{k: v for k, v in data.items() if k not in ORDER_CHILD_FIELDS},
                **{
                    name: self._commercial_page(identity, name, 0, ORDER_CHILD_PAGE)
                    for name in ORDER_CHILD_FIELDS
                },
                "lineItems": self._page(
                    self.lines.get(identity, []), 0, PAGE_LINE_ITEMS
                ),
            }
            for _, identity, data in page
        ]
        if self.on_orders_page is not None:
            hook = self.on_orders_page
            self.on_orders_page = None
            hook(self)
        info = self.page_info or {
            "hasNextPage": len(remaining) > PAGE_SIZE,
            "endCursor": f"{page[-1][0]}|{page[-1][1]}" if page else None,
        }
        return {"nodes": nodes, "pageInfo": info}

    def _matching(self, search: str) -> list[tuple[str, JsonObject]]:
        """Apply the quoted half-open timestamp search used by the source, or an
        ``id:1 OR id:2`` search, which ignores identities that do not exist."""
        if search.startswith("id:"):
            wanted = {
                f"gid://shopify/Order/{term[3:]}" for term in search.split(" OR ")
            }
            return [(i, data) for i, data in self.orders.items() if i in wanted]
        field_name = search.split(":", 1)[0]
        key = "updatedAt" if field_name == "updated_at" else "createdAt"
        start, end = search.split('"')[1], search.split('"')[3]
        return [
            (identity, data)
            for identity, data in self.orders.items()
            if start <= str(data[key]) < end
        ]

    def _refund_node(self, identity: str) -> JsonObject:
        children = self.refund_children.get(identity, {})
        node: JsonObject = {"id": identity}
        for connection in CONNECTIONS:
            nodes = self._refund_defaults(connection, children.get(connection, []))
            node[connection] = {
                "nodes": nodes[:REFUND_CHILD_PAGE],
                "pageInfo": {
                    "hasNextPage": len(nodes) > REFUND_CHILD_PAGE,
                    "endCursor": str(REFUND_CHILD_PAGE),
                },
            }
        return node

    def _refund_page(self, query: str, variables: JsonObject) -> JsonObject:
        connection = next(name for name in CONNECTIONS if name in query)
        nodes = self._refund_defaults(
            connection,
            self.refund_children.get(str(variables["id"]), {}).get(connection, []),
        )
        offset = int(str(variables["after"]))
        page = nodes[offset : offset + 250]
        return {
            connection: {
                "nodes": page,
                "pageInfo": {
                    "hasNextPage": offset + 250 < len(nodes),
                    "endCursor": str(offset + 250),
                },
            }
        }

    @staticmethod
    def _refund_defaults(name: str, nodes: list[JsonObject]) -> list[JsonObject]:
        defaults: JsonObject = {}
        if name == "orderAdjustments":
            defaults = {"reason": None}
        elif name == "refundShippingLines":
            defaults = {"shippingLine": None}
        return [{**defaults, **node} for node in nodes]

    @staticmethod
    def _page(nodes: list[JsonObject], offset: int, size: int) -> JsonObject:
        return {
            "nodes": nodes[offset : offset + size],
            "pageInfo": {
                "hasNextPage": offset + size < len(nodes),
                "endCursor": str(offset + size),
            },
        }

    def _commercial_page(
        self, identity: str, name: str, offset: int, size: int
    ) -> JsonObject:
        nodes = objects(self.orders[identity][name], name)
        if name == "agreements":
            nodes = [
                {
                    **item,
                    "sales": self._page(
                        objects(item["sales"], "sales"), 0, ORDER_CHILD_PAGE
                    ),
                }
                for item in nodes
            ]
        return self._page(nodes, offset, size)

    def _version(self, identity: str) -> JsonObject | None:
        data = self.orders.get(identity)
        if data is None:
            return None
        return {
            "id": identity,
            "updatedAt": self.version_overrides.get(identity, data["updatedAt"]),
        }


def _ids(variables: JsonObject) -> list[object]:
    value = variables.get("ids")
    assert isinstance(value, list)
    return list(value)  # pyright: ignore[reportUnknownArgumentType]


def settings(root: Path) -> Settings:
    """Local settings that load ``root / "shopify_orders.duckdb"``."""
    return Settings(
        credentials=ShopifyCredentials(DOMAIN, "fixture-token-not-a-secret"),
        dataset="shopify_orders_raw",
        target=Target("shopify_orders", "local", root, None),
    )


def at(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00"))
