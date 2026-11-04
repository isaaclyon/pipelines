"""Deterministic Admin GraphQL fake for offline customer tests."""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path

from analytics_ingestion.kit.runtime import Target
from analytics_ingestion.shopify.client import HttpResponse
from analytics_ingestion.shopify.jsonvalue import JsonObject, as_object, load_object
from analytics_ingestion.shopify.queries import API_VERSION
from analytics_ingestion.shopify.settings import (
    Settings,
    ShopifyCredentials,
)
from tests.shopify_fakes import DOMAIN, SHOP

# Query keyword that selects each group, and the node keys it owns.
GROUP_KEYWORDS = {
    "names": ("displayName", ["firstName", "lastName", "displayName", "note"]),
    "email": ("defaultEmailAddress", ["defaultEmailAddress"]),
    "addresses": ("addressesV2(first", ["addressesV2"]),
    "summaries": ("numberOfOrders", ["numberOfOrders", "amountSpent", "lastOrder"]),
    "statistics": ("statistics", ["statistics"]),
    "tags": (" tags", ["tags"]),
    "metafields": ("metafields(first", ["metafields"]),
    "tax_id": ("taxSettings", ["taxSettings"]),
    "company_contacts": ("companyContactProfiles", ["companyContactProfiles"]),
}


def consent(state: str = "SUBSCRIBED") -> JsonObject:
    return {
        "state": state,
        "optInLevel": "SINGLE_OPT_IN",
        "collectedFrom": "OTHER",
        "updatedAt": "2026-10-01T00:00:00Z",
        "sourceLocation": {"id": "gid://shopify/Location/1"},
    }


def address(number: int, city: str = "Springfield") -> JsonObject:
    return {
        "id": f"gid://shopify/MailingAddress/{number}",
        "firstName": "Ada",
        "lastName": "Lovelace",
        "name": "Ada Lovelace",
        "company": None,
        "phone": "+15550100",
        "address1": f"{number} Main St",
        "address2": None,
        "zip": "12345",
        "city": city,
        "province": "Oregon",
        "provinceCode": "OR",
        "country": "United States",
        "countryCodeV2": "US",
        "timeZone": "America/Los_Angeles",
        "latitude": 44.5,
        "longitude": -123.25,
        "coordinatesValidated": True,
        "validationResultSummary": "NO_ISSUES",
    }


def metafield(
    number: int, namespace: str = "custom", key: str = "birthday"
) -> JsonObject:
    return {
        "id": f"gid://shopify/Metafield/{number}",
        "namespace": namespace,
        "key": key,
        "type": "single_line_text_field",
        "value": f"value-{number}",
        "createdAt": "2026-10-01T00:00:00Z",
        "updatedAt": "2026-10-01T00:00:00Z",
    }


def customer(
    number: int,
    updated: str = "2026-10-01T00:30:00Z",
    addresses: list[JsonObject] | None = None,
    tags: list[str] | None = None,
    metafields: list[JsonObject] | None = None,
    contacts: list[JsonObject] | None = None,
) -> JsonObject:
    found = addresses if addresses is not None else [address(number * 100)]
    return {
        "id": f"gid://shopify/Customer/{number}",
        "legacyResourceId": str(number),
        "createdAt": "2026-09-01T00:00:00Z",
        "updatedAt": updated,
        "locale": "en",
        "state": "ENABLED",
        "verifiedEmail": True,
        "productSubscriberStatus": "NEVER_SUBSCRIBED",
        "dataSaleOptOut": False,
        "taxExempt": True,
        "taxExemptions": ["CA_BC_RESELLER_EXEMPTION"],
        "defaultAddress": {"id": found[0]["id"]} if found else None,
        "firstName": "Ada",
        "lastName": "Lovelace",
        "displayName": "Ada Lovelace",
        "note": "Prefers email",
        "defaultEmailAddress": {
            "emailAddress": f"ada{number}@example.invalid",
            "validFormat": True,
            "marketingState": "SUBSCRIBED",
            "marketingOptInLevel": "SINGLE_OPT_IN",
            "marketingUpdatedAt": "2026-10-01T00:00:00Z",
            "sourceLocation": None,
            "openTrackingLevel": "OPTED_IN",
        },
        "defaultPhoneNumber": {
            "phoneNumber": "+15550100",
            "smsMarketingConsent": consent(),
            "whatsAppMarketingConsent": None,
        },
        "addressesV2": found,
        "numberOfOrders": "3",
        "amountSpent": {"amount": "123.456789012", "currencyCode": "USD"},
        "lastOrder": {"id": "gid://shopify/Order/9"},
        "statistics": {"predictedSpendTier": "HIGH", "rfmGroup": "CHAMPIONS"},
        "tags": tags if tags is not None else ["vip", "Wholesale"],
        "metafields": metafields if metafields is not None else [],
        "taxSettings": {"taxId": "TAX-1"},
        "companyContactProfiles": contacts or [],
    }


def contact(number: int, locations: list[int]) -> JsonObject:
    return {
        "id": f"gid://shopify/CompanyContact/{number}",
        "title": "Buyer",
        "isMainContact": True,
        "company": {"id": "gid://shopify/Company/7"},
        "roleAssignments": [
            {"companyLocation": {"id": f"gid://shopify/CompanyLocation/{n}"}}
            for n in locations
        ],
    }


def _connection(
    nodes: list[JsonObject], query: str, key: str, after: str | None
) -> JsonObject:
    match = re.search(rf"{re.escape(key)}\(first: (\d+)", query)
    size = int(match[1]) if match else 50
    start = int(after) if after else 0
    page = nodes[start : start + size]
    more = start + size < len(nodes)
    return {
        "nodes": page,
        "pageInfo": {
            "hasNextPage": more,
            "endCursor": str(start + size) if more else None,
        },
    }


@dataclass
class FakeCustomerShop:
    """In-memory shop; tests mutate fields to inject source behavior."""

    customers: dict[str, JsonObject] = field(default_factory=dict)
    scopes: list[str] = field(default_factory=lambda: ["read_customers"])
    denied: set[str] = field(default_factory=set)
    unsupported: set[str] = field(default_factory=set)
    page_size: int = 50
    served_version: str = API_VERSION
    hidden: set[str] = field(default_factory=set)  # present but not listed
    deny_reads: bool = False
    on_page: Callable[[FakeCustomerShop], None] | None = None
    calls: list[str] = field(default_factory=list)

    def add(self, data: JsonObject) -> None:
        self.customers[str(data["id"])] = data

    def post(
        self, url: str, body: bytes, headers: Mapping[str, str], timeout: float
    ) -> HttpResponse:
        del url, headers, timeout
        request = load_object(body, "request")
        query = str(request["query"])
        variables = as_object(request.get("variables", {}), "variables")
        payload = self._route(query, variables)
        return HttpResponse(
            200, self.served_version, None, json.dumps(payload).encode()
        )

    def _error(self, code: str) -> JsonObject:
        return {"errors": [{"message": "no", "extensions": {"code": code}}]}

    def _route(self, query: str, variables: JsonObject) -> JsonObject:
        if "currentAppInstallation" in query:
            return {
                "data": {
                    "shop": {"id": SHOP, "myshopifyDomain": DOMAIN},
                    "currentAppInstallation": {
                        "accessScopes": [{"handle": s} for s in self.scopes]
                    },
                }
            }
        if "customers(first: 1)" in query:
            self.calls.append("probe")
            for group, (keyword, _) in GROUP_KEYWORDS.items():
                if keyword in query:
                    return self._probe(group)
            if "smsMarketingConsent" in query:
                return self._probe("phone")
            if "whatsAppMarketingConsent" in query:
                return self._probe("whatsapp_consent")
            raise AssertionError("unknown probe")
        if self.deny_reads:
            return self._error("ACCESS_DENIED")
        if "... on Customer { id updatedAt }" in query:
            self.calls.append("presence")
            ids = [str(i) for i in _list(variables["ids"])]
            return {
                "data": {
                    "nodes": [
                        {"id": i, "updatedAt": self.customers[i]["updatedAt"]}
                        if i in self.customers
                        else None
                        for i in ids
                    ]
                }
            }
        if "nodes(ids" in query:
            self.calls.append("by_id")
            ids = [str(i) for i in _list(variables["ids"])]
            return {
                "data": {
                    "nodes": [
                        self._shape(self.customers[i], query)
                        if i in self.customers
                        else None
                        for i in ids
                    ]
                }
            }
        if "... on CompanyContact" in query:
            self.calls.append("roles")
            for data in self.customers.values():
                for item in _objs(data["companyContactProfiles"]):
                    if item["id"] == variables["id"]:
                        return {
                            "data": {
                                "node": {
                                    "id": item["id"],
                                    "roleAssignments": _connection(
                                        _objs(item["roleAssignments"]),
                                        query,
                                        "roleAssignments",
                                        _after(variables),
                                    ),
                                }
                            }
                        }
            return {"data": {"node": None}}
        if "node(id" in query:
            self.calls.append("follow")
            key = "addressesV2" if "addressesV2" in query else "metafields"
            found = self.customers.get(str(variables["id"]))
            if found is None:
                return {"data": {"node": None}}
            return {
                "data": {
                    "node": {
                        "id": found["id"],
                        key: _connection(
                            _objs(found[key]), query, key, _after(variables)
                        ),
                    }
                }
            }
        if "customers(" in query:
            self.calls.append("page")
            if self.on_page is not None:
                self.on_page(self)
            search = str(variables.get("search") or "")
            ordered = sorted(
                (c for i, c in self.customers.items() if i not in self.hidden),
                key=lambda c: (str(c["updatedAt"]), str(c["id"])),
            )
            matching = [c for c in ordered if _matches(c, search)]
            start = int(_after(variables) or 0)
            page = matching[start : start + self.page_size]
            more = start + self.page_size < len(matching)
            return {
                "data": {
                    "customers": {
                        "nodes": [self._shape(c, query) for c in page],
                        "pageInfo": {
                            "hasNextPage": more,
                            "endCursor": str(start + self.page_size) if more else None,
                        },
                    }
                }
            }
        raise AssertionError(f"unrouted query: {query[:60]}")

    def _probe(self, group: str) -> JsonObject:
        if group in self.denied:
            return self._error("ACCESS_DENIED")
        if group in self.unsupported:
            return self._error("undefinedField")
        return {"data": {"customers": {"nodes": []}}}

    def _shape(self, data: JsonObject, query: str) -> JsonObject:
        shaped = dict(data)
        for keyword, keys in GROUP_KEYWORDS.values():
            if keyword not in query:
                for key in keys:
                    shaped.pop(key, None)
        phone = shaped.get("defaultPhoneNumber")
        if phone is not None:
            kept = dict(as_object(phone, "phone"))
            if "smsMarketingConsent" not in query:
                kept.pop("smsMarketingConsent", None)
                kept.pop("phoneNumber", None)
            if "whatsAppMarketingConsent" not in query:
                kept.pop("whatsAppMarketingConsent", None)
            shaped["defaultPhoneNumber"] = kept
        elif (
            "smsMarketingConsent" not in query
            and "whatsAppMarketingConsent" not in query
        ):
            shaped.pop("defaultPhoneNumber", None)
        if "addressesV2" in shaped:
            shaped["addressesV2"] = _connection(
                _objs(data["addressesV2"]), query, "addressesV2", None
            )
        if "metafields" in shaped:
            shaped["metafields"] = _connection(
                _objs(data["metafields"]), query, "metafields", None
            )
        if "companyContactProfiles" in shaped:
            shaped["companyContactProfiles"] = [
                {
                    **item,
                    "roleAssignments": _connection(
                        _objs(item["roleAssignments"]), query, "roleAssignments", None
                    ),
                }
                for item in _objs(data["companyContactProfiles"])
            ]
        return shaped


def _list(value: object) -> list[object]:
    assert isinstance(value, list)
    return list(value)  # pyright: ignore[reportUnknownArgumentType]


def _objs(value: object) -> list[JsonObject]:
    return [as_object(item, "item") for item in _list(value)]


def _after(variables: JsonObject) -> str | None:
    value = variables.get("after")
    return None if value is None else str(value)


def _matches(data: JsonObject, search: str) -> bool:
    updated = str(data["updatedAt"])
    lower = re.search(r'updated_at:>="([^"]+)"', search)
    upper = re.search(r'updated_at:<"([^"]+)"', search)
    return (not lower or updated >= lower[1]) and (not upper or updated < upper[1])


def customer_settings(root: Path) -> Settings:
    """Local settings that load ``root / "shopify_customers.duckdb"``."""
    return Settings(
        credentials=ShopifyCredentials(DOMAIN, "fixture-token-not-a-secret"),
        dataset="shopify_customers_raw",
        target=Target("shopify_customers", "local", root, None),
    )
