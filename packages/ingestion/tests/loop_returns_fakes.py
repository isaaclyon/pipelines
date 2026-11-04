"""Deterministic Loop Returns list-endpoint fake for offline tests.

The fake answers ``GET /warehouse/return/list`` from an in-memory record list.
Its boundary semantics, size limit and scripted failures are configurable so
tests can prove the client's half-open windows without the live API.
"""

from __future__ import annotations

import json
import urllib.parse
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from analytics_ingestion.loop_returns.client import BASE_URL, LIST_PATH

Response = tuple[int, dict[str, str], bytes]
SHOP_ID = "gid://shopify/Shop/1"
API_KEY = "loop-fixture-key-not-a-secret"


def instant(text: str) -> datetime:
    return datetime.fromisoformat(text).astimezone(UTC)


def api_return(
    return_id: str | int,
    created: str,
    *,
    state: object = "closed",
    order_id: object = "12600441897332",
    **changes: object,
) -> dict[str, object]:
    """One list-endpoint object with the fields the source reads plus PII."""
    record: dict[str, object] = {
        "id": return_id,
        "state": state,
        "created_at": created,
        "updated_at": created,
        "provider_order_id": order_id,
        "order_name": "#1001",
        "currency": "USD",
        "return_total": "189.48",
        "refund": "174.48",
        "handling_fee": "15",
        "gift_card": "0",
        "exchange_total": "N/A",
        "upsell": "0.00",
        "customer": "buyer@example.com",
        "email": "buyer@example.com",
        "tracking_number": "1Z-fixture",
        "line_items": [{"line_item_id": 1, "sku": "SKU-1"}],
        "exchanges": [],
    }
    record.update(changes)
    return record


@dataclass(frozen=True)
class Call:
    url: str
    headers: dict[str, str]
    timeout: float
    start: datetime | None
    end: datetime | None
    filter_mode: str
    paginate: str | None
    page_size: str | None
    cursor: str | None


@dataclass
class FakeLoopApi:
    """Answers list requests from ``records`` as paginated envelopes.

    ``exclusive_bounds`` makes both ends of the server filter exclusive; the
    default is ``[from, to)``. ``max_span`` answers 504 for longer windows.
    ``page_limit`` caps the rows per page below the requested ``pageSize``, so
    tests can force several pages. ``scripted`` supplies responses for the next
    requests, in order.
    """

    records: list[dict[str, object]] = field(default_factory=list)
    exclusive_bounds: bool = False
    max_span: timedelta | None = None
    page_limit: int | None = None
    scripted: list[Response] = field(default_factory=list)
    calls: list[Call] = field(default_factory=list)

    def __call__(self, url: str, headers: dict[str, str], timeout: float) -> Response:
        parsed = urllib.parse.urlsplit(url)
        assert (
            f"{BASE_URL}{LIST_PATH}"
            == f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        )
        query = dict(urllib.parse.parse_qsl(parsed.query))
        # Scripted next-page links may omit ``from`` and ``to``, as the vendor's
        # documented example does, so the window is read only when it is served.
        start = instant(query["from"]) if "from" in query else None
        end = instant(query["to"]) if "to" in query else None
        self.calls.append(
            Call(
                url,
                dict(headers),
                timeout,
                start,
                end,
                query["filter"],
                query.get("paginate"),
                query.get("pageSize"),
                query.get("cursor"),
            )
        )
        if self.scripted:
            return self.scripted.pop(0)
        assert start is not None and end is not None
        if self.max_span is not None and end - start > self.max_span:
            return 504, {}, b"gateway timeout"
        rows = sorted(
            (row for row in self.records if self._inside(row, start, end)),
            key=lambda row: instant(str(row["created_at"])),
        )
        offset = int(query.get("cursor", "0"))
        size = int(query.get("pageSize", "100"))
        if self.page_limit is not None:
            size = min(size, self.page_limit)
        page = rows[offset : offset + size]
        next_url: str | None = None
        if offset + size < len(rows):
            next_query = {**query, "cursor": str(offset + size)}
            next_url = f"{BASE_URL}{LIST_PATH}?{urllib.parse.urlencode(next_query)}"
        body = {"returns": page, "nextPageUrl": next_url, "previousPageUrl": None}
        return 200, {"x-ratelimit-remaining": "250"}, json.dumps(body).encode()

    def _inside(self, row: dict[str, object], start: datetime, end: datetime) -> bool:
        created = instant(str(row["created_at"]))
        if self.exclusive_bounds:
            return start < created < end
        return start <= created < end


def ok(rows: list[dict[str, object]]) -> Response:
    return page(rows)


def page(rows: list[dict[str, object]], next_page_url: str | None = None) -> Response:
    """One paginated envelope, as the list endpoint returns for ``paginate=true``."""
    body = {"returns": rows, "nextPageUrl": next_page_url, "previousPageUrl": None}
    return 200, {}, json.dumps(body).encode()


def status(code: int, headers: dict[str, str] | None = None) -> Response:
    return code, headers or {}, b"fixture failure body"
