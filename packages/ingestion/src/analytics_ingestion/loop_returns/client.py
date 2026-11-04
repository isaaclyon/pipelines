"""Loop Returns list-endpoint client: paged reads, bounded retries and window splits.

Each request pads its window by one second so a server with exclusive bounds
still returns boundary rows. Rows outside the true half-open window are
dropped here, so each row belongs to exactly one window.

Every request opts into pagination (``paginate=true``) at the maximum page
size and follows ``nextPageUrl`` until it is null. A next-page URL is sent
only when it names the list endpoint over HTTPS, because the request carries
the API key. A window that needs too many pages is split, not truncated.
"""

from __future__ import annotations

import http.client
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from http.client import HTTPResponse
from typing import cast

from analytics_ingestion.loop_returns.contract import ReturnHeader, parse_return
from analytics_ingestion.loop_returns.errors import (
    ConfigurationError,
    ContractError,
    SourceError,
)

BASE_URL = "https://api.loopreturns.com/api/v1"
LIST_PATH = "/warehouse/return/list"
FILTER = "created_at"
PAGE_SIZE = 750
MAX_PAGES_PER_WINDOW = 100
USER_AGENT = "analytics-platform/1.0 (loop-returns ingest)"
DEFAULT_TIMEOUT_SECONDS = 180.0
MAX_ATTEMPTS = 4
BACKOFF_BASE_SECONDS = 2.0
BACKOFF_CAP_SECONDS = 30.0
RETRY_AFTER_CAP_SECONDS = 120.0
MIN_WINDOW = timedelta(seconds=3600)
PAD = timedelta(seconds=1)
PAGE_KEYS = frozenset({"returns", "nextPageUrl"})
OPTIONAL_PAGE_KEYS = frozenset({"previousPageUrl"})

Response = tuple[int, Mapping[str, str], bytes]
HttpGet = Callable[[str, dict[str, str], float], Response]


class _RejectRedirect(urllib.request.HTTPRedirectHandler):
    """Never forward API credentials through an HTTP redirect."""

    def redirect_request(
        self,
        req: object,
        fp: object,
        code: int,
        msg: str,
        headers: object,
        newurl: str,
    ) -> None:
        return None


def _open_url(request: urllib.request.Request, timeout: float) -> HTTPResponse:
    opener = urllib.request.build_opener(_RejectRedirect())
    return cast(HTTPResponse, opener.open(request, timeout=timeout))


def urllib_get(url: str, headers: dict[str, str], timeout: float) -> Response:
    """Standard-library GET.

    Network failures, dropped connections and truncated bodies return status 0,
    which the client retries.
    """
    request = urllib.request.Request(url, headers=headers, method="GET")  # noqa: S310
    try:
        opened = _open_url(request, timeout=timeout)
        with opened as response:
            found = {name: value for name, value in response.headers.items()}
            return response.status, found, response.read()
    except urllib.error.HTTPError as error:
        failed = {name: value for name, value in error.headers.items()}
        return error.code, failed, _error_body(error)
    except (urllib.error.URLError, OSError, http.client.HTTPException):
        return 0, {}, b""


def _error_body(error: urllib.error.HTTPError) -> bytes:
    """The status code is the signal; an unreadable error body is not fatal."""
    try:
        return error.read()
    except (OSError, http.client.HTTPException):
        return b""


@dataclass
class ClientStats:
    requests: int = 0
    splits: int = 0


class _SplitWindow(Exception):
    """Internal: one request chain for the window was too slow or too long."""


class LoopReturnsClient:
    def __init__(
        self,
        api_key: str,
        shop_id: str,
        *,
        http_get: HttpGet = urllib_get,
        sleep: Callable[[float], None] = time.sleep,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        if not api_key:
            raise ConfigurationError("Loop Returns api key is missing")
        if not shop_id:
            raise ConfigurationError("Loop Returns shop id is missing")
        self._api_key = api_key
        self._shop_id = shop_id
        self._http_get = http_get
        self._sleep = sleep
        self._timeout = timeout
        self.stats = ClientStats()

    def fetch_window(self, start: datetime, end: datetime) -> list[ReturnHeader]:
        """Return every header created in ``[start, end)``, each once."""
        if start.tzinfo is None or end.tzinfo is None or end <= start:
            raise ValueError("window needs timezone-aware bounds with start before end")
        found: dict[str, ReturnHeader] = {}
        for header in self._fetch(start.astimezone(UTC), end.astimezone(UTC)):
            seen = found.get(header.return_id)
            if seen is None:
                found[header.return_id] = header
            elif seen != header:
                raise ContractError(
                    f"conflicting copies of return {header.return_id} in one window"
                )
        return list(found.values())

    def _fetch(self, start: datetime, end: datetime) -> list[ReturnHeader]:
        try:
            return self._request(start, end)
        except _SplitWindow:
            if end - start <= MIN_WINDOW:
                raise SourceError(
                    f"window {start.isoformat()} .. {end.isoformat()} still exceeds "
                    "the time or page limit at the minimum split size; investigate "
                    "the API before rerunning"
                ) from None
            self.stats.splits += 1
            middle = start + timedelta(seconds=int((end - start).total_seconds()) // 2)
            return self._fetch(start, middle) + self._fetch(middle, end)

    def _request(self, start: datetime, end: datetime) -> list[ReturnHeader]:
        low, high = start - PAD, end + PAD
        query = urllib.parse.urlencode(
            {
                "from": _atom(low),
                "to": _atom(high),
                "filter": FILTER,
                "paginate": "true",
                "pageSize": str(PAGE_SIZE),
            }
        )
        url: str | None = f"{BASE_URL}{LIST_PATH}?{query}"
        requested: set[str] = set()
        headers: list[ReturnHeader] = []
        pages = 0
        while url is not None:
            pages += 1
            if pages > MAX_PAGES_PER_WINDOW:
                raise _SplitWindow()
            requested.add(url)
            payload = self._get(url)
            # The API returns a bare [] for an empty initial window. Keep
            # continuation pages strict: a missing next-page signal there
            # cannot certify the rows already accumulated as complete.
            if pages == 1 and payload == []:
                return []
            records, next_url = _page(payload)
            for record in records:
                header = parse_return(record, self._shop_id)
                if not low <= header.created_at <= high:
                    raise SourceError(
                        f"return {header.return_id} was created outside "
                        "the requested range"
                    )
                if start <= header.created_at < end:
                    headers.append(header)
            if next_url is not None and next_url in requested:
                raise SourceError("Loop Returns pagination cycle: a page URL repeated")
            url = next_url
        return headers

    def _get(self, url: str) -> object:
        request_headers = {
            "X-Authorization": self._api_key,
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
        }
        for attempt in range(1, MAX_ATTEMPTS + 1):
            self.stats.requests += 1
            status, response_headers, raw = self._http_get(
                url, request_headers, self._timeout
            )
            if status == 200:
                try:
                    payload: object = json.loads(raw)  # pyright: ignore[reportAny]
                    return payload
                except ValueError:
                    raise SourceError("Loop Returns response is not JSON") from None
            if status == 504:
                raise _SplitWindow()
            if status in (0, 408, 429) or status >= 500:
                if attempt == MAX_ATTEMPTS:
                    raise SourceError(
                        f"Loop Returns request failed after {MAX_ATTEMPTS} attempts "
                        f"(last status {status})"
                    )
                self._sleep(_wait_seconds(attempt, response_headers))
                continue
            raise SourceError(f"Loop Returns request returned HTTP {status}")
        raise AssertionError("unreachable: every attempt returns or raises")


def _atom(moment: datetime) -> str:
    """UTC ISO 8601 with an explicit offset, as the API's date validator expects."""
    return moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S+00:00")


def _wait_seconds(attempt: int, headers: Mapping[str, str]) -> float:
    retry_after = {key.lower(): value for key, value in headers.items()}.get(
        "retry-after"
    )
    if retry_after is not None and retry_after.strip().isdigit():
        return min(float(retry_after), RETRY_AFTER_CAP_SECONDS)
    exponent = attempt - 1
    backoff = BACKOFF_BASE_SECONDS * (2.0**exponent)
    return min(backoff, BACKOFF_CAP_SECONDS)


def _page(payload: object) -> tuple[list[dict[str, object]], str | None]:
    """Read one paginated envelope: its records and the next page URL, if any.

    The API documents ``returns`` and ``nextPageUrl`` on every paginated
    response. A bare list has no next-page signal, so it is rejected.
    """
    if not isinstance(payload, dict):
        raise SourceError("Loop Returns response has an unrecognized envelope")
    envelope = cast(dict[str, object], payload)
    keys = set(envelope)
    if not PAGE_KEYS <= keys or not keys <= PAGE_KEYS | OPTIONAL_PAGE_KEYS:
        raise SourceError("Loop Returns response has an unrecognized envelope")
    rows = envelope["returns"]
    if not isinstance(rows, list):
        raise SourceError("Loop Returns response has an unrecognized envelope")
    records: list[dict[str, object]] = []
    for item in cast(list[object], rows):
        if not isinstance(item, dict):
            raise SourceError("Loop Returns response has an unrecognized envelope")
        records.append(cast(dict[str, object], item))
    return records, _next_page(envelope["nextPageUrl"])


def _next_page(value: object) -> str | None:
    """Accept a next-page URL only when it names the list endpoint over HTTPS.

    The request that follows carries the API key, so any other origin or path
    fails before it is sent. The vendor's next-page links may not repeat the
    request's ``from`` and ``to`` values, so those are checked per row instead.
    """
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise SourceError("Loop Returns next page URL is not usable; nothing was sent")
    base = urllib.parse.urlsplit(BASE_URL)
    try:
        parts = urllib.parse.urlsplit(value)
        port = parts.port
    except ValueError:
        raise SourceError(_refused_next_page()) from None
    query = urllib.parse.parse_qs(parts.query, keep_blank_values=True)
    if (
        parts.scheme != "https"
        or parts.hostname != base.hostname
        or port not in (None, 443)
        or parts.username is not None
        or parts.password is not None
        or parts.fragment
        or parts.path != f"{base.path}{LIST_PATH}"
        or query.get("filter", [FILTER]) != [FILTER]
    ):
        raise SourceError(_refused_next_page())
    return value


def _refused_next_page() -> str:
    return (
        "Loop Returns next page URL is not the list endpoint over HTTPS; "
        "nothing was sent"
    )
