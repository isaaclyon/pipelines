"""Admin GraphQL client with a per-client request, byte and time budget."""

from __future__ import annotations

import json
import re
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import Protocol, cast

from analytics_ingestion.shopify.errors import (
    AccessDenied,
    BudgetExhausted,
    QueryRejected,
    RetryableSourceError,
    SourceError,
)
from analytics_ingestion.shopify.jsonvalue import (
    JsonObject,
    as_object,
    load_object,
    objects,
)
from analytics_ingestion.shopify.queries import API_VERSION

MAX_RESPONSE_BYTES = 16 * 1024 * 1024
READ_ATTEMPTS = 3
RETRYABLE_HTTP = {429, 500, 502, 503, 504}
# GraphQL validation codes for a field the API version does not define.
INVALID_FIELD_CODES = frozenset(
    {"undefinedField", "undefinedType", "argumentNotAccepted", "selectionMismatch"}
)
SAFE_ERROR_CODE = re.compile(r"[A-Za-z_]{1,64}")


def _code_names(codes: Iterable[object]) -> str:
    """Name Shopify error codes in messages; anything else stays out of them."""
    names = {
        code
        if isinstance(code, str) and SAFE_ERROR_CODE.fullmatch(code)
        else "unrecognized"
        for code in codes
        if code is not None
    }
    return ", ".join(sorted(names)) or "none"


@dataclass(frozen=True)
class ExecutionLimits:
    """Runaway guards for one client; not a freshness target."""

    max_requests: int = 5_000
    max_source_bytes: int = 128 * 1024 * 1024
    max_active_seconds: float = 20 * 60


@dataclass
class Budget:
    """Requests, response bytes and active time one client has used.

    ``clock`` and ``sleeper`` are injectable for offline tests.
    """

    limits: ExecutionLimits
    requests_used: int = 0
    bytes_used: int = 0
    clock: Callable[[], float] = time.monotonic
    sleeper: Callable[[float], None] = time.sleep
    started: float = field(default=-1.0)

    def __post_init__(self) -> None:
        if self.started < 0:
            self.started = self.clock()

    def remaining_seconds(self, reserve_seconds: float = 0.0) -> float:
        remaining = self.limits.max_active_seconds - (self.clock() - self.started)
        if remaining <= reserve_seconds:
            raise BudgetExhausted("Execution time limit reached")
        return remaining

    def take_request(self) -> None:
        self.remaining_seconds()
        if self.requests_used >= self.limits.max_requests:
            raise BudgetExhausted("Request limit reached")
        self.requests_used += 1

    def add_bytes(self, count: int) -> None:
        self.bytes_used += count
        if self.bytes_used > self.limits.max_source_bytes:
            raise BudgetExhausted("Source data limit reached")

    def wait(self, seconds: float) -> None:
        self.remaining_seconds(reserve_seconds=seconds)
        self.sleeper(seconds)


@dataclass(frozen=True)
class HttpResponse:
    status: int
    served_version: str | None
    retry_after: str | None
    body: bytes


class Transport(Protocol):
    def post(
        self, url: str, body: bytes, headers: Mapping[str, str], timeout: float
    ) -> HttpResponse: ...


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: object,
        code: int,
        msg: str,
        headers: object,
        newurl: str,
    ) -> None:
        return None


class UrllibTransport:
    """Standard-library HTTPS transport. Bodies are bounded; no redirects."""

    def __init__(self) -> None:
        self._opener: urllib.request.OpenerDirector = urllib.request.build_opener(
            _NoRedirect()
        )

    def post(
        self, url: str, body: bytes, headers: Mapping[str, str], timeout: float
    ) -> HttpResponse:
        request = urllib.request.Request(url, data=body, headers=dict(headers))
        try:
            # urllib responses are dynamically typed; values are parsed here.
            with self._opener.open(request, timeout=timeout) as response:  # pyright: ignore[reportAny]
                served: str | None = response.headers.get("X-Shopify-API-Version")  # pyright: ignore[reportAny]
                raw: bytes = response.read(MAX_RESPONSE_BYTES + 1)  # pyright: ignore[reportAny]
                status: int = response.status  # pyright: ignore[reportAny]
        except urllib.error.HTTPError as error:
            retry: str | None = error.headers.get("Retry-After")
            return HttpResponse(error.code, None, retry, b"")
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            raise RetryableSourceError("Shopify transport failure") from None
        if len(raw) > MAX_RESPONSE_BYTES:
            raise SourceError("GraphQL response exceeds size limit")
        return HttpResponse(status, served, None, raw)


@dataclass
class RequestStats:
    """Per-path request counts and query cost evidence."""

    requests: dict[str, int] = field(default_factory=dict)
    throttled: int = 0
    # Query cost points from response extensions, for throughput evidence.
    requested_cost: float = 0.0
    actual_cost: float = 0.0
    min_available: float | None = None

    def record(self, path: str) -> None:
        self.requests[path] = self.requests.get(path, 0) + 1

    def record_cost(self, result: JsonObject) -> None:
        extensions = result.get("extensions")
        if not isinstance(extensions, dict):
            return
        cost = cast(dict[str, object], extensions).get("cost")
        if not isinstance(cost, dict):
            return
        values = cast(dict[str, object], cost)
        requested, actual = (
            values.get("requestedQueryCost"),
            values.get("actualQueryCost"),
        )
        if isinstance(requested, int | float):
            self.requested_cost += requested
        if isinstance(actual, int | float):
            self.actual_cost += actual
        status = values.get("throttleStatus")
        if isinstance(status, dict):
            available = cast(dict[str, object], status).get("currentlyAvailable")
            if isinstance(available, int | float) and (
                self.min_available is None or available < self.min_available
            ):
                self.min_available = float(available)


class ShopifyClient:
    def __init__(
        self,
        shop_domain: str,
        access_token: str,
        budget: Budget,
        transport: Transport | None = None,
    ) -> None:
        self.shop_domain = shop_domain
        self._token = access_token
        self.budget = budget
        self.transport: Transport = transport or UrllibTransport()
        self.stats = RequestStats()
        self.url = f"https://{shop_domain}/admin/api/{API_VERSION}/graphql.json"

    def query(
        self,
        document: str,
        variables: Mapping[str, object] | None = None,
        *,
        path: str = "graphql",
    ) -> JsonObject:
        """Return ``data`` of a complete response; partial responses are rejected."""
        body = json.dumps({"query": document, "variables": dict(variables or {})})
        last: SourceError = RetryableSourceError("No attempt made")
        for attempt in range(READ_ATTEMPTS):
            self.budget.take_request()
            delay = (1.0, 2.0, 4.0)[attempt]
            try:
                response = self.transport.post(
                    self.url,
                    body.encode(),
                    {
                        "Content-Type": "application/json",
                        "X-Shopify-Access-Token": self._token,
                    },
                    timeout=min(60.0, self.budget.remaining_seconds()),
                )
                self.stats.record(path)
                self.budget.add_bytes(len(response.body))
                self.budget.remaining_seconds()
                return self._parse(response)
            except RetryableSourceError as error:
                last = error
                delay = max(delay, error.wait)
            if attempt + 1 < READ_ATTEMPTS:
                self.budget.wait(delay)
        raise last

    def _parse(self, response: HttpResponse) -> JsonObject:
        if response.status in RETRYABLE_HTTP:
            wait = 1.0
            if response.retry_after and response.retry_after.isdigit():
                wait = float(response.retry_after)
            if response.status == 429:
                self.stats.throttled += 1
            raise RetryableSourceError(f"Shopify HTTP {response.status}", wait)
        if response.status in {401, 403}:
            raise SourceError(f"Shopify denied access (HTTP {response.status})")
        if response.status != 200:
            raise SourceError(f"Shopify HTTP {response.status}")
        if response.served_version != API_VERSION:
            raise SourceError("Shopify served an unexpected API version")
        result = load_object(response.body, "GraphQL response")
        self.stats.record_cost(result)
        errors = result.get("errors")
        if errors:
            parsed = objects(errors, "errors")
            codes = {
                as_object(error.get("extensions", {}), "extensions").get("code")
                for error in parsed
            }
            if codes == {"THROTTLED"}:
                self.stats.throttled += 1
                raise RetryableSourceError(
                    "Shopify throttled the request", self._throttle_wait(result)
                )
            if "ACCESS_DENIED" in codes:
                raise AccessDenied("Shopify denied access to a selected field")
            if codes & INVALID_FIELD_CODES:
                raise QueryRejected("Shopify rejected a selected field")
            raise SourceError(
                f"GraphQL rejected the request (codes: {_code_names(codes)});"
                " inspect query and scopes"
            )
        return as_object(result.get("data"), "data")

    @staticmethod
    def _throttle_wait(result: JsonObject) -> float:
        try:
            cost = as_object(
                as_object(result.get("extensions"), "extensions").get("cost"), "cost"
            )
            status = as_object(cost.get("throttleStatus"), "throttleStatus")
            requested = cost.get("requestedQueryCost")
            available = status.get("currentlyAvailable")
            rate = status.get("restoreRate")
            if (
                isinstance(requested, int | float)
                and isinstance(available, int | float)
                and isinstance(rate, int | float)
                and rate > 0
            ):
                return max(
                    1.0, (float(requested) - float(available)) / float(rate) + 0.5
                )
        except SourceError:
            pass
        return 2.0
