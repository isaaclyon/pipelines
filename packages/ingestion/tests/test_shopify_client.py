"""Offline Admin GraphQL client and bulk-file behavior."""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path

import pytest

from analytics_ingestion.shopify.client import (
    Budget,
    ExecutionLimits,
    HttpResponse,
    ShopifyClient,
)
from analytics_ingestion.shopify.errors import (
    BudgetExhausted,
    ConfigurationError,
    RetryableSourceError,
    SourceError,
)
from analytics_ingestion.shopify.queries import API_VERSION, search_window
from analytics_ingestion.shopify.settings import settings_from_env


class Replies:
    """Transport returning queued responses or raising queued errors."""

    def __init__(self, replies: list[HttpResponse | Exception]) -> None:
        self.replies = replies
        self.posts = 0

    def post(
        self, url: str, body: bytes, headers: Mapping[str, str], timeout: float
    ) -> HttpResponse:
        del url, body, headers, timeout
        self.posts += 1
        reply = self.replies.pop(0)
        if isinstance(reply, Exception):
            raise reply
        return reply


def ok(payload: object, version: str = API_VERSION) -> HttpResponse:
    return HttpResponse(200, version, None, json.dumps(payload).encode())


def client(transport: Replies, limits: ExecutionLimits | None = None) -> ShopifyClient:
    budget = Budget(limits or ExecutionLimits(), sleeper=lambda _: None)
    return ShopifyClient("fixture.myshopify.com", "fixture", budget, transport)


def test_transport_and_throttle_failures_share_three_attempts() -> None:
    throttled = ok({"errors": [{"extensions": {"code": "THROTTLED"}}]})
    transport = Replies(
        [RetryableSourceError("fixture"), throttled, HttpResponse(503, None, "1", b"")]
    )
    shopify = client(transport)
    with pytest.raises(RetryableSourceError):
        shopify.query("query { shop { id } }")
    assert transport.posts == shopify.budget.requests_used == 3
    assert shopify.stats.throttled == 1


def test_partial_errors_denials_and_version_fallback_fail_closed() -> None:
    transport = Replies(
        [
            ok({"data": {"orders": []}, "errors": [{"message": "fixture"}]}),
            ok({"data": None, "errors": [{"extensions": {"code": "ACCESS_DENIED"}}]}),
            ok({"data": {}}, version="2026-07"),
            HttpResponse(403, None, None, b""),
        ]
    )
    shopify = client(transport)
    for message in ("rejected", "denied access", "API version", "HTTP 403"):
        with pytest.raises(SourceError, match=message):
            shopify.query("query")
    assert transport.posts == 4


def test_rejected_request_names_safe_shopify_error_codes_only() -> None:
    def errors(*codes: object) -> HttpResponse:
        return ok({"errors": [{"extensions": {"code": code}} for code in codes]})

    transport = Replies(
        [
            errors("INTERNAL_SERVER_ERROR", "MAX_COST_EXCEEDED"),
            errors("secret token https://example.test"),
            errors(None),
        ]
    )
    shopify = client(transport)
    expected = (
        "codes: INTERNAL_SERVER_ERROR, MAX_COST_EXCEEDED",
        "codes: unrecognized",
        "codes: none",
    )
    for fragment in expected:
        with pytest.raises(SourceError, match=fragment) as raised:
            shopify.query("query")
        assert "secret" not in str(raised.value)


def test_request_and_time_limits_stop_work() -> None:
    shopify = client(Replies([ok({"data": {}})]), ExecutionLimits(max_requests=1))
    shopify.query("query")
    with pytest.raises(BudgetExhausted, match="Request limit"):
        shopify.query("query")

    budget = Budget(ExecutionLimits(max_active_seconds=10), clock=lambda: 0.0)
    with pytest.raises(BudgetExhausted, match="time limit"):
        budget.wait(10)


def test_late_response_cannot_complete_after_the_time_limit() -> None:
    ticks = iter([0.0, 0.0, 0.0, 100.0, 100.0])
    budget = Budget(ExecutionLimits(max_active_seconds=10), clock=lambda: next(ticks))
    shopify = ShopifyClient(
        "fixture.myshopify.com", "fixture", budget, Replies([ok({"data": {}})])
    )
    with pytest.raises(BudgetExhausted):
        shopify.query("query")


def test_window_searches_are_quoted_utc_half_open() -> None:
    start = datetime.fromisoformat("2026-10-05T10:00:00-07:00")
    end = datetime.fromisoformat("2026-10-05T18:00:00+00:00")
    assert search_window("created_at", start, end) == (
        'created_at:>="2026-10-05T17:00:00Z" created_at:<"2026-10-05T18:00:00Z"'
    )
    with pytest.raises(ValueError, match="timezone"):
        search_window("updated_at", datetime(2026, 10, 5), end)


def test_configuration_fails_before_any_read_or_write(tmp_path: Path) -> None:
    credentials = tmp_path / "secrets" / "shopify.json"
    credentials.parent.mkdir()
    credentials.write_text(
        json.dumps({"shop_domain": "example.com", "access_token": "x" * 32})
    )
    base = {
        "ANALYTICS_SOURCE_SECRETS_ROOT": str(credentials.parent),
        "ANALYTICS_INGESTION_STATE_ROOT": str(tmp_path),
    }
    with pytest.raises(ConfigurationError, match="ANALYTICS_ENVIRONMENT"):
        settings_from_env(base)
    with pytest.raises(ConfigurationError, match="myshopify"):
        settings_from_env({**base, "ANALYTICS_ENVIRONMENT": "local"})
    credentials.write_text(
        json.dumps({"shop_domain": "example.myshopify.com", "access_token": "x" * 32})
    )
    with pytest.raises(ConfigurationError, match="SNOWFLAKE_"):
        settings_from_env({**base, "ANALYTICS_ENVIRONMENT": "development"})
    local = settings_from_env({**base, "ANALYTICS_ENVIRONMENT": "local"})
    assert local.target.root == tmp_path / "shopify_orders" / "local"


def test_graphql_responses_consume_the_source_byte_budget() -> None:
    transport = Replies([ok({"data": {"shop": None}})])
    reader = client(transport, ExecutionLimits(max_source_bytes=1))
    with pytest.raises(BudgetExhausted, match="Source data"):
        reader.query("query { shop { id } }")
    assert reader.budget.bytes_used > 1
