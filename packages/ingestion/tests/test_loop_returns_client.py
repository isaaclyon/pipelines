"""Loop Returns list client: request shape, paging, retries, splits and checks."""

from __future__ import annotations

import email.message
import http.client
import json
import urllib.error
import urllib.request

import pytest

from analytics_ingestion.loop_returns import client as client_module
from analytics_ingestion.loop_returns.client import (
    BASE_URL,
    LIST_PATH,
    PAGE_SIZE,
    USER_AGENT,
    LoopReturnsClient,
    urllib_get,
)
from analytics_ingestion.loop_returns.errors import (
    ConfigurationError,
    ContractError,
    SourceError,
)
from tests.loop_returns_fakes import (
    API_KEY,
    SHOP_ID,
    FakeLoopApi,
    api_return,
    instant,
    ok,
    page,
    status,
)

START = instant("2026-07-01T00:00:00+00:00")
END = instant("2026-07-08T00:00:00+00:00")
NEXT_URL = f"{BASE_URL}{LIST_PATH}?filter=created_at&pageSize=750&cursor=2"


def client(api: FakeLoopApi, sleeps: list[float] | None = None) -> LoopReturnsClient:
    return LoopReturnsClient(
        API_KEY,
        SHOP_ID,
        http_get=api,
        sleep=(sleeps if sleeps is not None else []).append,
    )


def test_request_pins_endpoint_filter_auth_and_product_agent() -> None:
    api = FakeLoopApi()
    _ = client(api).fetch_window(START, END)

    call = api.calls[0]
    assert call.url.split("?")[0].endswith(LIST_PATH)
    assert call.filter_mode == "created_at"
    assert call.headers["X-Authorization"] == API_KEY
    assert call.headers["User-Agent"] == USER_AGENT
    assert USER_AGENT.startswith("analytics-platform/")


def test_request_opts_into_pagination_at_the_maximum_page_size() -> None:
    api = FakeLoopApi()
    _ = client(api).fetch_window(START, END)

    call = api.calls[0]
    assert call.paginate == "true"
    assert call.page_size == "750"
    assert PAGE_SIZE == 750


def test_window_request_pads_bounds_by_one_second() -> None:
    api = FakeLoopApi()
    _ = client(api).fetch_window(START, END)

    call = api.calls[0]
    assert call.start == instant("2026-06-30T23:59:59+00:00")
    assert call.end == instant("2026-07-08T00:00:01+00:00")


@pytest.mark.parametrize("exclusive", [False, True])
def test_boundary_rows_belong_to_exactly_one_half_open_window(exclusive: bool) -> None:
    api = FakeLoopApi(
        records=[
            api_return("1", "2026-07-01T00:00:00+00:00"),
            api_return("2", "2026-07-07T23:59:59+00:00"),
            api_return("3", "2026-07-08T00:00:00+00:00"),
        ],
        exclusive_bounds=exclusive,
    )
    first = client(api).fetch_window(START, END)
    second = client(api).fetch_window(END, instant("2026-07-15T00:00:00+00:00"))

    assert [row.return_id for row in first] == ["1", "2"]
    assert [row.return_id for row in second] == ["3"]


def test_every_page_is_fetched_by_following_next_page_urls() -> None:
    records = [api_return(str(n), f"2026-07-02T00:00:0{n}+00:00") for n in range(5)]
    api = FakeLoopApi(records=records, page_limit=2)

    rows = client(api).fetch_window(START, END)

    assert sorted(row.return_id for row in rows) == ["0", "1", "2", "3", "4"]
    assert len(api.calls) == 3
    assert api.calls[1].cursor == "2"
    assert api.calls[2].cursor == "4"


def test_next_page_url_is_requested_exactly_as_returned() -> None:
    api = FakeLoopApi()
    api.scripted = [
        page([api_return("1", "2026-07-02T00:00:00+00:00")], NEXT_URL),
        page([api_return("2", "2026-07-03T00:00:00+00:00")]),
    ]

    _ = client(api).fetch_window(START, END)

    assert api.calls[1].url == NEXT_URL
    assert api.calls[1].headers["X-Authorization"] == API_KEY


def test_rows_that_repeat_across_pages_are_returned_once() -> None:
    row = api_return("1", "2026-07-02T00:00:00+00:00")
    api = FakeLoopApi()
    api.scripted = [
        page([row], NEXT_URL),
        page([row, api_return("2", "2026-07-03T00:00:00+00:00")]),
    ]

    rows = client(api).fetch_window(START, END)

    assert sorted(item.return_id for item in rows) == ["1", "2"]


def test_conflicting_copies_across_pages_fail_closed() -> None:
    api = FakeLoopApi()
    api.scripted = [
        page([api_return("1", "2026-07-02T00:00:00+00:00")], NEXT_URL),
        page([api_return("1", "2026-07-02T00:00:00+00:00", state="open")]),
    ]

    with pytest.raises(ContractError, match="conflicting"):
        _ = client(api).fetch_window(START, END)


def test_row_outside_the_window_on_a_later_page_fails_instead_of_being_dropped() -> (
    None
):
    api = FakeLoopApi()
    api.scripted = [
        page([api_return("1", "2026-07-02T00:00:00+00:00")], NEXT_URL),
        page([api_return("2", "2026-06-01T00:00:00+00:00")]),
    ]

    with pytest.raises(SourceError, match="outside"):
        _ = client(api).fetch_window(START, END)


@pytest.mark.parametrize(
    "next_url",
    [
        "http://api.loopreturns.com/api/v1/warehouse/return/list?cursor=2",
        "https://api.loopreturns.com.evil.example/api/v1/warehouse/return/list?cursor=2",
        "https://evil.example/api/v1/warehouse/return/list?cursor=2",
        "https://api.loopreturns.com:8443/api/v1/warehouse/return/list?cursor=2",
        "https://user:secret@api.loopreturns.com/api/v1/warehouse/return/list?cursor=2",
        "https://api.loopreturns.com/api/v2/warehouse/return/list?cursor=2",
        "https://api.loopreturns.com/api/v1/warehouse/return/other?cursor=2",
        "https://api.loopreturns.com/api/v1/warehouse/return/list?filter=updated_at",
        "/api/v1/warehouse/return/list?cursor=2",
        "https://api.loopreturns.com/api/v1/warehouse/return/list?cursor=2#frag",
        "",
    ],
)
def test_next_page_url_off_the_list_origin_is_never_requested(next_url: str) -> None:
    api = FakeLoopApi()
    api.scripted = [page([api_return("1", "2026-07-02T00:00:00+00:00")], next_url)]

    with pytest.raises(SourceError, match="next page"):
        _ = client(api).fetch_window(START, END)

    assert len(api.calls) == 1


def test_non_string_next_page_url_fails() -> None:
    api = FakeLoopApi()
    body: dict[str, object] = {"returns": [], "nextPageUrl": 5}
    api.scripted = [(200, {}, json.dumps(body).encode())]

    with pytest.raises(SourceError, match="next page"):
        _ = client(api).fetch_window(START, END)

    assert len(api.calls) == 1


def test_repeated_next_page_url_fails_as_a_cycle() -> None:
    api = FakeLoopApi()
    api.scripted = [
        page([api_return("1", "2026-07-02T00:00:00+00:00")], NEXT_URL),
        page([api_return("2", "2026-07-03T00:00:00+00:00")], NEXT_URL),
    ]

    with pytest.raises(SourceError, match="cycle"):
        _ = client(api).fetch_window(START, END)

    assert len(api.calls) == 2


def test_non_consecutive_repeat_of_a_page_url_fails_as_a_cycle() -> None:
    other = f"{BASE_URL}{LIST_PATH}?filter=created_at&pageSize=750&cursor=3"
    api = FakeLoopApi()
    api.scripted = [
        page([api_return("1", "2026-07-02T00:00:00+00:00")], NEXT_URL),
        page([api_return("2", "2026-07-03T00:00:00+00:00")], other),
        page([api_return("3", "2026-07-04T00:00:00+00:00")], NEXT_URL),
    ]

    with pytest.raises(SourceError, match="cycle"):
        _ = client(api).fetch_window(START, END)

    assert len(api.calls) == 3


def test_page_bound_splits_the_window_instead_of_returning_partial_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(client_module, "MAX_PAGES_PER_WINDOW", 2)
    records = [
        api_return("1", "2026-07-01T12:00:00+00:00"),
        api_return("2", "2026-07-02T12:00:00+00:00"),
        api_return("3", "2026-07-06T12:00:00+00:00"),
    ]
    api = FakeLoopApi(records=records, page_limit=1)
    made = client(api)

    rows = made.fetch_window(START, END)

    assert sorted(row.return_id for row in rows) == ["1", "2", "3"]
    assert made.stats.splits >= 1


def test_page_bound_at_minimum_window_fails_instead_of_truncating(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(client_module, "MAX_PAGES_PER_WINDOW", 2)
    records = [api_return(str(n), f"2026-07-01T00:00:0{n}+00:00") for n in range(1, 5)]
    api = FakeLoopApi(records=records, page_limit=1)

    with pytest.raises(SourceError, match="minimum"):
        _ = client(api).fetch_window(START, instant("2026-07-01T00:30:00+00:00"))


def test_split_on_repeated_gateway_timeout_returns_the_same_rows() -> None:
    records = [
        api_return("1", "2026-07-01T12:00:00+00:00"),
        api_return("2", "2026-07-05T12:00:00+00:00"),
    ]
    api = FakeLoopApi(
        records=records, max_span=instant("2026-07-04T00:00:00+00:00") - START
    )
    api.scripted = [status(504), status(504)]
    made = client(api)

    rows = made.fetch_window(START, END)

    assert sorted(row.return_id for row in rows) == ["1", "2"]
    assert made.stats.splits >= 1
    assert made.stats.requests > 2


def test_transient_server_error_is_retried_with_backoff() -> None:
    api = FakeLoopApi(records=[api_return("1", "2026-07-02T00:00:00+00:00")])
    api.scripted = [status(500), status(503)]
    sleeps: list[float] = []

    rows = client(api, sleeps).fetch_window(START, END)

    assert [row.return_id for row in rows] == ["1"]
    assert sleeps == [2.0, 4.0]


@pytest.mark.parametrize("code", [408, 429, 500, 502, 503])
def test_transient_status_recovers_on_a_later_attempt(code: int) -> None:
    api = FakeLoopApi(records=[api_return("1", "2026-07-02T00:00:00+00:00")])
    api.scripted = [status(code)]
    made = client(api, [])

    rows = made.fetch_window(START, END)

    assert [row.return_id for row in rows] == ["1"]


def test_retry_after_is_honored_for_rate_limits() -> None:
    api = FakeLoopApi()
    api.scripted = [status(429, {"retry-after": "7"})]
    sleeps: list[float] = []

    _ = client(api, sleeps).fetch_window(START, END)

    assert sleeps == [7.0]


def test_persistent_server_error_fails_after_bounded_attempts() -> None:
    api = FakeLoopApi()
    api.scripted = [status(500)] * 4
    sleeps: list[float] = []

    with pytest.raises(SourceError, match="attempts"):
        _ = client(api, sleeps).fetch_window(START, END)

    assert len(api.calls) == 4
    assert len(sleeps) == 3


@pytest.mark.parametrize("code", [401, 403, 422])
def test_authorization_and_contract_rejections_do_not_retry(code: int) -> None:
    api = FakeLoopApi()
    api.scripted = [status(code)]

    with pytest.raises(SourceError):
        _ = client(api).fetch_window(START, END)

    assert len(api.calls) == 1


def test_window_that_still_times_out_at_minimum_size_fails_permanently() -> None:
    api = FakeLoopApi(max_span=instant("2026-07-01T00:30:00+00:00") - START)

    with pytest.raises(SourceError, match="minimum"):
        _ = client(api).fetch_window(START, instant("2026-07-01T02:00:00+00:00"))


@pytest.mark.parametrize(
    "body",
    [
        {"returns": [api_return("1", "2026-07-02T00:00:00+00:00")], "next": "page-2"},
        {"returns": []},
        {"other": []},
        "not a list",
        [api_return("1", "2026-07-02T00:00:00+00:00")],
    ],
)
def test_unrecognized_envelope_fails_instead_of_truncating(body: object) -> None:
    api = FakeLoopApi()
    api.scripted = [(200, {}, json.dumps(body).encode())]

    with pytest.raises(SourceError, match="envelope"):
        _ = client(api).fetch_window(START, END)


def test_bare_empty_first_response_is_an_empty_window() -> None:
    # The live API returns [] for an empty creation-time window, even with
    # paginate=true. Nonempty bare arrays still lack completeness evidence.
    api = FakeLoopApi()
    api.scripted = [(200, {}, b"[]")]

    assert client(api).fetch_window(START, END) == []
    assert len(api.calls) == 1


def test_bare_empty_continuation_does_not_certify_a_partial_window() -> None:
    api = FakeLoopApi()
    api.scripted = [
        page([api_return("1", "2026-07-02T00:00:00+00:00")], NEXT_URL),
        (200, {}, b"[]"),
    ]

    with pytest.raises(SourceError, match="envelope"):
        _ = client(api).fetch_window(START, END)


def test_paged_envelope_with_null_next_page_is_accepted() -> None:
    api = FakeLoopApi()
    rows = [api_return("1", "2026-07-02T00:00:00+00:00")]
    body = {"returns": rows, "nextPageUrl": None}
    api.scripted = [(200, {}, json.dumps(body).encode())]

    assert [row.return_id for row in client(api).fetch_window(START, END)] == ["1"]


def test_row_outside_the_requested_range_fails_instead_of_being_dropped() -> None:
    api = FakeLoopApi()
    api.scripted = [ok([api_return("1", "2026-06-01T00:00:00+00:00")])]

    with pytest.raises(SourceError, match="outside"):
        _ = client(api).fetch_window(START, END)


def test_contract_violation_in_a_row_is_reported() -> None:
    api = FakeLoopApi()
    api.scripted = [ok([api_return("1", "2026-07-02T00:00:00+00:00", state="")])]

    with pytest.raises(ContractError, match="state"):
        _ = client(api).fetch_window(START, END)


def test_empty_api_key_is_a_configuration_error() -> None:
    with pytest.raises(ConfigurationError):
        _ = LoopReturnsClient("", SHOP_ID)


class _TruncatedBody:
    """A response whose body ends early, as a dropped connection leaves it."""

    status = 200
    headers = email.message.Message()

    def __enter__(self) -> _TruncatedBody:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def read(self) -> bytes:
        raise http.client.IncompleteRead(b'{"returns": [')


class _BrokenErrorBody:
    """An error body that fails to read after the status line arrived."""

    def read(self) -> bytes:
        raise http.client.IncompleteRead(b"partial")

    def close(self) -> None:
        return None


def _http_error(code: int) -> urllib.error.HTTPError:
    return urllib.error.HTTPError(
        BASE_URL + LIST_PATH,
        code,
        "fixture",
        email.message.Message(),
        _BrokenErrorBody(),  # pyright: ignore[reportArgumentType]
    )


def test_truncated_success_body_is_a_retryable_network_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def truncated(*_args: object, **_kwargs: object) -> _TruncatedBody:
        return _TruncatedBody()

    monkeypatch.setattr(client_module, "_open_url", truncated)

    status_code, _headers, body = urllib_get("https://example.test", {}, 1.0)

    assert (status_code, body) == (0, b"")


def test_connection_failure_while_opening_is_retryable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def refuse(*_args: object, **_kwargs: object) -> object:
        raise http.client.BadStatusLine("garbage")

    monkeypatch.setattr(client_module, "_open_url", refuse)

    status_code, _headers, _body = urllib_get("https://example.test", {}, 1.0)

    assert status_code == 0


def test_error_body_that_cannot_be_read_keeps_the_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail(*_args: object, **_kwargs: object) -> object:
        raise _http_error(503)

    monkeypatch.setattr(client_module, "_open_url", fail)

    status_code, _headers, body = urllib_get("https://example.test", {}, 1.0)

    assert (status_code, body) == (503, b"")


def test_truncated_success_is_retried_through_the_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    good = FakeLoopApi(records=[api_return("1", "2026-07-02T00:00:00+00:00")])
    calls: list[int] = []

    def flaky(request: urllib.request.Request, timeout: float) -> object:
        calls.append(1)
        if len(calls) == 1:
            return _TruncatedBody()
        status_code, _headers, body = good(
            request.full_url, dict(request.header_items()), timeout
        )
        assert status_code == 200
        return _Opened(body)

    monkeypatch.setattr(client_module, "_open_url", flaky)
    sleeps: list[float] = []

    rows = LoopReturnsClient(API_KEY, SHOP_ID, sleep=sleeps.append).fetch_window(
        START, END
    )

    assert [row.return_id for row in rows] == ["1"]
    assert sleeps == [2.0]


class _Opened:
    status = 200
    headers = email.message.Message()

    def __init__(self, body: bytes) -> None:
        self._body = body

    def __enter__(self) -> _Opened:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def read(self) -> bytes:
        return self._body


def test_transport_does_not_forward_credentials_through_redirects() -> None:
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from threading import Thread

    paths: list[str] = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            paths.append(self.path)
            if self.path == "/start":
                self.send_response(302)
                self.send_header("Location", "/unexpected")
            else:
                self.send_response(200)
            self.end_headers()

        def log_message(self, format: str, *args: object) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    worker = Thread(target=server.serve_forever, daemon=True)
    worker.start()
    try:
        code, _, _ = urllib_get(
            f"http://127.0.0.1:{server.server_port}/start",
            {"X-Authorization": API_KEY},
            2.0,
        )
        assert code == 302
        assert paths == ["/start"]
    finally:
        server.shutdown()
        server.server_close()
        worker.join(timeout=5)
