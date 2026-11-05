"""Impact Radius Actions through dlt's REST API source, against an offline fake."""

import json
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import duckdb
import pytest
import requests
from requests.adapters import HTTPAdapter

from analytics_ingestion.defs.impact_radius import build_definitions
from analytics_ingestion.impact_radius.source import load_actions
from tests.client_files import listing

# Impact rejects fractional seconds, so the window drops them.
NOW = datetime(2026, 11, 2, 12, 0, 0, 123456, tzinfo=UTC)
SID = "IRfixtureSID"


class Impact(requests.Session):
    """Answers ``GET Actions`` from in-memory pages, like Impact's v14 API."""

    def __init__(self, pages: list[list[dict[str, object]]]) -> None:
        super().__init__()
        self.pages = pages
        self.requests: list[requests.PreparedRequest] = []

    def send(  # pyright: ignore[reportIncompatibleMethodOverride]
        self, request: requests.PreparedRequest, **_: object
    ) -> requests.Response:
        self.requests.append(request)
        query = parse_qs(urlsplit(request.url or "").query)
        page = int(query["Page"][0])
        response = requests.Response()
        response.status_code = 200
        response.request = request
        response.url = request.url or ""
        response._content = json.dumps(  # pyright: ignore[reportPrivateUsage]
            {
                "@page": str(page),
                "@numpages": str(len(self.pages)),
                "@total": str(sum(len(rows) for rows in self.pages)),
                "Actions": self.pages[page - 1] if self.pages else [],
            }
        ).encode()
        return response


def action(action_id: str, state: str = "PENDING") -> dict[str, object]:
    return {
        "Id": action_id,
        "CampaignId": "1234",
        "EventDate": "2026-11-01T08:00:00-07:00",
        "State": state,
        "Amount": "120.00",
        "Payout": "12.00",
        "CustomerEmail": "buyer@example.com",
        "IpAddress": "192.0.2.1",
    }


def environment(
    root: Path, settings: dict[str, object] | None = None
) -> dict[str, str]:
    folder = root / "secrets"
    folder.mkdir(exist_ok=True)
    (folder / "impact_radius.json").write_text(
        json.dumps(
            settings
            or {"account_sid": SID, "auth_token": "token", "campaign_id": "1234"}
        )
    )
    return {
        "ANALYTICS_ENVIRONMENT": "local",
        "ANALYTICS_SOURCE_SECRETS_ROOT": str(folder),
        "ANALYTICS_INGESTION_STATE_ROOT": str(root / "state"),
    }


def rows(root: Path, sql: str) -> list[tuple[object, ...]]:
    raw = root / "state/impact_radius/local/impact_radius.duckdb"
    with duckdb.connect(str(raw), read_only=True) as connection:
        return connection.execute(sql).fetchall()


def test_reads_every_page_of_the_recent_window(tmp_path: Path) -> None:
    impact = Impact([[action("1"), action("2")], [action("3")]])

    report = load_actions(environment(tmp_path), session=impact, now=NOW)

    assert report["row_count"] == 3
    assert [parse_qs(urlsplit(r.url or "").query)["Page"] for r in impact.requests] == [
        ["1"],
        ["2"],
    ]
    first = impact.requests[0]
    assert urlsplit(first.url or "").path == f"/Advertisers/{SID}/Actions"
    assert parse_qs(urlsplit(first.url or "").query) | {"Page": []} == {
        "ActionDateStart": ["2026-10-30T12:00:00+00:00"],
        "ActionDateEnd": ["2026-11-02T12:00:00+00:00"],
        "CampaignId": ["1234"],
        "PageSize": ["20000"],
        "Page": [],
    }
    assert first.headers["IR-Version"] == "14"
    assert str(first.headers["Authorization"]).startswith("Basic ")
    assert rows(tmp_path, "select id from raw_impact_radius.actions order by id") == [
        ("1",),
        ("2",),
        ("3",),
    ]


def test_live_requests_retry_rate_limits_with_a_timeout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Without an injected session, dlt's own session retries and times out."""
    statuses = [429, 200]
    timeouts: list[object] = []

    def send(
        _: HTTPAdapter,
        request: requests.PreparedRequest,
        **kwargs: object,
    ) -> requests.Response:
        response = requests.Response()
        if urlsplit(request.url or "").hostname != "api.impact.com":
            response.status_code = 204  # dlt telemetry, not the source.
            return response
        timeouts.append(kwargs.get("timeout"))
        response.status_code = statuses.pop(0)
        response.headers["Retry-After"] = "0"
        response.request = request
        response.url = request.url or ""
        response._content = json.dumps(  # pyright: ignore[reportPrivateUsage]
            {"@numpages": "1", "Actions": [action("1")]}
        ).encode()
        return response

    monkeypatch.setattr(HTTPAdapter, "send", send)

    report = load_actions(environment(tmp_path), now=NOW)

    assert report["row_count"] == 1
    assert len(timeouts) == 2
    assert all(timeout is not None for timeout in timeouts)


def test_rerun_updates_actions_by_id(tmp_path: Path) -> None:
    env = environment(tmp_path)
    _ = load_actions(env, session=Impact([[action("1"), action("2")]]), now=NOW)
    _ = load_actions(env, session=Impact([[action("2", "APPROVED")]]), now=NOW)
    assert rows(
        tmp_path, "select id, state from raw_impact_radius.actions order by id"
    ) == [("1", "PENDING"), ("2", "APPROVED")]


def test_only_approved_fields_are_kept(tmp_path: Path) -> None:
    _ = load_actions(environment(tmp_path), session=Impact([[action("1")]]), now=NOW)
    columns = {
        name
        for (name,) in rows(
            tmp_path,
            "select column_name from information_schema.columns "
            "where table_schema = 'raw_impact_radius' and table_name = 'actions'",
        )
    }
    assert {"id", "campaign_id", "event_date", "state", "amount"} <= columns
    assert not {"customer_email", "ip_address"} & columns


def test_empty_window_loads_nothing(tmp_path: Path) -> None:
    report = load_actions(environment(tmp_path), session=Impact([]), now=NOW)
    assert report["row_count"] == 0


@pytest.mark.parametrize(
    "settings",
    [
        {"auth_token": "token", "campaign_id": "1234"},
        {"account_sid": SID, "campaign_id": "1234"},
        {"account_sid": SID, "auth_token": "token"},
        {"account_sid": SID, "auth_token": "token", "campaign_id": "12a"},
    ],
)
def test_settings_are_checked_before_any_request(
    tmp_path: Path, settings: dict[str, object]
) -> None:
    impact = Impact([[action("1")]])
    with pytest.raises(ValueError, match="impact_radius.json"):
        _ = load_actions(environment(tmp_path, settings), session=impact, now=NOW)
    assert not impact.requests


def test_job_is_manual_and_local_only(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config = listing(monkeypatch, tmp_path, "impact_radius")
    env = environment(tmp_path)
    definitions = build_definitions(config, env)
    job = definitions.resolve_job_def("impact_radius_actions_load")
    assert job.tags["analytics/exclusive-resource"] == "impact-radius"
    assert definitions.schedules is None
    with pytest.raises(ValueError, match="local target"):
        _ = build_definitions(config, env | {"ANALYTICS_ENVIRONMENT": "development"})
    unlisted = listing(monkeypatch, tmp_path, "goodday")
    assert not build_definitions(unlisted, env).jobs
