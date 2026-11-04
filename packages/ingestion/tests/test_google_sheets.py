"""Google Sheets reads and retained dlt captures use offline response fixtures only."""

import json
import re
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import dlt
import duckdb
import pytest
from dlt.pipeline.pipeline import Pipeline
from google.auth.exceptions import GoogleAuthError
from google.oauth2.credentials import Credentials
from requests import Request, RequestException, Response, Session

from analytics_ingestion.google_sheets import client
from analytics_ingestion.google_sheets.client import google_reader, read_capture
from analytics_ingestion.google_sheets.source import capture_sheet
from analytics_ingestion.google_sheets.spec import SheetSpec
from analytics_ingestion.kit.capture import recover

NOW = datetime(2026, 11, 1, tzinfo=UTC)
SPREADSHEET_ID = "fixture-spreadsheet"
SHEET_ID = 7
FIELDS = (
    ("SKU", "sku"),
    ("Style Number", "style_number"),
    ("Category", "category"),
    ("Style Name", "style_name"),
    ("Color Name", "color_name"),
    ("Size Name", "size_name"),
    ("Material ", "material"),
    ("Intro Date", "intro_date"),
)
SPEC = SheetSpec("products", SPREADSHEET_ID, SHEET_ID, FIELDS, frozenset({"sku"}))


@pytest.mark.parametrize(
    "problem", ["", "http", "unauthorized", "json", "auth", "network"]
)
def test_transport_is_bounded_and_does_not_leak_errors(
    monkeypatch: pytest.MonkeyPatch, problem: str
) -> None:
    credentials = Credentials(
        token="fixture-token",
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
    )

    calls: list[str] = []
    refreshes: list[bool] = []

    def refresh(request: object) -> None:
        refreshes.append(True)
        credentials.token = "refreshed-fixture-token"

    monkeypatch.setattr(credentials, "refresh", refresh)

    def request(self: Session, method: str, url: str, **kwargs: object) -> Response:
        calls.append(method)
        assert method == "GET"
        assert ":getByDataFilter" not in url
        assert kwargs["timeout"] == (10, 30)
        assert kwargs["allow_redirects"] is False
        assert kwargs.get("data") is None
        assert "json" not in kwargs
        assert kwargs["params"]
        assert kwargs["stream"] is True
        if problem == "network":
            raise RequestException("private credential")
        if problem == "auth":
            raise GoogleAuthError("private credential")
        response = Response()
        response.status_code = {"http": 403, "unauthorized": 401}.get(problem, 200)
        response._content = b"private credential" if problem else b'{"sheets": []}'  # pyright: ignore[reportPrivateUsage] Offline HTTP response fixture.
        response._content_consumed = True  # pyright: ignore[reportPrivateUsage] Offline buffered response.
        return response

    monkeypatch.setattr(Session, "request", request)
    if problem:
        with pytest.raises(ValueError) as exc:
            google_reader(credentials, SPREADSHEET_ID)(
                {"fields": "sheets(properties(sheetId,title,gridProperties))"}
            )
        assert "private credential" not in str(exc.value)
    else:
        assert google_reader(credentials, SPREADSHEET_ID)(
            {"fields": "sheets(properties(sheetId,title,gridProperties))"}
        ) == {"sheets": []}
    assert calls == ["GET"]
    assert refreshes == []


@pytest.mark.parametrize(
    "scopes",
    [
        [],
        ["https://www.googleapis.com/auth/spreadsheets"],
        [
            "https://www.googleapis.com/auth/spreadsheets.readonly",
            "https://www.googleapis.com/auth/drive",
        ],
    ],
)
def test_transport_rejects_missing_or_broader_scopes(scopes: list[str]) -> None:
    with pytest.raises(ValueError, match="only spreadsheets.readonly"):
        google_reader(Credentials(token="fixture-token", scopes=scopes), SPREADSHEET_ID)


@pytest.mark.parametrize("problem", ["size", "elapsed"])
def test_stream_budget_stops_before_consuming_remaining_body(
    monkeypatch: pytest.MonkeyPatch,
    problem: str,
) -> None:
    credentials = Credentials(token="fixture-token", scopes=[client.READONLY_SCOPE])
    consumed: list[int] = []
    closed: list[bool] = []
    response = Response()
    response.status_code = 200

    def chunks(chunk_size: int) -> Iterator[bytes]:
        assert chunk_size == 65_536
        for index in range(4):
            consumed.append(index)
            yield b"x" * 8_000_000
        pytest.fail("transport consumed an unbounded body")

    def request(*args: object, **kwargs: object) -> Response:
        return response

    monkeypatch.setattr(Session, "request", request)
    monkeypatch.setattr(response, "iter_content", chunks)
    monkeypatch.setattr(response, "close", lambda: closed.append(True))
    if problem == "elapsed":
        instants = iter([0.0, 41.0])
        monkeypatch.setattr(client, "monotonic", lambda: next(instants))
    with pytest.raises(ValueError, match="time budget|bounded capture size"):
        google_reader(credentials, SPREADSHEET_ID)(
            {"fields": "sheets(properties(sheetId,title,gridProperties))"}
        )
    assert consumed == ([0] if problem == "elapsed" else [0, 1, 2])
    assert closed == [True]


def test_http_query_escapes_title_and_repeats_only_selected_ranges(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sheet = Sheet(["001a"])
    sheet.title = "Team's ! 東京 & Sheet"
    urls: list[str] = []

    def request(self: Session, method: str, url: str, **kwargs: object) -> Response:
        assert method == "GET"
        params = kwargs["params"]
        assert isinstance(params, str)
        prepared = Request(method, url, params=params).prepare()
        assert prepared.url is not None
        urls.append(prepared.url)
        query = parse_qs(urlsplit(prepared.url).query)
        fields = query.pop("fields")
        parsed: dict[str, object] = {"fields": fields[0], **query}
        response = Response()
        response.status_code = 200
        response._content = json.dumps(sheet(parsed)).encode()  # pyright: ignore[reportPrivateUsage] Offline HTTP response fixture.
        response._content_consumed = True  # pyright: ignore[reportPrivateUsage] Offline buffered response.
        return response

    monkeypatch.setattr(Session, "request", request)
    credentials = Credentials(token="fixture-token", scopes=[client.READONLY_SCOPE])
    capture = read_capture(
        google_reader(credentials, SPREADSHEET_ID), SPEC, clock=lambda: NOW
    )
    assert len(urls) == 3
    assert capture.rows[0]["sku"] == "001a"
    assert "%27Team%27%27s" in urls[1]
    assert "%26" in urls[1]
    assert "%E6%9D%B1%E4%BA%AC" in urls[1]
    ranges = parse_qs(urlsplit(urls[2]).query)["ranges"]
    assert len(ranges) == len(FIELDS)
    assert all("!C1:" not in span for span in ranges)


class Sheet:
    """Google GridData fixtures retain physical offsets, not compacted range rows."""

    def __init__(self, skus: list[str | None]) -> None:
        self.skus = skus
        self.calls: list[dict[str, object]] = []
        self.partial = False
        self.bad_header = False
        self.fail = False
        self.problem = ""
        self.title = "Products"

    def __call__(self, body: dict[str, object]) -> object:
        self.calls.append(body)
        if self.fail:
            raise ValueError("fixture interrupted read")
        headers = [header for header, _ in FIELDS]
        # A restricted column between approved columns must never be requested.
        headers.insert(2, "Vendor Contact Preferred")
        if self.problem == "duplicate_header":
            headers[2] = "SKU"
        elif self.problem == "missing_header":
            headers[0] = "Changed SKU"
        properties: dict[str, object] = {
            "sheetId": SHEET_ID,
            "title": self.title,
            "gridProperties": {"rowCount": 100, "columnCount": len(headers)},
        }
        quoted = "'" + self.title.replace("'", "''") + "'"
        if len(self.calls) % 3 == 1:
            assert body == {
                "fields": "sheets(properties(sheetId,title,gridProperties))"
            }
            selected: dict[str, object] = {"properties": properties}
            if self.problem == "metadata_cells":
                selected["data"] = []
            elif self.problem == "missing_identity":
                properties["sheetId"] = 1
            elif self.problem == "empty_title":
                properties["title"] = ""
            elif self.problem == "oversized_grid":
                properties["gridProperties"] = {
                    "rowCount": 20_001,
                    "columnCount": len(headers),
                }
            tabs = [{"properties": {"sheetId": 99, "title": "Unselected"}}, selected]
            if self.problem == "duplicate_identity":
                tabs.append(selected)
            return {"sheets": tabs}
        if self.problem == "identity_drift":
            properties["sheetId"] = 1
        if (self.problem == "rename_header" and len(self.calls) == 2) or (
            self.problem == "rename_data" and len(self.calls) == 3
        ):
            properties["title"] = "Renamed"
        if self.problem == "grid_drift":
            properties["gridProperties"] = {
                "rowCount": 101,
                "columnCount": len(headers),
            }
        if len(self.calls) % 3 == 2:
            assert body["ranges"] == [
                f"{quoted}!A1:{client.column_name(len(headers) - 1)}1"
            ]
            data: list[dict[str, object]] = [
                {
                    "startRow": 0,
                    "startColumn": 0,
                    "rowData": [
                        {
                            "values": [
                                {
                                    "formattedValue": h,
                                    "effectiveValue": {"stringValue": h},
                                }
                                for h in headers
                            ]
                        }
                    ],
                }
            ]
        else:
            if self.problem == "interrupted":
                raise ValueError("fixture interrupted batch")
            ranges = body["ranges"]
            assert isinstance(ranges, list)
            data = []
            for span in ranges:  # pyright: ignore[reportUnknownVariableType] fixture request boundary
                assert isinstance(span, str)
                assert span.startswith(quoted + "!")
                match = re.fullmatch(r"([A-Z]+)1:([A-Z]+)100", span[len(quoted) + 1 :])
                assert match is not None
                assert match[1] == match[2]
                col = 0
                for letter in match[1]:
                    col = col * 26 + ord(letter) - ord("A") + 1
                col -= 1
                assert col != 2, "restricted column requested"
                h = headers[col]
                rows: list[dict[str, object]] = [
                    {
                        "values": [
                            {"formattedValue": h, "effectiveValue": {"stringValue": h}}
                        ]
                    }
                ]
                for index, sku in enumerate(self.skus):
                    value = (
                        sku
                        if h == "SKU"
                        else ("blue" if index == 2 and h == "Color Name" else None)
                    )
                    if self.problem == "data_error" and value == "blue":
                        rows.append(
                            {
                                "values": [
                                    {
                                        "formattedValue": "#N/A",
                                        "effectiveValue": {
                                            "errorValue": {
                                                "type": "N_A",
                                                "message": "private payload",
                                            }
                                        },
                                    }
                                ]
                            }
                        )
                        continue
                    rows.append(
                        {}
                        if value is None
                        else {
                            "values": [
                                {
                                    "formattedValue": f"display {value}"
                                    if self.problem == "formatted_text"
                                    else value,
                                    "effectiveValue": {"stringValue": value},
                                }
                            ]
                        }
                    )
                # Real API omits trailing empty rows independently in each range.
                while len(rows) > 1 and not rows[-1]:
                    rows.pop()
                data.append({"startRow": 0, "startColumn": col, "rowData": rows})
            if self.partial:
                data.pop()
            if self.bad_header:
                data[0]["rowData"] = [
                    {
                        "values": [
                            {
                                "formattedValue": "Wrong",
                                "effectiveValue": {"stringValue": "Wrong"},
                            }
                        ]
                    }
                ]
        if self.problem and len(self.calls) == 3:
            if self.problem == "duplicate_range":
                data[-1] = data[0]
            elif self.problem == "shifted_range":
                data[0]["startRow"] = 1
            elif self.problem == "missing_header":
                data[0]["rowData"] = []
            elif self.problem == "numeric_sku":
                data[0]["rowData"] = [
                    {
                        "values": [
                            {
                                "formattedValue": "SKU",
                                "effectiveValue": {"stringValue": "SKU"},
                            }
                        ]
                    },
                    {
                        "values": [
                            {
                                "formattedValue": "001",
                                "effectiveValue": {"numberValue": 1},
                            }
                        ]
                    },
                ]
            elif self.problem == "merged_ranges":
                data[0]["rowData"] = [
                    {
                        "values": [
                            {
                                "formattedValue": "SKU",
                                "effectiveValue": {"stringValue": "SKU"},
                            },
                            {"formattedValue": "Style Number"},
                        ]
                    }
                ]
            elif self.problem == "empty_formula":
                data[0]["rowData"] = [
                    {
                        "values": [
                            {
                                "formattedValue": "SKU",
                                "effectiveValue": {"stringValue": "SKU"},
                            }
                        ]
                    },
                    {"values": [{"effectiveValue": {"stringValue": ""}}]},
                ]
            elif self.problem == "source_error":
                data[0]["rowData"] = [
                    {
                        "values": [
                            {
                                "formattedValue": "private payload",
                                "effectiveValue": {
                                    "errorValue": {"message": "private payload"}
                                },
                            }
                        ]
                    }
                ]
        return {
            "sheets": [
                {
                    "properties": properties,
                    "data": data,
                }
            ]
        }


def pipeline(root: Path) -> Pipeline:
    return dlt.pipeline(
        pipeline_name="google_sheets_test",
        pipelines_dir=str(root / "state"),
        destination=dlt.destinations.duckdb(str(root / "raw.duckdb")),
        dataset_name="master",
    )


def rows(root: Path, sql: str) -> list[tuple[object, ...]]:
    with duckdb.connect(str(root / "raw.duckdb"), read_only=True) as conn:
        return conn.execute(sql).fetchall()


@pytest.mark.parametrize("title", ["Products", "SKU", "Team's ! 東京 & Sheet"])
def test_selected_columns_keep_physical_rows_and_original_sku(title: str) -> None:
    sheet = Sheet([" 001a ", None, "B", "C"])
    sheet.title = title
    capture = read_capture(sheet, SPEC, clock=lambda: NOW)
    assert [row["sku"] for row in capture.rows] == [" 001a ", "B", "C"]
    assert [row["_sheet_row"] for row in capture.rows] == [2, 4, 5]
    assert [row["color_name"] for row in capture.rows] == [None, "blue", None]
    assert len(sheet.calls) == 3
    assert capture.observed_at == NOW


def test_formula_errors_in_data_cells_are_captured_as_blank() -> None:
    # Owners leave lookup errors such as #N/A in the sheet; one must not block
    # every other row. Header errors still fail through the header checks.
    sheet = Sheet([" 001a ", None, "B", "C"])
    sheet.problem = "data_error"
    capture = read_capture(sheet, SPEC, clock=lambda: NOW)
    assert [row["sku"] for row in capture.rows] == [" 001a ", "B", "C"]
    assert [row["color_name"] for row in capture.rows] == [None, None, None]


@pytest.mark.parametrize("failure", ["partial", "bad_header", "fail"])
def test_failed_or_partial_read_has_no_complete_capture(
    failure: str, tmp_path: Path
) -> None:
    sheet = Sheet(["A"])
    setattr(sheet, failure, True)
    with pytest.raises(ValueError):
        capture_sheet(pipeline(tmp_path), sheet, SPEC, clock=lambda: NOW)
    assert not (tmp_path / "raw.duckdb").exists()


@pytest.mark.parametrize(
    "problem",
    [
        "interrupted",
        "duplicate_range",
        "shifted_range",
        "missing_header",
        "duplicate_header",
        "numeric_sku",
        "source_error",
        "metadata_cells",
        "missing_identity",
        "duplicate_identity",
        "empty_title",
        "oversized_grid",
        "identity_drift",
        "rename_header",
        "rename_data",
        "grid_drift",
        "merged_ranges",
        "empty_formula",
    ],
)
def test_malformed_batch_cannot_become_empty_success(
    problem: str, tmp_path: Path
) -> None:
    sheet = Sheet(["A"])
    sheet.problem = problem
    with pytest.raises(ValueError) as exc:
        capture_sheet(pipeline(tmp_path), sheet, SPEC, clock=lambda: NOW)
    assert "private payload" not in str(exc.value)
    assert len(sheet.calls) <= 3
    assert not (tmp_path / "raw.duckdb").exists()


def test_raw_ingestion_does_not_apply_business_key_rules() -> None:
    capture = read_capture(Sheet(["A", "A", None]), SPEC, clock=lambda: NOW)
    assert [row["sku"] for row in capture.rows] == ["A", "A", None]
    assert capture.rows[-1]["color_name"] == "blue"


def test_original_text_is_not_replaced_by_cell_display_format() -> None:
    sheet = Sheet(["001a"])
    sheet.problem = "formatted_text"
    capture = read_capture(sheet, SPEC, clock=lambda: NOW)
    assert capture.rows[0]["sku"] == "001a"


def test_observation_is_actual_completed_read_not_started_time() -> None:
    instants = iter([NOW, NOW + timedelta(minutes=3)])
    capture = read_capture(Sheet([]), SPEC, clock=lambda: next(instants))
    assert capture.started_at == NOW
    assert capture.observed_at == NOW + timedelta(minutes=3)


@pytest.mark.parametrize("problem", ["missing_header", "duplicate_header"])
def test_invalid_headers_fail_before_product_data(problem: str) -> None:
    sheet = Sheet(["A"])
    sheet.problem = problem
    with pytest.raises(ValueError):
        read_capture(sheet, SPEC, clock=lambda: NOW)
    assert len(sheet.calls) == 2


def test_naive_clock_cannot_establish_observation() -> None:
    with pytest.raises(ValueError, match="aware instants"):
        read_capture(Sheet([]), SPEC, clock=lambda: NOW.replace(tzinfo=None))


def test_history_and_complete_empty_membership(tmp_path: Path) -> None:
    p = pipeline(tmp_path)
    first = capture_sheet(p, Sheet([" 001a ", "b"]), SPEC, clock=lambda: NOW)
    second = capture_sheet(p, Sheet([]), SPEC, clock=lambda: NOW)
    assert first != second
    assert rows(
        tmp_path, "select sku from master.products_rows order by _sheet_row"
    ) == [(" 001a ",), ("b",)]
    assert rows(tmp_path, "select count(*) from master.products_rows") == [(2,)]
    assert rows(
        tmp_path, "select row_count from master.products_captures order by row_count"
    ) == [(0,), (2,)]
    assert rows(
        tmp_path,
        "select count(*) from master.products_captures c "
        "join master._dlt_loads l on c._dlt_load_id=l.load_id where l.status=0",
    ) == [(2,)]


def test_initial_empty_capture_materializes_schema(tmp_path: Path) -> None:
    p = pipeline(tmp_path)
    capture_sheet(p, Sheet([]), SPEC, clock=lambda: NOW)
    assert rows(tmp_path, "select count(*) from master.products_rows") == [(0,)]
    assert rows(tmp_path, "select row_count from master.products_captures") == [(0,)]
    for name in ("products_rows", "products_captures"):
        table = p.default_schema.tables[name]
        assert table.get("description")
        for key, column in table.get("columns", {}).items():
            if not key.startswith("_dlt_"):
                assert column.get("description"), key


@pytest.mark.parametrize("failed_table", ["products_rows", "products_captures"])
def test_recovery_keeps_observation_without_reread(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failed_table: str
) -> None:
    from dlt.common.destination.exceptions import DestinationTerminalException
    from dlt.destinations.insert_job_client import InsertValuesLoadJob

    p = pipeline(tmp_path)
    capture_sheet(p, Sheet(["old"]), SPEC, clock=lambda: NOW)
    original = InsertValuesLoadJob.run

    def fail(job: InsertValuesLoadJob) -> None:
        if job.load_table_name == failed_table:
            raise DestinationTerminalException("fixture failed destination job")
        original(job)

    sheet = Sheet(["new"])
    with monkeypatch.context() as patch:
        patch.setattr(InsertValuesLoadJob, "run", fail)
        with pytest.raises(
            Exception, match="fixture failed destination job|failed jobs"
        ):
            capture_sheet(p, sheet, SPEC, clock=lambda: NOW + timedelta(hours=1))
    assert p.has_pending_data
    succeeded_table = (
        "products_captures" if failed_table == "products_rows" else "products_rows"
    )
    identity = "capture_id" if succeeded_table == "products_captures" else "_capture_id"
    before = rows(
        tmp_path, f"select {identity} from master.{succeeded_table} order by {identity}"
    )
    assert rows(tmp_path, "select count(*) from master._dlt_loads where status=0") == [
        (1,)
    ]
    recover(pipeline(tmp_path))
    assert len(sheet.calls) == 3
    assert (
        rows(
            tmp_path,
            f"select {identity} from master.{succeeded_table} order by {identity}",
        )
        == before
    )
    assert rows(
        tmp_path,
        "select count(distinct capture_id), count(*) from master.products_captures",
    ) == [(2, 2)]
    assert rows(
        tmp_path,
        "select distinct observed_at from master.products_captures "
        "order by observed_at",
    ) == [(NOW,), (NOW + timedelta(hours=1),)]
    assert rows(tmp_path, "select count(*) from master._dlt_loads where status=0") == [
        (2,)
    ]
    assert rows(
        tmp_path,
        "select r.sku, r._sheet_row from master.products_rows r "
        "join master.products_captures c on r._capture_id=c.capture_id "
        "and r._dlt_load_id=c._dlt_load_id "
        "join master._dlt_loads l on c._dlt_load_id=l.load_id "
        "where l.status=0 order by r.sku",
    ) == [("new", 2), ("old", 2)]
