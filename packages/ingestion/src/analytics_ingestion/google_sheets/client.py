"""Read approved cells through Google Sheets GridData, preserving physical rows.

No credentials are discovered here. The runtime supplies explicit read-only
Google credentials; fixtures supply the request callable.
"""

import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from time import monotonic
from typing import cast
from urllib.parse import urlencode

from google.auth.credentials import Credentials, ReadOnlyScoped
from google.auth.exceptions import GoogleAuthError
from google.auth.transport.requests import AuthorizedSession
from requests import RequestException

from analytics_ingestion.google_sheets.spec import SheetSpec

Read = Callable[[dict[str, object]], object]
Clock = Callable[[], datetime]
MAX_ROWS = 20_000
MAX_COLUMNS = 200
READONLY_SCOPE = "https://www.googleapis.com/auth/spreadsheets.readonly"
_METADATA_FIELDS = "sheets(properties(sheetId,title,gridProperties))"
_RESPONSE_FIELDS = (
    "sheets(properties(sheetId,title,gridProperties),"
    "data(startRow,startColumn,rowData(values(formattedValue,effectiveValue))))"
)


def google_reader(credentials: Credentials, spreadsheet_id: str) -> Read:
    """Use explicit read-only credentials; never discover an identity or retry reads."""
    if not isinstance(credentials, ReadOnlyScoped) or set(credentials.scopes or ()) != {
        READONLY_SCOPE
    }:
        raise ValueError("Google Sheets requires only spreadsheets.readonly scope")
    session = AuthorizedSession(
        credentials,
        refresh_status_codes=(),
        max_refresh_attempts=0,
        refresh_timeout=30,
    )

    def read(params: dict[str, object]) -> object:
        started = monotonic()
        try:
            with session.get(
                f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}",
                params=urlencode(params, doseq=True),
                timeout=(10, 30),
                allow_redirects=False,
                stream=True,
            ) as response:
                if response.status_code != 200:
                    raise ValueError(
                        f"Google Sheets read failed: HTTP {response.status_code}"
                    )
                payload = bytearray()
                for chunk in response.iter_content(chunk_size=65_536):
                    if monotonic() - started > 40:
                        raise ValueError(
                            "Google Sheets response exceeded read time budget"
                        )
                    if len(payload) + len(chunk) > 16_000_000:
                        raise ValueError(
                            "Google Sheets response exceeds bounded capture size"
                        )
                    payload.extend(chunk)
                result: object = json.loads(payload)  # pyright: ignore[reportAny] JSON boundary parsed below.
                return result
        except (GoogleAuthError, RequestException, json.JSONDecodeError, UnicodeError):
            raise ValueError("Google Sheets read failed; no complete capture") from None

    return read


def object_value(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError("Google Sheets expected JSON object")
    result: dict[str, object] = {}
    for key, child in cast(dict[object, object], value).items():
        if not isinstance(key, str):
            raise ValueError("Google Sheets expected string field name")
        result[key] = child
    return result


def array(value: object) -> list[object]:
    if not isinstance(value, list):
        raise ValueError("Google Sheets expected JSON array")
    return list(cast(list[object], value))


def integer(value: object) -> int:
    if type(value) is not int:
        raise ValueError("Google Sheets expected integer grid coordinate")
    return value


def cell_text(value: object, *, text: bool = False) -> str | None:
    cell = object_value(value)
    effective = object_value(cell.get("effectiveValue", {}))
    if not effective and "formattedValue" not in cell:
        return None
    if "errorValue" in effective:
        # Formula errors such as #N/A persist in the source sheet. Capture them
        # as blank so one bad lookup cannot block every other row; a blank
        # header still fails the header checks.
        return None
    display = cell.get("formattedValue")
    if not isinstance(display, str):
        raise ValueError("Google Sheets cell display text is missing")
    if text and not isinstance(effective.get("stringValue"), str):
        raise ValueError(
            "Google Sheets text column must hold source text, not a coerced number"
        )
    source_text = effective.get("stringValue")
    return source_text if isinstance(source_text, str) else display


def sheet_properties(value: object, sheet_id: int) -> tuple[str, int, int]:
    properties = object_value(value)
    if integer(properties.get("sheetId")) != sheet_id:
        raise ValueError("Google Sheets sheet identity mismatch")
    title = properties.get("title")
    if not isinstance(title, str) or not title:
        raise ValueError("Google Sheets sheet title is missing")
    grid = object_value(properties.get("gridProperties"))
    rows, columns = integer(grid.get("rowCount")), integer(grid.get("columnCount"))
    if not 1 <= rows <= MAX_ROWS or not 1 <= columns <= MAX_COLUMNS:
        raise ValueError("Google Sheets grid exceeds reviewed capture bounds")
    return title, rows, columns


def sheet_parts(
    response: object, title: str, sheet_id: int
) -> tuple[int, int, list[object]]:
    sheets = array(object_value(response).get("sheets"))
    if len(sheets) != 1:
        raise ValueError("Google Sheets response must contain only the selected tab")
    sheet = object_value(sheets[0])
    returned_title, rows, columns = sheet_properties(sheet.get("properties"), sheet_id)
    if returned_title != title:
        raise ValueError("Google Sheets sheet renamed during read")
    return rows, columns, array(sheet.get("data"))


def column_name(index: int) -> str:
    """Convert the bounded zero-based physical column to A1 notation."""
    name = ""
    index += 1
    while index:
        index, remainder = divmod(index - 1, 26)
        name = chr(65 + remainder) + name
    return name


@dataclass(frozen=True)
class Capture:
    started_at: datetime
    observed_at: datetime
    rows: tuple[dict[str, object], ...]


def read_capture(
    read: Read, spec: SheetSpec, *, clock: Clock = lambda: datetime.now(UTC)
) -> Capture:
    """Read tab metadata, bounded headers, then one exact approved-column batch.

    Source strings and other cells' display text (including dates) are retained.
    Grid bounds and headers are revalidated in the data response. This is observed
    membership, not proof of transactional isolation from concurrent sheet edits.
    """
    started = clock()
    metadata = array(object_value(read({"fields": _METADATA_FIELDS})).get("sheets"))
    selected: list[object] = []
    for raw in metadata:
        sheet = object_value(raw)
        if "data" in sheet:
            raise ValueError("Google Sheets metadata unexpectedly includes cell data")
        properties = object_value(sheet.get("properties"))
        if properties.get("sheetId") == spec.sheet_id:
            selected.append(properties)
    if len(selected) != 1:
        raise ValueError(
            "Google Sheets metadata must identify exactly one approved tab"
        )
    title, row_count, column_count = sheet_properties(selected[0], spec.sheet_id)
    quoted = "'" + title.replace("'", "''") + "'"
    header_rows_count, header_columns_count, blocks = sheet_parts(
        read(
            {
                "fields": _RESPONSE_FIELDS,
                "ranges": [f"{quoted}!A1:{column_name(column_count - 1)}1"],
            }
        ),
        title,
        spec.sheet_id,
    )
    if (header_rows_count, header_columns_count) != (row_count, column_count):
        raise ValueError("Google Sheets grid changed during header read")
    if len(blocks) != 1:
        raise ValueError("Google Sheets header response is incomplete")
    block = object_value(blocks[0])
    if (
        integer(block.get("startRow", 0)) != 0
        or integer(block.get("startColumn", 0)) != 0
    ):
        raise ValueError("Google Sheets header offset mismatch")
    header_rows = array(block.get("rowData"))
    if len(header_rows) != 1:
        raise ValueError("Google Sheets requires one header row")
    headers = [cell_text(c) for c in array(object_value(header_rows[0]).get("values"))]
    if len(headers) > column_count:
        raise ValueError("Google Sheets header exceeds grid width")
    positions: dict[int, tuple[str, str]] = {}
    for header, name in spec.fields:
        if headers.count(header) != 1:
            raise ValueError(f"Google Sheets requires one approved header: {name}")
        positions[headers.index(header)] = (header, name)
    ranges = [
        f"{quoted}!{column_name(col)}1:{column_name(col)}{row_count}"
        for col in positions
    ]
    returned_rows, returned_columns, blocks = sheet_parts(
        read({"fields": _RESPONSE_FIELDS, "ranges": ranges}), title, spec.sheet_id
    )
    observed = clock()
    if (
        started.utcoffset() is None
        or observed.utcoffset() is None
        or observed < started
    ):
        raise ValueError(
            "Google Sheets observation bounds must be ordered aware instants"
        )
    if (returned_rows, returned_columns) != (row_count, column_count) or len(
        blocks
    ) != len(positions):
        raise ValueError("Google Sheets data response is incomplete or grid changed")
    columns: dict[str, list[str | None]] = {}
    seen: set[int] = set()
    for raw in blocks:
        block = object_value(raw)
        col = integer(block.get("startColumn", 0))
        if (
            col not in positions
            or col in seen
            or integer(block.get("startRow", 0)) != 0
        ):
            raise ValueError("Google Sheets response range mismatch")
        seen.add(col)
        header, name = positions[col]
        data = array(block.get("rowData"))
        if not data or len(data) > row_count:
            raise ValueError("Google Sheets range header missing or oversized")
        cells: list[str | None] = []
        for index, raw_row in enumerate(data):
            values = array(object_value(raw_row).get("values", []))
            if len(values) > 1:
                raise ValueError("Google Sheets response includes unrequested columns")
            cells.append(
                cell_text(values[0], text=name in spec.text_columns and index > 0)
                if values
                else None
            )
        if cells[0] != header:
            raise ValueError("Google Sheets selected header changed during read")
        columns[name] = cells[1:]
    records: list[dict[str, object]] = []
    for index in range(max(map(len, columns.values()), default=0)):
        row: dict[str, object] = {
            name: values[index] if index < len(values) else None
            for name, values in columns.items()
        }
        if all(value is None or value == "" for value in row.values()):
            continue
        row["_sheet_row"] = index + 2
        records.append(row)
    return Capture(started.astimezone(UTC), observed.astimezone(UTC), tuple(records))
