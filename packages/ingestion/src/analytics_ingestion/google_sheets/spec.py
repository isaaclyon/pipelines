"""Which sheet tabs to capture, parsed from the ``google_sheets.json`` settings file.

Each entry names one tab by its stable numeric ``sheet_id`` (not its title), the
exact source headers to read, and the column name each header loads as. Headers
are matched exactly, so source spelling and trailing spaces are intentional.
"""

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import cast

from analytics_ingestion.kit.runtime import NAME

SOURCE = "google_sheets"
SPREADSHEET = re.compile(r"[A-Za-z0-9_-]+")


@dataclass(frozen=True)
class SheetSpec:
    """One approved tab: where it is, which headers to read, and how to name them."""

    table: str
    spreadsheet_id: str
    sheet_id: int
    # (source header, loaded column name), in settings order.
    fields: tuple[tuple[str, str], ...]
    # Columns whose cells must hold source text, never a number coerced to text.
    text_columns: frozenset[str] = frozenset()


def _fail(index: int, message: str) -> ValueError:
    return ValueError(f"{SOURCE}.json sheets[{index}]: {message}")


def parse_sheet(value: object, index: int) -> SheetSpec:
    if not isinstance(value, dict):
        raise _fail(index, "must be a JSON object")
    entry = cast(dict[str, object], value)
    table = entry.get("table")
    if not isinstance(table, str) or not NAME.fullmatch(table):
        raise _fail(index, "table must be a lowercase name")
    spreadsheet_id = entry.get("spreadsheet_id")
    if not isinstance(spreadsheet_id, str) or not SPREADSHEET.fullmatch(spreadsheet_id):
        raise _fail(index, "spreadsheet_id must be a Google spreadsheet ID")
    sheet_id = entry.get("sheet_id")
    if type(sheet_id) is not int or sheet_id < 0:
        raise _fail(index, "sheet_id must be the tab's non-negative numeric ID")
    columns = entry.get("columns")
    if not isinstance(columns, dict) or not columns:
        raise _fail(index, "columns must map source headers to column names")
    fields: list[tuple[str, str]] = []
    for header, name in cast(dict[object, object], columns).items():
        if not isinstance(header, str) or not header:
            raise _fail(index, "every source header must be nonempty text")
        if not isinstance(name, str) or not NAME.fullmatch(name):
            raise _fail(index, f"column for {header!r} must be a lowercase name")
        fields.append((header, name))
    names = [name for _, name in fields]
    if len(set(names)) != len(names):
        raise _fail(index, "column names must be unique")
    raw_text = entry.get("text_columns", [])
    if not isinstance(raw_text, list):
        raise _fail(index, "text_columns must be a list of column names")
    text_columns = cast(list[object], raw_text)
    if not all(isinstance(name, str) and name in names for name in text_columns):
        raise _fail(index, "text_columns must name configured columns")
    return SheetSpec(
        table,
        spreadsheet_id,
        sheet_id,
        tuple(fields),
        frozenset(cast(list[str], text_columns)),
    )


def parse_sheets(settings: Mapping[str, object]) -> tuple[SheetSpec, ...]:
    raw = settings.get("sheets")
    if not isinstance(raw, list) or not raw:
        raise ValueError(f"{SOURCE}.json must set sheets to a nonempty list")
    sheets = tuple(
        parse_sheet(value, index) for index, value in enumerate(cast(list[object], raw))
    )
    tables = [sheet.table for sheet in sheets]
    if len(set(tables)) != len(tables):
        raise ValueError(f"{SOURCE}.json sheets must use unique table names")
    return sheets
