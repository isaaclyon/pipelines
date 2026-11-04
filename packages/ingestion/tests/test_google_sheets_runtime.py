"""Live local Google Sheets captures (ADR 0029), with offline fixtures."""

import json
from collections.abc import Callable
from pathlib import Path

import duckdb
import pytest

from analytics_ingestion.defs.google_sheets import build_definitions
from analytics_ingestion.google_sheets.client import Read
from analytics_ingestion.google_sheets.runtime import (
    bounded,
    credentials_from_info,
    load_google_sheets,
)
from tests.test_google_sheets import FIELDS, SHEET_ID, SPREADSHEET_ID, Sheet

SHEET: dict[str, object] = {
    "table": "products",
    "spreadsheet_id": SPREADSHEET_ID,
    "sheet_id": SHEET_ID,
    "columns": dict(FIELDS),
    "text_columns": ["sku"],
}
SETTINGS: dict[str, object] = {
    "service_account": {"type": "service_account"},
    "sheets": [SHEET],
}


def fixture_reader(sheet: Sheet) -> Callable[[str], Read]:
    def reader(spreadsheet_id: str) -> Read:
        assert spreadsheet_id == SPREADSHEET_ID
        return sheet

    return reader


def environment(root: Path) -> dict[str, str]:
    folder = root / "secrets"
    folder.mkdir(exist_ok=True)
    (folder / "google_sheets.json").write_text(json.dumps(SETTINGS))
    return {
        "ANALYTICS_ENVIRONMENT": "local",
        "ANALYTICS_SOURCE_SECRETS_ROOT": str(folder),
        "ANALYTICS_INGESTION_STATE_ROOT": str(root / "state"),
    }


@pytest.mark.parametrize("target", ["", "ci", "development", "production"])
def test_live_capture_is_local_only(tmp_path: Path, target: str) -> None:
    """Remote loads need separate authority; CI uses fixtures only."""
    env = environment(tmp_path) | {"ANALYTICS_ENVIRONMENT": target}
    with pytest.raises(ValueError, match="local"):
        _ = load_google_sheets(env, reader=fixture_reader(Sheet(["A"])))


def test_settings_need_a_service_account(tmp_path: Path) -> None:
    env = environment(tmp_path)
    (tmp_path / "secrets" / "google_sheets.json").write_text(
        json.dumps({"sheets": [SHEET]})
    )
    with pytest.raises(ValueError, match="service_account"):
        _ = load_google_sheets(env, reader=fixture_reader(Sheet(["A"])))


def test_local_capture_writes_the_raw_file_local_dbt_attaches(
    tmp_path: Path,
) -> None:
    sheet = Sheet(["A", None, "B"])

    captures = load_google_sheets(environment(tmp_path), reader=fixture_reader(sheet))
    capture_id = captures["products"]

    raw = tmp_path / "state" / "google_sheets" / "local" / "google_sheets.duckdb"
    with duckdb.connect(str(raw), read_only=True) as connection:
        assert connection.execute(
            "select _capture_id, sku, _sheet_row "
            "from raw_google_sheets.products_rows order by _sheet_row"
        ).fetchall() == [(capture_id, "A", 2), (capture_id, "B", 4)]
        assert connection.execute(
            "select capture_id, row_count from raw_google_sheets.products_captures"
        ).fetchall() == [(capture_id, 2)]


def test_reads_stop_at_the_wall_limit() -> None:
    now = [0.0]
    calls: list[dict[str, object]] = []

    def read(params: dict[str, object]) -> object:
        calls.append(params)
        now[0] += 50
        return {}

    limited = bounded(read, seconds=120, clock=lambda: now[0])
    _ = limited({"first": 1})
    _ = limited({"second": 2})
    with pytest.raises(ValueError, match="120-second"):
        _ = limited({"third": 3})
    assert len(calls) == 3
    with pytest.raises(ValueError, match="120-second"):
        _ = limited({"fourth": 4})
    assert len(calls) == 3


def test_invalid_credential_does_not_leak() -> None:
    with pytest.raises(ValueError, match="credential is invalid") as error:
        _ = credentials_from_info({"private_key": "private fixture"})
    assert "private fixture" not in str(error.value)


def test_disabled_source_registers_no_job(tmp_path: Path) -> None:
    assert not build_definitions(environment(tmp_path)).jobs


def test_activation_requires_a_readable_settings_file(tmp_path: Path) -> None:
    env = environment(tmp_path) | {"ANALYTICS_SOURCES": "google_sheets"}
    definitions = build_definitions(env)
    job = definitions.resolve_job_def("google_sheets_capture")
    assert job.tags["analytics/exclusive-resource"] == "google-sheets"
    assert job.tags["dagster/retry_on_asset_or_op_failure"] == "false"
    assert job.tags["dagster/max_runtime"] == "600"
    assert definitions.schedules is None
    (tmp_path / "secrets" / "google_sheets.json").unlink()
    with pytest.raises(ValueError, match="google_sheets.json"):
        _ = build_definitions(env)


def test_remote_activation_is_refused(tmp_path: Path) -> None:
    env = environment(tmp_path) | {
        "ANALYTICS_SOURCES": "google_sheets",
        "ANALYTICS_ENVIRONMENT": "development",
    }
    with pytest.raises(ValueError, match="local target"):
        _ = build_definitions(env)


def test_each_configured_tab_loads_its_own_tables(tmp_path: Path) -> None:
    env = environment(tmp_path)
    second = SHEET | {"table": "price_list"}
    (tmp_path / "secrets" / "google_sheets.json").write_text(
        json.dumps(SETTINGS | {"sheets": [SHEET, second]})
    )
    sheets = {"products": Sheet(["A"]), "price_list": Sheet(["B", "C"])}
    order = iter(sheets.values())

    captures = load_google_sheets(env, reader=lambda _: next(order))

    assert list(captures) == ["products", "price_list"]
    raw = tmp_path / "state" / "google_sheets" / "local" / "google_sheets.duckdb"
    with duckdb.connect(str(raw), read_only=True) as connection:
        for table, expected in (("products", ["A"]), ("price_list", ["B", "C"])):
            assert [
                row[0]
                for row in connection.execute(
                    f"select sku from raw_google_sheets.{table}_rows "
                    "order by _sheet_row"
                ).fetchall()
            ] == expected


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"sheets": []}, "nonempty list"),
        ({"table": "Products"}, "lowercase name"),
        ({"spreadsheet_id": "../other"}, "spreadsheet ID"),
        ({"sheet_id": -1}, "numeric ID"),
        ({"sheet_id": "7"}, "numeric ID"),
        ({"columns": {}}, "map source headers"),
        ({"columns": {"SKU": "_capture_id"}}, "lowercase name"),
        ({"columns": {"SKU": "sku", "Code": "sku"}}, "unique"),
        ({"text_columns": ["missing"]}, "configured columns"),
    ],
)
def test_invalid_sheet_settings_fail_before_source_access(
    tmp_path: Path, change: dict[str, object], message: str
) -> None:
    env = environment(tmp_path)
    settings = dict(SETTINGS)
    if "sheets" in change:
        settings |= change
    else:
        settings["sheets"] = [SHEET | change]
    (tmp_path / "secrets" / "google_sheets.json").write_text(json.dumps(settings))
    sheet = Sheet(["A"])
    with pytest.raises(ValueError, match=message):
        _ = load_google_sheets(env, reader=fixture_reader(sheet))
    assert sheet.calls == []


def test_duplicate_tables_are_refused(tmp_path: Path) -> None:
    env = environment(tmp_path)
    entry = SHEET
    (tmp_path / "secrets" / "google_sheets.json").write_text(
        json.dumps(SETTINGS | {"sheets": [entry, entry]})
    )
    with pytest.raises(ValueError, match="unique table names"):
        _ = load_google_sheets(env, reader=fixture_reader(Sheet(["A"])))


def test_first_tab_id_zero_is_valid(tmp_path: Path) -> None:
    from analytics_ingestion.google_sheets.spec import parse_sheet

    entry = SHEET | {"sheet_id": 0}
    assert parse_sheet(entry, 0).sheet_id == 0
