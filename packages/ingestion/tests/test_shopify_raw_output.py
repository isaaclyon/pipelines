"""What Shopify orders and customer runs write to raw, end to end.

A fixed sequence of runs covers every run type, rejects and their re-reads,
parallel readers and an empty first run. The test compares every raw table and
each run's Dagster metadata with a committed snapshot, so a change to how runs
load is visible as a snapshot diff. Values that differ between executions, such
as read instants and dlt row IDs, are masked. Regenerate after a deliberate
change with:

    UPDATE_RAW_OUTPUT_SNAPSHOT=1 uv run pytest tests/test_shopify_raw_output.py
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable, Mapping
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

from analytics_ingestion.shopify.customer_source import CustomersRun, load_customers
from analytics_ingestion.shopify.jsonvalue import JsonObject, as_object
from analytics_ingestion.shopify.source import OrdersRun, load_orders
from tests.customer_fakes import (
    FakeCustomerShop,
    address,
    contact,
    customer,
    customer_settings,
    metafield,
)
from tests.shopify_fakes import FakeShop, at, line, money, order, settings

SNAPSHOT = Path(__file__).parent / "fixtures" / "shopify_raw_output.json"
VOLATILE_COLUMNS = frozenset(
    {
        "_observed_at",
        "_dlt_load_id",
        "_dlt_id",
        "_dlt_parent_id",
        "_dlt_root_id",
        "rejected_at",
        "completed_at",
        "observed_started_at",
        "observed_finished_at",
        "checked_at",
        "load_ids",
    }
)
VOLATILE_METADATA = frozenset({"extract_seconds", "load_ids"})
MASKED = "<masked>"

Metadata = Mapping[str, str | int | float | None]


def _value(value: object) -> object:
    # DuckDB returns timestamps in the machine's time zone.
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return value


def _fetch(
    connection: duckdb.DuckDBPyConnection, sql: str, *parameters: str
) -> list[tuple[object, ...]]:
    return connection.execute(sql, list(parameters)).fetchall()


def _tables(database: Path, schema: str) -> dict[str, object]:
    """Every raw table in ``schema``: its columns by name and its rows."""
    tables: dict[str, object] = {}
    with duckdb.connect(str(database), read_only=True) as connection:
        names = [
            str(name)
            for (name,) in _fetch(
                connection,
                "select table_name from information_schema.tables"
                " where table_schema = ? and not starts_with(table_name, '_dlt_')"
                " order by table_name",
                schema,
            )
        ]
        for name in names:
            columns = _fetch(
                connection,
                "select column_name, data_type from information_schema.columns"
                " where table_schema = ? and table_name = ? order by column_name",
                schema,
                name,
            )
            order = [str(column[0]) for column in columns]
            selected = ", ".join(f'"{column}"' for column in order)
            rows = [
                {
                    column: (MASKED if value is not None else None)
                    if column in VOLATILE_COLUMNS
                    else _value(value)
                    for column, value in zip(order, row, strict=True)
                }
                for row in _fetch(
                    connection, f'select {selected} from "{schema}"."{name}"'
                )
            ]
            tables[name] = {
                "columns": {str(column[0]): str(column[1]) for column in columns},
                "rows": sorted(rows, key=lambda row: json.dumps(row, sort_keys=True)),
            }
    return tables


def _metadata(metadata: Metadata) -> dict[str, object]:
    return {
        key: MASKED if key in VOLATILE_METADATA else value
        for key, value in sorted(metadata.items())
    }


def _bad_order(number: int, updated: str) -> dict[str, object]:
    data = order(number, updated=updated)
    data["totalPriceSet"] = money("not-a-number")
    return data


def orders_output(root: Path) -> dict[str, object]:
    shop = FakeShop()
    shop.add(order(1, updated="2026-10-01T00:05:00Z"), [line(1), line(2, 3, "4.50")])
    shop.add(_bad_order(2, "2026-10-01T00:10:00Z"))
    shop.add(order(3, updated="2026-10-01T00:40:00Z", total="7.25"), [line(3)])
    runs: list[dict[str, object]] = []

    def load(run_id: str, kind: str, start: str, end: str, workers: int = 1) -> None:
        report = load_orders(
            settings(root),
            OrdersRun(run_id, kind, at(start), at(end), workers),  # pyright: ignore[reportArgumentType]
            transport=shop,
            sleeper=lambda _: None,
        )
        runs.append({"run_id": run_id, "metadata": _metadata(report.metadata())})

    load("lookback-1", "lookback", "2026-10-01T00:00:00Z", "2026-10-01T01:00:00Z", 2)
    load("reextract-1", "reextract", "2026-09-29T00:00:00Z", "2026-10-01T00:00:00Z")
    # The bad order is re-read and fails again at the same version.
    load("refresh-1", "refresh", "2026-10-01T00:30:00Z", "2026-10-01T01:00:00Z")
    shop.add(order(2, updated="2026-10-01T01:10:00Z"))
    shop.add(order(4, updated="2026-10-01T01:15:00Z"), [line(4)])
    load("refresh-2", "refresh", "2026-10-01T00:30:00Z", "2026-10-01T01:30:00Z")
    # The cursor is already past this end, so the run reads nothing.
    load("refresh-3", "refresh", "2026-10-01T00:30:00Z", "2026-10-01T01:00:00Z")
    return {
        "runs": runs,
        "tables": _tables(root / "shopify_orders.duckdb", "shopify_orders_raw"),
    }


def customers_output(root: Path) -> dict[str, object]:
    shop = FakeCustomerShop(page_size=2)
    shop.add(
        customer(
            1,
            updated="2026-10-01T00:05:00Z",
            addresses=[address(11), address(12)],
            metafields=[metafield(1, key="birthday")],
            contacts=[contact(5, [1, 2])],
        )
    )
    bad = customer(2, updated="2026-10-01T00:10:00Z")
    bad["numberOfOrders"] = "many"
    shop.add(bad)
    shop.add(customer(3, updated="2026-10-01T00:40:00Z", addresses=[]))
    runs: list[dict[str, object]] = []

    def load(run_id: str, kind: str, start: str, end: str) -> None:
        report = load_customers(
            customer_settings(root),
            CustomersRun(run_id, kind, at(start), at(end)),  # pyright: ignore[reportArgumentType]
            transport=shop,
            sleeper=lambda _: None,
            collect_tax_id=True,
            approved_metafields=frozenset({("custom", "birthday")}),
        )
        runs.append({"run_id": run_id, "metadata": _metadata(report.metadata())})

    load("reextract-1", "reextract", "2026-10-01T00:00:00Z", "2026-10-01T01:00:00Z")
    # The bad customer is re-read and fails again at the same version.
    load("refresh-1", "refresh", "2026-10-01T00:30:00Z", "2026-10-01T01:00:00Z")
    shop.add(customer(2, updated="2026-10-01T01:10:00Z"))
    shop.add(customer(4, updated="2026-10-01T01:15:00Z"))
    load("lookback-1", "lookback", "2026-10-01T01:00:00Z", "2026-10-01T01:30:00Z")
    load("sweep-1", "sweep", "2026-10-01T00:00:00Z", "2026-10-01T02:00:00Z")
    load("refresh-2", "refresh", "2026-10-01T00:30:00Z", "2026-10-01T02:00:00Z")
    # The cursor is already past this end, so the run reads nothing.
    load("refresh-3", "refresh", "2026-10-01T00:30:00Z", "2026-10-01T01:00:00Z")
    return {
        "runs": runs,
        "tables": _tables(root / "shopify_customers.duckdb", "shopify_customers_raw"),
    }


def empty_orders_output(root: Path) -> dict[str, object]:
    report = load_orders(
        settings(root),
        OrdersRun(
            "empty", "lookback", at("2026-10-01T00:00:00Z"), at("2026-10-01T01:00:00Z")
        ),
        transport=FakeShop(),
        sleeper=lambda _: None,
    )
    return {
        "runs": [{"run_id": "empty", "metadata": _metadata(report.metadata())}],
        "tables": _tables(root / "shopify_orders.duckdb", "shopify_orders_raw"),
    }


def empty_customers_output(root: Path) -> dict[str, object]:
    report = load_customers(
        customer_settings(root),
        CustomersRun(
            "empty", "reextract", at("2026-10-01T00:00:00Z"), at("2026-10-01T01:00:00Z")
        ),
        transport=FakeCustomerShop(),
        sleeper=lambda _: None,
    )
    return {
        "runs": [{"run_id": "empty", "metadata": _metadata(report.metadata())}],
        "tables": _tables(root / "shopify_customers.duckdb", "shopify_customers_raw"),
    }


SCENARIOS: dict[str, Callable[[Path], dict[str, object]]] = {
    "orders": orders_output,
    "customers": customers_output,
    "empty_orders": empty_orders_output,
    "empty_customers": empty_customers_output,
}


def _json(text: str) -> object:
    value: object = json.loads(text)  # pyright: ignore[reportAny]
    return value


def _snapshot() -> JsonObject:
    if not SNAPSHOT.exists():
        return {}
    return as_object(_json(SNAPSHOT.read_text()), "snapshot")


@pytest.mark.parametrize("name", sorted(SCENARIOS))
def test_raw_output_matches_the_snapshot(name: str, tmp_path: Path) -> None:
    actual = _json(json.dumps(SCENARIOS[name](tmp_path)))
    if os.environ.get("UPDATE_RAW_OUTPUT_SNAPSHOT"):
        snapshot = _snapshot()
        snapshot[name] = actual
        _ = SNAPSHOT.write_text(json.dumps(snapshot, indent=1, sort_keys=True) + "\n")
    assert actual == _snapshot().get(name)
