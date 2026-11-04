"""Offline tests for the dlt-native orders source (ADR 0024, stage 1)."""

from __future__ import annotations

import dataclasses
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

from analytics_ingestion.shopify.client import Budget, ExecutionLimits, ShopifyClient
from analytics_ingestion.shopify.errors import SourceError
from analytics_ingestion.shopify.jsonvalue import JsonObject, objects
from analytics_ingestion.shopify.queries import PAGE_SIZE
from analytics_ingestion.shopify.settings import Settings
from analytics_ingestion.shopify.source import (
    READER_LIMITS,
    REJECTS_TABLE,
    OrdersRun,
    PageReader,
    RunReport,
    build_pipeline,
    load_orders,
)
from tests.shopify_fakes import (
    SHOP,
    FakeShop,
    at,
    line,
    money,
    order,
    refund,
    settings,
)
from tests.test_shopify_economics import rich_shop, shipping

START = at("2026-10-01T00:00:00Z")
END = at("2026-10-01T01:00:00Z")
SCHEMA = "shopify_orders_dlt_raw"


def isolated(root: Path) -> Settings:
    return dataclasses.replace(settings(root), dataset=SCHEMA.upper())


def run(
    root: Path,
    shop: FakeShop,
    kind: str = "lookback",
    start: datetime = START,
    end: datetime = END,
    workers: int = 1,
) -> RunReport:
    return load_orders(
        isolated(root),
        OrdersRun(f"run-{kind}-{end:%H%M}", kind, start, end, workers),  # pyright: ignore[reportArgumentType]
        transport=shop,
        sleeper=lambda _: None,
    )


def rows(root: Path, sql: str) -> list[tuple[object, ...]]:
    with duckdb.connect(
        str(root / "shopify_orders.duckdb"), read_only=True
    ) as connection:
        return connection.execute(sql.replace("{s}", SCHEMA)).fetchall()


def minute(n: int) -> str:
    return f"2026-10-01T00:{n:02d}:00Z"


def test_loads_the_environment_dataset(tmp_path: Path) -> None:
    shop = FakeShop()
    shop.add(order(1, created="2026-10-01T00:10:00Z"))
    report = load_orders(
        settings(tmp_path),
        OrdersRun("run", "lookback", START, END),
        transport=shop,
        sleeper=lambda _: None,
    )
    assert report.orders == 1


def test_pages_and_follow_ups_load_complete_orders_with_exact_money(
    tmp_path: Path,
) -> None:
    shop = FakeShop()
    for number in range(1, PAGE_SIZE + 6):
        shop.add(order(number, updated=minute(number % 60)), [line(number)])
    big = [line(1000 + n, 1, "0.333333333") for n in range(7)]
    shop.add(order(900, updated=minute(30), refunds=[refund(7)]), big)
    shop.orders["gid://shopify/Order/900"]["shippingLines"] = [
        shipping(n) for n in range(1, 7)
    ]
    shop.refund_children["gid://shopify/Refund/7"] = {
        "refundLineItems": [
            {
                "id": f"gid://shopify/RefundLineItem/{n}",
                "quantity": 1,
                "lineItem": {"id": "gid://shopify/LineItem/1000"},
                "subtotalSet": money("0.10"),
                "totalTaxSet": money("0.00"),
            }
            for n in range(30)
        ]
    }

    report = run(tmp_path, shop)

    assert report.orders == PAGE_SIZE + 6
    assert report.pages == 2
    assert "lines_page" in shop.calls
    assert "order_page:shippingLines" in shop.calls
    assert rows(
        tmp_path,
        "select count(*), sum(original_unit_price_shop_amount) from {s}.orders__lines"
        " where order_id = 'gid://shopify/Order/900'",
    ) == [(7, Decimal("2.333333331"))]
    assert rows(tmp_path, "select count(*) from {s}.orders__refunds__lines") == [(30,)]
    assert rows(
        tmp_path,
        "select shop_id, _extraction_revision, _extraction_kind, _content_digest"
        " from {s}.orders where order_id = 'gid://shopify/Order/900'",
    ) == [(SHOP, 4, "lookback", None)]


def test_end_bound_is_exclusive_and_start_inclusive(tmp_path: Path) -> None:
    shop = FakeShop()
    shop.add(order(1, updated="2026-10-01T00:00:00Z"))
    shop.add(order(2, updated="2026-10-01T01:00:00Z"))
    # Shopify's `<` matches the whole end second; the source drops it.
    shop.page_info = None
    original = shop._matching  # pyright: ignore[reportPrivateUsage]
    shop._matching = lambda search: [  # pyright: ignore[reportPrivateUsage]
        *original(search),
        ("gid://shopify/Order/2", shop.orders["gid://shopify/Order/2"]),
    ]
    report = run(tmp_path, shop)
    assert report.orders == 1
    assert report.outside_window == 1
    assert rows(tmp_path, "select order_id from {s}.orders") == [
        ("gid://shopify/Order/1",)
    ]


def test_incomplete_order_fails_the_run_and_loads_nothing(tmp_path: Path) -> None:
    shop = FakeShop()
    shop.add(order(1, updated=minute(1)))
    bad = order(2, updated=minute(2))
    bad["transactionsCount"] = {"count": 3, "precision": "EXACT"}
    shop.add(bad)
    with pytest.raises(SourceError, match="transactions_unverified"):
        run(tmp_path, shop)
    assert not (tmp_path / "shopify_orders.duckdb").exists() or rows(
        tmp_path,
        "select count(*) from information_schema.tables where table_schema = '{s}'"
        " and table_name = 'orders'",
    ) == [(0,)]


def test_contract_failure_is_rejected_while_the_rest_loads(tmp_path: Path) -> None:
    shop = FakeShop()
    shop.add(order(1, updated=minute(1)))
    bad = order(2, updated=minute(2))
    bad["totalPriceSet"] = money("not-a-number")
    shop.add(bad)
    report = run(tmp_path, shop)
    assert (report.orders, report.rejects) == (1, 1)
    assert rows(
        tmp_path,
        f"select order_id, run_id, error like 'contract:%' from {{s}}.{REJECTS_TABLE}",
    ) == [("gid://shopify/Order/2", "run-lookback-0100", True)]


def test_nonadvancing_cursor_fails(tmp_path: Path) -> None:
    shop = FakeShop()
    shop.add(order(1, updated=minute(1)))
    shop.page_info = {"hasNextPage": True, "endCursor": "0"}
    with pytest.raises(SourceError, match="Nonadvancing"):
        run(tmp_path, shop)


def test_merge_replaces_children_and_keeps_newest_version(tmp_path: Path) -> None:
    shop = FakeShop()
    shop.add(order(1, updated=minute(10), refunds=[refund(7)]), [line(1), line(2)])
    run(tmp_path, shop)
    shop.add(order(1, updated=minute(50), refunds=[]), [line(1)])
    run(tmp_path, shop)
    assert rows(tmp_path, "select line_item_id from {s}.orders__lines") == [
        ("gid://shopify/LineItem/1",)
    ]
    assert rows(tmp_path, "select count(*) from {s}.orders__refunds") == [(0,)]


def test_order_updated_during_paging_keeps_the_newer_observation(
    tmp_path: Path,
) -> None:
    shop = FakeShop()
    for number in range(1, PAGE_SIZE + 2):
        shop.add(order(number, updated=minute(1), created=minute(1)))

    def touch(fake: FakeShop) -> None:
        fake.orders["gid://shopify/Order/1"]["updatedAt"] = minute(59)
        fake.orders["gid://shopify/Order/1"]["name"] = "#1-edited"

    shop.on_orders_page = touch
    report = run(tmp_path, shop, kind="reextract")
    assert report.orders == PAGE_SIZE + 1
    assert rows(
        tmp_path, "select name from {s}.orders where order_id = 'gid://shopify/Order/1'"
    ) == [("#1-edited",)]


def test_refresh_advances_the_cursor_with_lag(tmp_path: Path) -> None:
    shop = FakeShop()
    shop.add(order(1, updated=minute(30)))
    first = run(tmp_path, shop, kind="refresh")
    assert first.window_start == START
    shop.add(order(2, updated="2026-10-01T01:30:00Z"))
    second = run(tmp_path, shop, kind="refresh", end=at("2026-10-01T02:00:00Z"))
    assert second.window_start == at("2026-10-01T00:28:00Z")
    assert rows(tmp_path, "select count(*) from {s}.orders") == [(2,)]


def test_reader_hands_on_each_page_before_reading_the_next() -> None:
    shop = FakeShop()
    for number in range(1, PAGE_SIZE + 2):
        shop.add(order(number, updated=minute(1)))
    client = ShopifyClient(
        "fixture.myshopify.com", "fixture", Budget(ExecutionLimits()), shop
    )
    pages = PageReader(client, SHOP).read("updated_at", START, END)
    assert len(next(pages)) == PAGE_SIZE
    assert shop.calls.count("orders_page") == 1
    assert len(next(pages)) == 1


def test_reader_limits_fit_a_month_at_eight_readers() -> None:
    # 2026-10-15: July (67,020 orders) at eight readers read about 16 KiB of
    # source per order and about 9.5 orders a second per reader.
    per_reader = 67_020 / 8
    assert READER_LIMITS.max_source_bytes >= 2 * per_reader * 16 * 1024
    assert READER_LIMITS.max_active_seconds >= 2 * per_reader / 9.5
    assert READER_LIMITS.max_requests == ExecutionLimits().max_requests


def test_failure_after_streamed_pages_loads_nothing_and_keeps_the_cursor(
    tmp_path: Path,
) -> None:
    shop = FakeShop()
    shop.add(order(1, updated=minute(30)))
    run(tmp_path, shop, kind="refresh")
    later = at("2026-10-01T02:00:00Z")
    for number in range(2, PAGE_SIZE + 3):
        shop.add(order(number, updated=f"2026-10-01T01:00:{number:02d}Z"))
    bad = shop.orders[f"gid://shopify/Order/{PAGE_SIZE + 2}"]
    good = bad["transactionsCount"]
    bad["transactionsCount"] = {"count": 3, "precision": "EXACT"}
    with pytest.raises(SourceError, match="transactions_unverified"):
        run(tmp_path, shop, kind="refresh", end=later)
    assert rows(tmp_path, "select count(*) from {s}.orders") == [(1,)]
    bad["transactionsCount"] = good
    report = run(tmp_path, shop, kind="refresh", end=later)
    assert report.window_start == at("2026-10-01T00:28:00Z")
    assert rows(tmp_path, "select count(*) from {s}.orders") == [(PAGE_SIZE + 2,)]


def test_concurrent_slices_load_the_same_rows(tmp_path: Path) -> None:
    def shop() -> FakeShop:
        fake = FakeShop()
        for number in range(1, 40):
            fake.add(order(number, updated=minute(number)), [line(number)])
        return fake

    one, many = tmp_path / "one", tmp_path / "many"
    run(one, shop(), workers=1)
    report = run(many, shop(), workers=4)
    assert report.orders == 39
    sql = "select order_id, updated_at from {s}.orders order by 1"
    assert rows(one, sql) == rows(many, sql)


def test_report_exposes_request_and_cost_counts(tmp_path: Path) -> None:
    shop = FakeShop()
    shop.add(order(1, updated=minute(1)))
    report = run(tmp_path, shop)
    metadata: JsonObject = dict(report.metadata())
    assert metadata["orders"] == 1
    assert metadata["requests"] == 2  # identity + one page
    assert "extract_seconds" in metadata


EVIDENCE = (
    "select batch_id, kind, window_field, window_start, window_end, status,"
    " completeness, orders_observed, exceptions, extraction_revision,"
    " observed_started_at <= observed_finished_at, load_ids is not null"
    " from {s}.ingestion_batches order by window_end, completed_at"
)


def test_successful_run_appends_one_evidence_row(tmp_path: Path) -> None:
    shop = FakeShop()
    shop.add(order(1, updated=minute(1)))
    bad = order(2, updated=minute(2))
    bad["totalPriceSet"] = money("not-a-number")
    shop.add(bad)
    run(tmp_path, shop)
    assert rows(tmp_path, EVIDENCE) == [
        (
            "run-lookback-0100",
            "lookback",
            "updated_at",
            START,
            END,
            "complete",
            "complete",
            1,
            1,
            4,
            True,
            True,
        )
    ]


def test_failed_run_writes_no_evidence(tmp_path: Path) -> None:
    shop = FakeShop()
    shop.add(order(1, updated=minute(1)))
    run(tmp_path, shop)
    bad = order(2, updated=minute(2))
    bad["transactionsCount"] = {"count": 3, "precision": "EXACT"}
    shop.add(bad)
    with pytest.raises(SourceError):
        run(tmp_path, shop, end=at("2026-10-01T02:00:00Z"))
    assert rows(tmp_path, "select count(*) from {s}.ingestion_batches") == [(1,)]


def test_each_refresh_records_the_window_it_covered(tmp_path: Path) -> None:
    shop = FakeShop()
    shop.add(order(1, updated=minute(30)))
    run(tmp_path, shop, kind="refresh")
    run(tmp_path, shop, kind="refresh", end=at("2026-10-01T02:00:00Z"))
    windows = rows(
        tmp_path,
        "select window_start, window_end from {s}.ingestion_batches order by 2",
    )
    assert windows == [
        (START, END),
        (at("2026-10-01T00:28:00Z"), at("2026-10-01T02:00:00Z")),
    ]


def test_package_left_by_a_crash_is_loaded_before_the_run(tmp_path: Path) -> None:
    shop = FakeShop()
    shop.add(order(1, updated=minute(30)))
    run(tmp_path, shop, kind="refresh")
    # A run killed during load leaves a normalized package behind.
    pending = build_pipeline(isolated(tmp_path))
    pending.extract([{"probe": 1}], table_name="pending_probe")
    pending.normalize()
    shop.add(order(2, updated="2026-10-01T01:30:00Z"))

    report = run(tmp_path, shop, kind="refresh", end=at("2026-10-01T02:00:00Z"))

    assert report.orders == 2
    assert rows(tmp_path, "select count(*) from {s}.pending_probe") == [(1,)]
    assert rows(tmp_path, "select max(window_end) from {s}.ingestion_batches") == [
        (at("2026-10-01T02:00:00Z"),)
    ]


def test_refresh_cursor_is_kept_per_dataset(tmp_path: Path) -> None:
    shop = FakeShop()
    shop.add(order(1, updated=minute(30)))
    run(tmp_path, shop, kind="refresh")
    other = dataclasses.replace(isolated(tmp_path), dataset="OTHER_RAW")
    report = load_orders(
        other,
        OrdersRun("other", "refresh", START, END),
        transport=shop,
        sleeper=lambda _: None,
    )
    assert report.window_start == START


def bad_order(number: int, updated: str) -> JsonObject:
    data = order(number, updated=updated)
    data["totalPriceSet"] = money("not-a-number")
    return data


LATER = at("2026-10-01T00:30:00Z")


def test_refresh_rereads_open_rejects_by_id(tmp_path: Path) -> None:
    shop = FakeShop()
    shop.add(bad_order(2, minute(2)))
    run(tmp_path, shop)
    # The contract is fixed without a new version, so no window sees it again.
    shop.add(order(2, updated=minute(2)))

    report = run(tmp_path, shop, kind="refresh", start=LATER)

    assert (report.orders, report.rereads, report.open_rejects) == (0, 1, 0)
    assert report.oldest_open_reject is None
    assert rows(tmp_path, "select order_id from {s}.orders") == [
        ("gid://shopify/Order/2",)
    ]


def test_reject_that_fails_again_stays_open_without_a_new_row(
    tmp_path: Path,
) -> None:
    shop = FakeShop()
    shop.add(bad_order(2, minute(2)))
    run(tmp_path, shop)

    report = run(tmp_path, shop, kind="refresh", start=LATER)

    assert (report.rereads, report.rejects, report.open_rejects) == (1, 0, 1)
    assert report.oldest_open_reject is not None
    assert rows(tmp_path, f"select count(*) from {{s}}.{REJECTS_TABLE}") == [(1,)]


def test_a_newer_bad_version_is_recorded_again(tmp_path: Path) -> None:
    shop = FakeShop()
    shop.add(bad_order(2, minute(2)))
    run(tmp_path, shop)
    shop.add(bad_order(2, minute(40)))

    report = run(tmp_path, shop, kind="refresh", start=LATER)

    assert (report.rejects, report.open_rejects) == (1, 1)
    assert rows(tmp_path, f"select count(*) from {{s}}.{REJECTS_TABLE}") == [(2,)]


@pytest.mark.parametrize(
    "connection", ["agreements", "shippingLines", "discountApplications"]
)
def test_duplicate_commercial_children_are_rejected(
    tmp_path: Path, connection: str
) -> None:
    shop = rich_shop()
    values = objects(shop.orders["gid://shopify/Order/1"][connection], connection)
    shop.orders["gid://shopify/Order/1"][connection] = [*values, values[0]]
    with pytest.raises(SourceError, match="duplicate"):
        run(tmp_path, shop)


def test_repeated_sales_identity_across_pages_is_rejected(tmp_path: Path) -> None:
    shop = rich_shop()
    source = shop.orders["gid://shopify/Order/1"]
    agreements = objects(source["agreements"], "agreements")
    sales = objects(agreements[0]["sales"], "sales")
    agreements[0]["sales"] = [*sales, sales[0]]
    source["agreements"] = agreements
    with pytest.raises(SourceError, match="duplicate sales"):
        run(tmp_path, shop)


def test_unsafe_loader_configuration_is_refused_before_loading(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Recovery shares the kit guard: a failed job must never count as loaded."""
    monkeypatch.setenv("LOAD__RAISE_ON_FAILED_JOBS", "false")
    shop = FakeShop()
    shop.add(order(1, created="2026-10-01T00:10:00Z"))

    with pytest.raises(ValueError, match="raise_on_failed_jobs"):
        _ = run(tmp_path, shop)
    assert not (tmp_path / "shopify_orders.duckdb").exists()
