"""Offline tests for weekly count reconciliation (ADR 0024)."""

from __future__ import annotations

from pathlib import Path

import pytest

from analytics_ingestion.shopify.errors import ConfigurationError, SourceError
from analytics_ingestion.shopify.reconcile import ReconcileReport, reconcile
from tests.shopify_fakes import FakeShop, order
from tests.test_shopify_source import isolated, rows, run

DELETED = "gid://shopify/Order/2"


def loaded(root: Path, count: int = 3) -> FakeShop:
    shop = FakeShop()
    for number in range(1, count + 1):
        shop.add(order(number, created=f"2026-09-{number:02d}T00:00:00Z"))
    run(root, shop, kind="refresh")
    return shop


def check(root: Path, shop: FakeShop, limit: int = 500) -> ReconcileReport:
    return reconcile(
        isolated(root),
        "reconcile-1",
        shop,
        sleeper=lambda _: None,
        max_candidates=limit,
    )


def statuses(root: Path) -> list[tuple[object, ...]]:
    return rows(
        root, "select order_id, status from {s}.order_source_status order by order_id"
    )


def test_equal_months_skip_the_direct_reads(tmp_path: Path) -> None:
    shop = loaded(tmp_path)
    shop.calls.clear()

    report = check(tmp_path, shop)

    assert (report.months, report.differing_months, report.unexplained) == (2, 0, 0)
    assert "versions" not in shop.calls


def test_a_deleted_order_is_marked_once_and_kept_in_raw(tmp_path: Path) -> None:
    shop = loaded(tmp_path)
    del shop.orders[DELETED]

    first = check(tmp_path, shop)
    shop.calls.clear()
    second = check(tmp_path, shop)

    assert (first.differing_months, first.confirmed, first.unexplained) == (1, 1, 0)
    assert statuses(tmp_path) == [(DELETED, "unavailable")]
    assert rows(tmp_path, "select count(*) from {s}.orders") == [(3,)]
    assert (second.differing_months, "versions" in shop.calls) == (0, False)


def test_a_reappeared_order_is_marked_available(tmp_path: Path) -> None:
    shop = loaded(tmp_path)
    kept = shop.orders.pop(DELETED)
    check(tmp_path, shop)
    shop.orders[DELETED] = kept

    report = check(tmp_path, shop)

    assert (report.reappeared, report.unexplained) == (1, 0)
    assert statuses(tmp_path) == [(DELETED, "available")]


def test_an_order_missing_from_raw_is_an_unexplained_gap(tmp_path: Path) -> None:
    shop = loaded(tmp_path)
    shop.add(order(9, created="2026-09-20T00:00:00Z", updated="2026-09-20T00:00:00Z"))
    shop.calls.clear()

    report = check(tmp_path, shop)

    assert (report.source_only, report.unexplained) == (1, 1)
    assert "enumerate" not in shop.calls


def test_a_month_with_no_raw_orders_costs_no_reads(tmp_path: Path) -> None:
    shop = loaded(tmp_path)
    shop.add(order(9, created="2026-07-02T00:00:00Z", updated="2026-07-02T00:00:00Z"))
    shop.calls.clear()

    report = check(tmp_path, shop)

    assert (report.months, report.source_only) == (4, 1)
    assert not {"enumerate", "versions"} & set(shop.calls)


def test_too_many_candidates_marks_nothing(tmp_path: Path) -> None:
    shop = loaded(tmp_path)
    shop.orders.clear()

    with pytest.raises(SourceError, match="candidates"):
        check(tmp_path, shop, limit=2)

    assert check(tmp_path, shop, limit=3).confirmed == 3


def test_reconcile_needs_a_complete_refresh(tmp_path: Path) -> None:
    shop = FakeShop()
    shop.add(order(1))
    run(tmp_path, shop)

    with pytest.raises(ConfigurationError, match="refresh"):
        check(tmp_path, shop)


def test_reconcile_refuses_a_pending_order_package(tmp_path: Path) -> None:
    """dlt's run would load the leftover package and drop this run's marks."""
    import dlt

    from analytics_ingestion.shopify.source import build_pipeline

    shop = loaded(tmp_path)
    del shop.orders[DELETED]
    pipeline = build_pipeline(isolated(tmp_path))
    pipeline.extract(dlt.resource([{"note": "left over"}], name="left_over"))

    with pytest.raises(ValueError, match="pending"):
        check(tmp_path, shop)

    assert pipeline.has_pending_data
    assert rows(
        tmp_path,
        "select count(*) from information_schema.tables"
        " where table_name = 'order_source_status'",
    ) == [(0,)]
