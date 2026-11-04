"""The shared Shopify load cycle, driven by an in-memory reader."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from datetime import datetime, timedelta
from pathlib import Path

import dlt
import pytest
from dlt.extract.resource import DltResource

from analytics_ingestion.shopify.errors import SourceError
from analytics_ingestion.shopify.load_cycle import (
    EPOCH,
    Connection,
    CycleReport,
    Page,
    RawTables,
    ReadMode,
    Reject,
    RejectLedger,
    Run,
    Tally,
    Window,
    run_load_cycle,
)
from analytics_ingestion.shopify.pipeline import Column, Row, pipeline
from tests.shopify_fakes import SHOP, at, settings

START = at("2026-10-01T00:00:00Z")
END = at("2026-10-01T01:00:00Z")


def _column(name: str, kind: str = "text") -> Column:
    return Column(name, kind, description=name)  # pyright: ignore[reportArgumentType]


TABLES = RawTables(
    RejectLedger(
        "item_rejects", "items", "item_id", "item_status", "deleted", "checked_at"
    ),
    [
        _column("shop_id"),
        _column("item_id"),
        _column("updated_at", "timestamp"),
        _column("_unused"),
    ],
    {("parts",): [_column("part_id")]},
    [
        _column("shop_id"),
        _column("item_id"),
        _column("updated_at", "timestamp"),
        _column("run_id"),
        _column("error"),
        _column("rejected_at", "timestamp"),
    ],
    query_version="q1",
    extraction_revision=3,
    null_columns=("_unused",),
)


class Shop:
    """Items by ID: (updated_at, whether the version fails parsing)."""

    def __init__(self) -> None:
        self.items: dict[str, tuple[datetime, bool]] = {}
        self.windows: list[Window] = []
        self.id_reads: list[list[str]] = []

    def add(self, number: int, updated: datetime, bad: bool = False) -> None:
        self.items[f"item/{number}"] = (updated, bad)


class FakeReader:
    def __init__(self, shop: Shop) -> None:
        self.shop = shop
        self.kept = 0
        self.pages = 0
        self.rejects: list[Reject] = []

    def read(self, window: Window) -> Iterator[Page]:
        self.shop.windows.append(window)
        yield self._page(
            item_id
            for item_id, (updated, _) in self.shop.items.items()
            if (window.start is None or window.start <= updated)
            and updated < window.end
        )

    def read_ids(self, ids: Sequence[str]) -> Iterator[Page]:
        self.shop.id_reads.append(list(ids))
        yield self._page(item_id for item_id in ids if item_id in self.shop.items)

    def tally(self) -> Tally:
        return Tally(self.kept, self.pages, 0, 0, self.rejects)

    def _page(self, ids: Iterator[str]) -> Page:
        self.pages += 1
        page: list[tuple[Row, datetime]] = []
        for item_id in sorted(ids):
            updated, bad = self.shop.items[item_id]
            if bad:
                self.rejects.append(Reject(item_id, updated, "contract: broken"))
                continue
            self.kept += 1
            row: Row = {
                "shop_id": SHOP,
                "item_id": item_id,
                "updated_at": updated,
                "parts": [{"part_id": f"{item_id}/p"}],
            }
            page.append((row, updated))
        return page


def cycle(
    root: Path,
    shop: Shop,
    mode: ReadMode = "bounded",
    start: datetime = START,
    end: datetime = END,
    run_id: str = "run",
) -> CycleReport:
    report = CycleReport(run_id, f"kind-{mode}")
    connection = Connection(SHOP, [], lambda: pytest.fail("no client is needed"))

    def evidence() -> Sequence[DltResource]:
        return [
            dlt.resource(
                [{"run_id": run_id, "records": report.records}],
                name="evidence",
                write_disposition="append",
            )
        ]

    run_load_cycle(
        pipeline(settings(root), "cycle"),
        connection,
        Run(run_id, f"kind-{mode}", mode, "updated_at", start, end),
        TABLES,
        report,
        reader=lambda: FakeReader(shop),
        evidence=evidence,
    )
    return report


def query(root: Path, sql: str) -> list[tuple[object, ...]]:
    with pipeline(settings(root), "cycle").sql_client() as client:
        rows = client.execute_sql(sql.replace("{s}", client.dataset_name)) or []
    return [tuple(row) for row in rows]


def test_bounded_run_loads_its_window_with_metadata_and_evidence(
    tmp_path: Path,
) -> None:
    shop = Shop()
    shop.add(1, START + timedelta(minutes=5))
    shop.add(2, END)

    report = cycle(tmp_path, shop)

    assert shop.windows == [Window("updated_at", START, END)]
    assert (report.window_start, report.window_end) == (START, END)
    assert (report.records, report.pages, report.rejects) == (1, 1, 0)
    assert report.observed_started_at is not None
    assert query(
        tmp_path,
        "select item_id, _batch_id, _extraction_kind, _query_version,"
        " _extraction_revision, _api_version, _unused from {s}.items",
    ) == [("item/1", "run", "kind-bounded", "q1", 3, "2026-10", None)]
    assert query(tmp_path, "select part_id from {s}.items__parts") == [("item/1/p",)]
    assert query(tmp_path, "select run_id, records from {s}.evidence") == [("run", 1)]


def test_refresh_resumes_from_the_cursor_less_the_lag(tmp_path: Path) -> None:
    shop = Shop()
    shop.add(1, START + timedelta(minutes=30))
    _ = cycle(tmp_path, shop, "refresh", run_id="first")

    later = END + timedelta(hours=1)
    report = cycle(tmp_path, shop, "refresh", end=later, run_id="second")

    resumed = START + timedelta(minutes=28)
    assert shop.windows[-1] == Window("updated_at", resumed, later)
    assert report.window_start == resumed


def test_sweep_reads_all_time_without_a_cursor(tmp_path: Path) -> None:
    shop = Shop()
    shop.add(1, EPOCH + timedelta(days=1))

    report = cycle(tmp_path, shop, "sweep")
    again = cycle(tmp_path, shop, "sweep", run_id="again")

    assert shop.windows == [Window("updated_at", None, END)] * 2
    assert (report.window_start, again.records) == (EPOCH, 1)


def test_refresh_rereads_open_rejects_and_records_each_version_once(
    tmp_path: Path,
) -> None:
    shop = Shop()
    shop.add(1, START + timedelta(minutes=5), bad=True)
    first = cycle(tmp_path, shop, "refresh", run_id="first")
    assert (first.rejects, first.open_rejects) == (1, 1)
    assert first.oldest_open_reject is not None

    # The same bad version is read again but not recorded twice.
    again = cycle(tmp_path, shop, "refresh", run_id="again")
    assert (again.rereads, again.rejects, again.open_rejects) == (1, 0, 1)
    assert shop.id_reads == [["item/1"]]

    shop.add(1, START + timedelta(minutes=5))
    fixed = cycle(tmp_path, shop, "refresh", run_id="fixed")

    assert (fixed.rereads, fixed.open_rejects, fixed.oldest_open_reject) == (1, 0, None)
    assert query(tmp_path, "select item_id from {s}.items") == [("item/1",)]
    assert query(tmp_path, "select count(*) from {s}.item_rejects") == [(1,)]


def test_bounded_run_does_not_reread_rejects(tmp_path: Path) -> None:
    shop = Shop()
    shop.add(1, START + timedelta(minutes=5), bad=True)
    _ = cycle(tmp_path, shop)

    report = cycle(tmp_path, shop, run_id="again")

    assert (shop.id_reads, report.rereads, report.open_rejects) == ([], 0, 1)


def test_nothing_left_to_read_writes_no_evidence(tmp_path: Path) -> None:
    shop = Shop()
    _ = cycle(tmp_path, shop, "refresh", end=END, run_id="first")

    report = cycle(tmp_path, shop, "refresh", end=START, run_id="empty")

    assert len(shop.windows) == 1
    assert report.observed_started_at is None
    assert query(tmp_path, "select run_id from {s}.evidence") == [("first",)]


def test_reader_failure_loads_nothing(tmp_path: Path) -> None:
    class Failing(FakeReader):
        def read(self, window: Window) -> Iterator[Page]:
            yield from super().read(window)
            raise SourceError("source broke")

    shop = Shop()
    shop.add(1, START + timedelta(minutes=5))
    with pytest.raises(SourceError, match="source broke"):
        run_load_cycle(
            pipeline(settings(tmp_path), "cycle"),
            Connection(SHOP, [], lambda: pytest.fail("no client is needed")),
            Run("run", "kind", "bounded", "updated_at", START, END),
            TABLES,
            CycleReport("run", "kind"),
            reader=lambda: Failing(shop),
            evidence=lambda: pytest.fail("evidence must not load"),
        )

    with pipeline(settings(tmp_path), "cycle").sql_client() as client:
        assert not client.has_dataset()


def test_metadata_places_source_keys_before_load_ids() -> None:
    class Report(CycleReport):
        RECORDS_KEY = "items"

        def extra_metadata(self) -> dict[str, str | int | float | None]:
            return {"extra": 1}

    report = Report("run", "kind")
    report.records, report.load_ids = 4, ["a", "b"]

    keys = list(report.metadata())

    assert report.metadata()["items"] == 4
    assert keys[-2:] == ["extra", "load_ids"]
    assert report.metadata()["load_ids"] == "a,b"
