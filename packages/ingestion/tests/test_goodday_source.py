"""Offline GoodDay snapshots exercise the real dlt destination and recovery."""

from collections.abc import Iterator
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import dlt
import duckdb
import pytest
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.pipeline.pipeline import Pipeline

from analytics_ingestion.goodday.source import capture_inventory


class Snapshot:
    source_relation = "fixture.merchant.inventory_by_item"
    observed_at = datetime(2026, 10, 26, tzinfo=UTC)
    columns: TTableSchemaColumns = {
        "merchant_id": {
            "data_type": "bigint",
            "description": "Source merchant identity.",
        },
        "item_id": {"data_type": "bigint", "description": "Source item identity."},
        "location_id": {
            "data_type": "bigint",
            "description": "Source location identity.",
        },
        "channel_id": {
            "data_type": "bigint",
            "description": "Source channel identity.",
        },
        "on_hand_total": {
            "data_type": "bigint",
            "description": "Source on-hand units.",
        },
        "avg_cost_per_unit": {
            "data_type": "decimal",
            "precision": 38,
            "scale": 9,
            "description": "Source average cost per unit.",
        },
    }

    def __init__(self, items: list[int], *, fail: bool = False) -> None:
        self.items = items
        self.fail = fail

    def rows(self) -> Iterator[dict[str, object]]:
        for item in self.items:
            yield {
                "merchant_id": 58,
                "item_id": item,
                "location_id": None,
                "channel_id": 2,
                "on_hand_total": item,
                "avg_cost_per_unit": Decimal("12345.123456789"),
            }
            if self.fail:
                raise RuntimeError("fixture interrupted page")


def pipeline(root: Path) -> Pipeline:
    return dlt.pipeline(
        pipeline_name="goodday_test",
        pipelines_dir=str(root / "state"),
        destination=dlt.destinations.duckdb(str(root / "raw.duckdb")),
        dataset_name="goodday",
    )


def rows(root: Path, sql: str) -> list[tuple[object, ...]]:
    with duckdb.connect(str(root / "raw.duckdb"), read_only=True) as conn:
        return conn.execute(sql).fetchall()


def test_retains_captures_and_empty_capture_is_not_old_inventory(
    tmp_path: Path,
) -> None:
    p = pipeline(tmp_path)
    first = capture_inventory(p, Snapshot([1, 2]))
    second = capture_inventory(p, Snapshot([2]))
    empty = capture_inventory(p, Snapshot([]))
    assert len({first.capture_id, second.capture_id, empty.capture_id}) == 3
    assert rows(tmp_path, "select count(*) from goodday.inventory_by_item") == [(3,)]
    assert rows(
        tmp_path,
        "select row_count from goodday.inventory_captures order by captured_at",
    ) == [(2,), (1,), (0,)]
    assert rows(
        tmp_path, "select distinct avg_cost_per_unit from goodday.inventory_by_item"
    ) == [(Decimal("12345.123456789"),)]
    assert rows(
        tmp_path,
        "select count(*) from goodday.inventory_captures c "
        "join goodday._dlt_loads l on c._dlt_load_id = l.load_id where l.status = 0",
    ) == [(3,)]


def test_failed_read_does_not_publish_and_retry_does_not_load_partial_capture(
    tmp_path: Path,
) -> None:
    p = pipeline(tmp_path)
    capture_inventory(p, Snapshot([1]))
    with pytest.raises(Exception, match="fixture interrupted page"):
        capture_inventory(p, Snapshot([2, 3], fail=True))
    capture_inventory(p, Snapshot([4]))
    assert rows(
        tmp_path, "select item_id from goodday.inventory_by_item order by item_id"
    ) == [(1,), (4,)]
    assert rows(tmp_path, "select count(*) from goodday.inventory_captures") == [(2,)]


@pytest.mark.parametrize("failed_table", ["inventory_captures", "inventory_by_item"])
def test_failed_load_is_not_complete_and_recovery_keeps_capture_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failed_table: str
) -> None:
    from dlt.common.destination.exceptions import DestinationTerminalException
    from dlt.destinations.insert_job_client import InsertValuesLoadJob

    from analytics_ingestion.kit.capture import recover

    p = pipeline(tmp_path)
    capture_inventory(p, Snapshot([1]))
    original = InsertValuesLoadJob.run

    def fail_evidence(job: InsertValuesLoadJob) -> None:
        if job.load_table_name == failed_table:
            raise DestinationTerminalException("fixture evidence load failure")
        original(job)

    with monkeypatch.context() as patch:
        patch.setattr(InsertValuesLoadJob, "run", fail_evidence)
        with pytest.raises(
            Exception, match="fixture evidence load failure|failed jobs"
        ):
            capture_inventory(p, Snapshot([2, 3]))
    assert p.has_pending_data
    assert rows(
        tmp_path, "select count(*) from goodday._dlt_loads where status = 0"
    ) == [(1,)]
    recover(pipeline(tmp_path))
    assert rows(
        tmp_path, "select count(*) from goodday._dlt_loads where status = 0"
    ) == [(2,)]
    assert rows(
        tmp_path,
        "select count(distinct _capture_id), count(*) from goodday.inventory_by_item",
    ) == [(2, 3)]
    assert rows(tmp_path, "select count(*) from goodday.inventory_captures") == [(2,)]


def test_first_empty_capture_creates_inventory_table(tmp_path: Path) -> None:
    capture_inventory(pipeline(tmp_path), Snapshot([]))
    assert rows(tmp_path, "select count(*) from goodday.inventory_by_item") == [(0,)]
    assert rows(tmp_path, "select row_count from goodday.inventory_captures") == [(0,)]


def test_unknown_fields_and_nested_data_survive_capture(tmp_path: Path) -> None:
    class Extended(Snapshot):
        columns: TTableSchemaColumns = {
            **Snapshot.columns,
            "vendor_extension": {
                "data_type": "json",
                "description": "Opaque vendor data.",
            },
        }

        def rows(self) -> Iterator[dict[str, object]]:
            for row in super().rows():
                yield {
                    **row,
                    "vendor_extension": {"tags": ["one", "two"], "nullable": None},
                }

    capture_inventory(pipeline(tmp_path), Extended([1]))
    assert rows(
        tmp_path,
        "select vendor_extension ->> '$.tags[1]' from goodday.inventory_by_item",
    ) == [("two",)]


def test_latest_complete_capture_query_does_not_fall_back_after_empty(
    tmp_path: Path,
) -> None:
    p = pipeline(tmp_path)
    capture_inventory(p, Snapshot([1]))
    capture_inventory(p, Snapshot([]))
    assert (
        rows(
            tmp_path,
            """
        with latest_capture as (
            select c.* from goodday.inventory_captures c
            join goodday._dlt_loads l on l.load_id = c._dlt_load_id
            where l.status = 0
            order by c.captured_at desc, c.capture_id desc limit 1
        )
        select i.item_id from goodday.inventory_by_item i join latest_capture c
        on i._capture_id = c.capture_id and i._dlt_load_id = c._dlt_load_id
    """,
        )
        == []
    )


def test_raw_capture_schema_has_descriptions(tmp_path: Path) -> None:
    p = pipeline(tmp_path)
    capture_inventory(p, Snapshot([]))
    for name in ("inventory_by_item", "inventory_captures"):
        table = p.default_schema.tables[name]
        assert table.get("description")
        for name, column in table.get("columns", {}).items():
            if not name.startswith("_dlt_"):
                assert column.get("description"), name


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("LOAD__RAISE_ON_FAILED_JOBS", "false"),
        ("GOODDAY_TEST__LOAD__RAISE_ON_FAILED_JOBS", "false"),
        ("LOAD__AUTO_ABORT_ON_TERMINAL_ERROR", "true"),
    ],
)
def test_unsafe_loader_configuration_fails_before_capture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, key: str, value: str
) -> None:
    monkeypatch.setenv(key, value)
    with pytest.raises(ValueError, match="requires dlt"):
        capture_inventory(pipeline(tmp_path), Snapshot([1]))
    assert not (tmp_path / "raw.duckdb").exists()
