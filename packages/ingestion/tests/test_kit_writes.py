"""The package write policy: pending packages, failed jobs and payload files."""

from __future__ import annotations

import gzip
from collections.abc import Sequence
from pathlib import Path

import dlt
import pytest
from dlt.common.exceptions import TerminalValueError
from dlt.common.schema.typing import TTableSchema
from dlt.common.typing import TDataItems
from dlt.pipeline.pipeline import Pipeline

from analytics_ingestion.kit.writes import open_writes

PII = b"ada@example.invalid"


def holders(root: Path, needle: bytes) -> list[Path]:
    """Files under ``root`` that contain ``needle``, compressed or not."""
    found: list[Path] = []
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        data = path.read_bytes()
        if path.suffix == ".gz":
            data = gzip.decompress(data)
        if needle in data:
            found.append(path)
    return found


def duckdb(root: Path) -> Pipeline:
    return dlt.pipeline(
        "writes",
        pipelines_dir=str(root),
        destination=dlt.destinations.duckdb(str(root / "writes.duckdb")),
        dataset_name="raw",
    )


def tables(root: Path) -> list[str]:
    with duckdb(root).sql_client() as client:
        result: Sequence[Sequence[object]] | None = client.execute_sql(
            "select table_name from information_schema.tables"
            " where table_schema = 'raw' and table_name not like '_dlt%'"
            " order by table_name"
        )
    return [str(row[0]) for row in result or []]


def pending(pipeline: Pipeline, name: str = "left_over") -> None:
    _ = pipeline.extract(dlt.resource([{"email": PII.decode()}], name=name))


def test_a_load_finishes_a_pending_package_before_new_data(tmp_path: Path) -> None:
    pipeline = duckdb(tmp_path)
    pending(pipeline)

    writes = open_writes(pipeline)
    _ = writes.run(dlt.resource([{"value": 1}], name="new"))

    assert not pipeline.has_pending_data
    assert tables(tmp_path) == ["left_over", "new"]


def test_a_reconciliation_refuses_a_pending_package(tmp_path: Path) -> None:
    pipeline = duckdb(tmp_path)
    pending(pipeline)

    with pytest.raises(ValueError, match="pending"):
        _ = open_writes(pipeline, pending="refuse")

    assert pipeline.has_pending_data


def test_a_write_refuses_data_dlt_would_skip(tmp_path: Path) -> None:
    """dlt's run loads a pending package and drops the data it was given."""
    pipeline = duckdb(tmp_path)
    writes = open_writes(pipeline)
    pending(pipeline)

    with pytest.raises(ValueError, match="pending"):
        _ = writes.run(dlt.resource([{"value": 1}], name="new"))

    assert pipeline.has_pending_data


def test_a_loaded_package_with_failed_jobs_blocks_only_reconciliation(
    tmp_path: Path,
) -> None:
    @dlt.destination(batch_size=10, loader_file_format="typed-jsonl")
    def sink(items: TDataItems, table: TTableSchema) -> None:
        del items
        if table.get("name") == "bad":
            raise TerminalValueError("rejected")

    pipeline = dlt.pipeline(
        "failed", pipelines_dir=str(tmp_path), destination=sink, dataset_name="d"
    )
    good = dlt.resource([{"email": PII.decode()}], name="good")
    bad = dlt.resource([{"email": PII.decode()}], name="bad")
    # Only unsafe settings, which open_writes rejects, complete such a package.
    _ = pipeline.extract([good, bad])
    _ = pipeline.normalize()
    _ = pipeline.load(raise_on_failed_jobs=False)

    with pytest.raises(ValueError, match="failed jobs"):
        _ = open_writes(pipeline, pending="refuse")
    _ = open_writes(pipeline)

    # The failed package keeps every file for inspection.
    assert holders(Path(pipeline.working_dir), PII)


def test_loaded_packages_keep_no_payload_files(tmp_path: Path) -> None:
    pipeline = duckdb(tmp_path)
    # An earlier run left a loaded package with its job files.
    _ = pipeline.run(dlt.resource([{"email": PII.decode()}], name="earlier"))
    working = Path(pipeline.working_dir)
    assert holders(working, PII)

    writes = open_writes(pipeline)
    assert holders(working, PII) == []
    _ = writes.run(dlt.resource([{"email": PII.decode()}], name="new"))
    assert holders(working, PII) == []

    pending(pipeline)
    assert holders(working, PII)  # a pending package keeps its files
