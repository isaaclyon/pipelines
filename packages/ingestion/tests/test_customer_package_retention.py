"""Customer payload does not stay in dlt package files after a load."""

from __future__ import annotations

import gzip
import os
import tempfile
from pathlib import Path

import dlt
from dlt.common.exceptions import TerminalValueError
from dlt.common.schema.typing import TTableSchema
from dlt.common.typing import TDataItems

from analytics_ingestion.shopify.customer_source import (
    DELETE_COMPLETED_JOBS_ENV,
    build_pipeline,
    delete_completed_jobs,
)
from tests.customer_fakes import FakeCustomerShop, customer, customer_settings
from tests.test_customer_source import run

PII = b"ada1@example.invalid"


def holders(root: Path, needle: bytes) -> list[Path]:
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


def test_loaded_customer_package_keeps_no_payload_files(tmp_path: Path) -> None:
    shop = FakeCustomerShop()
    shop.add(customer(1))
    pipeline = build_pipeline(customer_settings(tmp_path))
    # An earlier run left a loaded package with its job files.
    _ = pipeline.run(dlt.resource([{"email": PII.decode()}], name="left_over"))
    assert holders(Path(pipeline.working_dir), PII)

    _ = run(tmp_path, shop)

    assert holders(Path(pipeline.working_dir), PII) == []
    assert DELETE_COMPLETED_JOBS_ENV not in os.environ


def test_failed_package_and_pending_package_stay_recoverable() -> None:
    @dlt.destination(batch_size=10, loader_file_format="typed-jsonl")
    def sink(items: TDataItems, table: TTableSchema) -> None:
        del items
        if table.get("name") == "bad":
            raise TerminalValueError("rejected")

    directory = tempfile.mkdtemp()
    pipeline = dlt.pipeline(
        "retention", pipelines_dir=directory, destination=sink, dataset_name="d"
    )
    good = dlt.resource([{"email": PII.decode()}], name="good")
    bad = dlt.resource([{"email": PII.decode()}], name="bad")
    with delete_completed_jobs(pipeline):
        _ = pipeline.extract([good, bad])
        _ = pipeline.normalize()
        _ = pipeline.load(raise_on_failed_jobs=False)
        # The failed package keeps every file for inspection and retry.
        assert holders(Path(pipeline.working_dir), PII)
        _ = pipeline.extract(dlt.resource([{"email": "pending"}], name="later"))
    assert pipeline.has_pending_data
