"""Customer payload does not stay in dlt package files after a load."""

from __future__ import annotations

from pathlib import Path

import dlt

from analytics_ingestion.shopify.customer_source import build_pipeline
from tests.customer_fakes import FakeCustomerShop, customer, customer_settings
from tests.test_customer_source import run
from tests.test_kit_writes import holders

PII = b"ada1@example.invalid"


def test_loaded_customer_package_keeps_no_payload_files(tmp_path: Path) -> None:
    shop = FakeCustomerShop()
    shop.add(customer(1))
    pipeline = build_pipeline(customer_settings(tmp_path))
    # An earlier run left a loaded package with its job files.
    _ = pipeline.run(dlt.resource([{"email": PII.decode()}], name="left_over"))
    assert holders(Path(pipeline.working_dir), PII)

    _ = run(tmp_path, shop)

    assert holders(Path(pipeline.working_dir), PII) == []
