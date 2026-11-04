"""Every named input writer, including evidence and deletion repair, is locked."""

from pathlib import Path

import dagster as dg
import pytest

from analytics_ingestion.defs import (
    goodday,
    loop_returns,
    shopify_customers,
    shopify_orders,
)
from tests.test_goodday_runtime import local_environment


@pytest.mark.parametrize(
    ("definitions", "job_name", "resource"),
    [
        (shopify_orders.defs, shopify_orders.JOB.name, "shopify-orders"),
        (shopify_orders.defs, shopify_orders.RECONCILE_JOB.name, "shopify-orders"),
        (shopify_customers.defs, shopify_customers.JOB.name, "shopify-customers"),
        (
            shopify_customers.defs,
            shopify_customers.RECONCILE_JOB.name,
            "shopify-customers",
        ),
        (loop_returns.defs, loop_returns.JOB.name, "loop-returns"),
        (None, "goodday_inventory_capture", "goodday-inventory"),
    ],
)
def test_input_writer_coordinates_with_publication(
    tmp_path: Path, definitions: dg.Definitions | None, job_name: str, resource: str
) -> None:
    if definitions is None:
        # Kit sources register their job only once enabled with settings.
        env = local_environment(tmp_path) | {"ANALYTICS_SOURCES": "goodday"}
        definitions = goodday.build_definitions(env)
    job = definitions.resolve_job_def(job_name)
    lock = {
        "shopify-orders": "development-marts",
        "loop-returns": "development-marts",
        "shopify-customers": "development-customers",
        "goodday-inventory": "development-inventory",
    }[resource]
    assert job.tags["analytics/publication-input"] == lock
    assert job.tags["analytics/exclusive-resource"] == resource
