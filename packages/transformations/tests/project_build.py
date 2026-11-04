"""One synthetic build of the real dbt project, shared by a test session.

The build runs the publication gates once over Shopify orders and customers.
Tests read it, or copy it before they change data and run single checks
(ADR 0026). Scenarios share the dataset, so each test filters its own rows.
"""

import os
import shutil
from dataclasses import dataclass
from pathlib import Path

import duckdb
import pytest

from tests.customer_fixtures import write_customers
from tests.model_build import build_models
from tests.shopify_order_fixtures import (
    GOOD_CODES,
    GOOD_LINES,
    GOOD_ORDERS,
    GOOD_TAGS,
    add_financial_raw,
    write_raw,
)

NAMESPACE = "ci_project"
ENV = {
    "ANALYTICS_ENVIRONMENT": "ci",
    "DBT_TARGET": "local",
    "DBT_NAMESPACE": NAMESPACE,
    "SHOPIFY_ORDERS_DATASET": "raw",
    "SHOPIFY_CUSTOMERS_ENABLED": "true",
    "SHOPIFY_CUSTOMERS_DATASET": "raw_customers",
}
# Distinct values on order 1002 show which geography column each output reads.
GEOGRAPHY_PROBES = {
    "shipping_address_zip": "84601-probe-ship-zip",
    "billing_address_zip": "84043-probe-bill-zip",
    "shipping_address_city": "Probe Ship City",
    "billing_address_city": "Probe Bill City",
}
# Order 1004 names a customer whose profile ingestion purged.
REMOVED_CUSTOMER = "customer-removed"


@dataclass(frozen=True)
class ProjectBuild:
    database: Path
    # dbt artifacts of the build step.
    target: Path
    generation: str

    def copy(self, directory: Path) -> Path:
        """A changeable copy; the file name names the database in staging views."""
        path = directory / self.database.name
        _ = shutil.copyfile(self.database, path)
        return path

    def env(self, database: Path) -> dict[str, str]:
        """The build's environment, pointed at a database file."""
        return dict(os.environ) | ENV | {"DUCKDB_PATH": str(database)}


def write_project_raw(path: Path) -> None:
    write_raw(path, GOOD_ORDERS, GOOD_LINES, GOOD_TAGS, GOOD_CODES)
    add_financial_raw(path)
    write_customers(path)
    with duckdb.connect(str(path)) as db:
        _ = db.execute(
            "update raw.orders set "
            + ", ".join(f"{column} = ?" for column in GEOGRAPHY_PROBES)
            + " where order_id = '1002'",
            list(GEOGRAPHY_PROBES.values()),
        )
        _ = db.execute(
            "update raw.orders set customer_id = ? where order_id = '1004'",
            [REMOVED_CUSTOMER],
        )
        # A customer without a default address, and one outside the US.
        _ = db.execute(
            "update raw_customers.customers set default_address_id = null "
            "where shop_id = 'shop-1' and customer_id = 'customer-2'"
        )
        _ = db.execute(
            "update raw_customers.customers__addresses set city = 'Berlin', "
            "postal_code = '10115' where shop_id = 'shop-2'"
        )
        # Shop 2 denies email access; shop 1 has a legitimately empty email.
        _ = db.execute(
            "update raw_customers.customers set email_address = null "
            "where customer_id = 'customer-1'"
        )
        _ = db.execute(
            "update raw_customers.customer_field_access set status = 'access_denied' "
            "where field_group = 'email' and shop_id = 'shop-2'"
        )
        _ = db.execute("checkpoint")


def build_project(directory: Path) -> ProjectBuild:
    database = directory / "project.duckdb"
    write_project_raw(database)
    with pytest.MonkeyPatch.context() as patch:
        for name, value in ENV.items():
            patch.setenv(name, value)
        # Each independent group is built once in the shared test database.
        # Keep orders last so its unit-test results remain in the fixture target.
        _ = build_models(database, namespace=NAMESPACE, publication_group="customers")
        generation = build_models(
            database, namespace=NAMESPACE, publication_group="orders"
        )
    return ProjectBuild(database, directory / "target" / "build", generation)
