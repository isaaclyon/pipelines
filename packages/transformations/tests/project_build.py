"""One synthetic build of the real dbt project, shared by a test session.

The build runs the model gates once over every enabled source.
Tests read its artifacts (ADR 0026).
"""

from dataclasses import dataclass
from pathlib import Path

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


@dataclass(frozen=True)
class ProjectBuild:
    database: Path
    # dbt artifacts of the build step.
    target: Path


def write_project_raw(path: Path) -> None:
    write_raw(path, GOOD_ORDERS, GOOD_LINES, GOOD_TAGS, GOOD_CODES)
    add_financial_raw(path)
    write_customers(path)


def build_project(directory: Path) -> ProjectBuild:
    database = directory / "project.duckdb"
    write_project_raw(database)
    with pytest.MonkeyPatch.context() as patch:
        for name, value in ENV.items():
            patch.setenv(name, value)
        target = build_models(database, namespace=NAMESPACE)
    return ProjectBuild(database, target)
