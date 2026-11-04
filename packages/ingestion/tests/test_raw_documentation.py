"""Every raw table and column ingestion writes carries a description.

Descriptions sit beside the declarations they explain. dlt-managed tables and
``_dlt_*`` columns are tool-owned and excluded.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import pytest

from analytics_ingestion.shopify import customer_model
from analytics_ingestion.shopify import model as order_model
from analytics_ingestion.shopify.pipeline import Column
from analytics_ingestion.shopify.source import REJECT_COLUMNS, REJECTS_TABLE

ORDER_TABLES: dict[str, Sequence[Column]] = {
    order_model.ROOT_TABLE: order_model.ORDER_COLUMNS,
    **{
        "__".join((order_model.ROOT_TABLE, *path)): columns
        for path, columns in order_model.NESTED_TABLES.items()
    },
    order_model.SOURCE_STATUS_TABLE: order_model.SOURCE_STATUS_COLUMNS,
    order_model.BATCH_EVIDENCE_TABLE: order_model.BATCH_EVIDENCE_COLUMNS,
    REJECTS_TABLE: REJECT_COLUMNS,
}
CUSTOMER_TABLES: dict[str, Sequence[Column]] = {
    customer_model.ROOT_TABLE: customer_model.CUSTOMER_COLUMNS,
    **{
        "__".join((customer_model.ROOT_TABLE, *path)): columns
        for path, columns in customer_model.NESTED_TABLES.items()
    },
    customer_model.REJECTS_TABLE: customer_model.REJECT_COLUMNS,
    customer_model.EVIDENCE_TABLE: customer_model.EVIDENCE_COLUMNS,
    customer_model.ACCESS_TABLE: customer_model.ACCESS_COLUMNS,
    customer_model.INVENTORY_TABLE: customer_model.INVENTORY_COLUMNS,
    customer_model.STATUS_TABLE: customer_model.STATUS_COLUMNS,
}
DATASETS = [
    pytest.param(
        order_model.DATASET_DESCRIPTION,
        order_model.TABLE_DESCRIPTIONS,
        ORDER_TABLES,
        id="orders",
    ),
    pytest.param(
        customer_model.DATASET_DESCRIPTION,
        customer_model.TABLE_DESCRIPTIONS,
        CUSTOMER_TABLES,
        id="customers",
    ),
]


def undocumented(
    dataset: str,
    tables: Mapping[str, str],
    declared: Mapping[str, Sequence[Column]],
) -> list[str]:
    problems = [] if dataset.strip() else ["dataset: add a description"]
    for table in sorted(declared.keys() - tables.keys()):
        problems.append(f"{table}: add a table description")
    for table in sorted(tables.keys() - declared.keys()):
        problems.append(f"{table}: describes an undeclared table")
    for table, description in sorted(tables.items()):
        if not description.strip():
            problems.append(f"{table}: table description is blank")
    for table, columns in sorted(declared.items()):
        problems.extend(
            f"{table}.{column.name}: add a column description"
            for column in columns
            if not column.description.strip()
        )
    return problems


@pytest.mark.parametrize(("dataset", "tables", "declared"), DATASETS)
def test_every_raw_table_and_column_is_described(
    dataset: str,
    tables: Mapping[str, str],
    declared: Mapping[str, Sequence[Column]],
) -> None:
    assert undocumented(dataset, tables, declared) == []


def test_missing_descriptions_are_reported() -> None:
    declared = {
        "orders": [
            Column("order_id", "text", False, description="Order ID."),
            Column("name", "text", description=" "),
        ],
        "new_table": [Column("id", "text", description="Row ID.")],
    }
    assert undocumented(
        "", {"orders": "Orders.", "gone": "x", "blank": ""}, declared
    ) == [
        "dataset: add a description",
        "new_table: add a table description",
        "blank: describes an undeclared table",
        "gone: describes an undeclared table",
        "blank: table description is blank",
        "orders.name: add a column description",
    ]
