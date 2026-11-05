"""Typed empty raw children for fixtures with older order evidence."""

import duckdb

from tests.test_shopify_commercial_staging import commercial_raw_tables


def empty_commercial_tables() -> dict[str, dict[str, str]]:
    tables = {
        name: dict(columns)
        for name, (columns, _) in commercial_raw_tables().items()
        if name not in {"orders", "orders__lines"}
    }
    tables["orders__transactions"] = {
        "shop_id": "varchar",
        "order_id": "varchar",
        "transaction_id": "varchar",
        "parent_transaction_id": "varchar",
        "created_at": "timestamptz",
        "processed_at": "timestamptz",
        "kind": "varchar",
        "status": "varchar",
        "gateway": "varchar",
        "test": "boolean",
        "error_code": "varchar",
        "amount_shop_amount": "decimal(38,9)",
        "amount_shop_currency": "varchar",
        "amount_presentment_amount": "decimal(38,9)",
        "amount_presentment_currency": "varchar",
    }
    tables["orders__refunds__transactions"] = {
        "shop_id": "varchar",
        "order_id": "varchar",
        "refund_id": "varchar",
        "transaction_id": "varchar",
    }
    return tables


def create_empty_commercial_tables(db: duckdb.DuckDBPyConnection) -> None:
    for name, columns in empty_commercial_tables().items():
        _ = db.execute(
            f"create table raw.{name} ("
            + ", ".join(f'"{column}" {kind}' for column, kind in columns.items())
            + ")"
        )
