"""Opt-in development check: the published customer marts match DuckDB.

Ordinary checks skip this test. It loads every synthetic raw fixture table into
uniquely named disposable Snowflake schemas, publishes customers and orders as
separate groups, and compares customer mart columns, physical types and values
with the local DuckDB build.
It also checks the unprivileged reader role. The role is provisioned separately.
"""

import os
import re
import subprocess
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import duckdb
import pytest
import snowflake.connector

from analytics_transformations.mart_contracts import (
    analytical,
    contracts,
    identifier,
    read_json,
    restricted,
)
from analytics_transformations.publication import publish_snowflake_marts
from tests.customer_fixtures import CUSTOMER_TABLES, write_customers
from tests.shopify_order_fixtures import (
    CODE_COLUMNS,
    GOOD_CODES,
    GOOD_LINES,
    GOOD_ORDERS,
    GOOD_TAGS,
    LINE_COLUMNS,
    ORDER_COLUMNS,
    ROOT,
    TAG_COLUMNS,
    write_raw,
)


def build(path: Path, target: Path) -> None:
    """Build the customer and order marts from the local synthetic fixtures."""
    result = subprocess.run(
        [
            "dbt",
            "build",
            "--project-dir",
            str(ROOT / "dbt"),
            "--profiles-dir",
            str(ROOT / "dbt"),
            "--target-path",
            str(target),
            "--no-partial-parse",
            "--quiet",
            "--select",
            "+path:models/marts",
        ],
        env=dict(os.environ)
        | {
            "ANALYTICS_ENVIRONMENT": "ci",
            "DBT_TARGET": "local",
            "DBT_NAMESPACE": "ci_customers",
            "DUCKDB_PATH": str(path),
            "SHOPIFY_ORDERS_DATASET": "raw",
            "SHOPIFY_CUSTOMERS_DATASET": "raw_customers",
            "SHOPIFY_CUSTOMERS_ENABLED": "true",
        },
        capture_output=True,
        text=True,
        timeout=240,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


SNOWFLAKE_TYPES = {
    "decimal(38, 9)": "number(38, 9)",
    "timestamptz": "timestamp_tz",
    "bigint": "number(38, 0)",
}
# Portable names for DuckDB and Snowflake information_schema representations.
PHYSICAL_TYPES = {
    "varchar": "text",
    "text": "text",
    "bigint": "number(38,0)",
    "boolean": "boolean",
    "date": "date",
    "timestamp": "timestamp_ntz",
    "timestamp_ntz": "timestamp_ntz",
    "timestamp with time zone": "timestamp_tz",
    "timestamp_tz": "timestamp_tz",
}

Rows = list[tuple[object, ...]]


def _kind(name: str, precision: object, scale: object) -> str:
    """Canonical type from an information_schema row on either backend."""
    name = name.lower()
    if name in ("number", "decimal", "numeric") and precision is not None:
        return f"number({precision},{scale})"
    if name == "bigint" or (name == "number" and scale == 0):
        return "number(38,0)"
    match = re.fullmatch(r"decimal\((\d+),\s*(\d+)\)", name)
    if match:
        return f"number({match[1]},{match[2]})"
    return PHYSICAL_TYPES.get(name, name)


def _expected(data_type: str) -> str:
    match = re.fullmatch(r"decimal\((\d+),\s*(\d+)\)", data_type.lower())
    if match:
        return f"number({match[1]},{match[2]})"
    return PHYSICAL_TYPES[data_type.lower()]


def _normal(value: object) -> tuple[str, object]:
    """Type name and value: UTC instants, exact decimals, plain dates."""
    if isinstance(value, datetime):
        value = value.astimezone(UTC) if value.tzinfo else value
    elif isinstance(value, Decimal):
        value = value.normalize()
    elif not (isinstance(value, date | str | bool | int) or value is None):
        raise TypeError(f"Unexpected mart value type {type(value).__name__}")
    return type(value).__name__, value


def _bind(value: object) -> object:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return value


@pytest.mark.skipif(
    os.getenv("CUSTOMERS_LIVE_ACCEPTANCE") != "development",
    reason="explicit development acceptance only",
)
def test_customer_marts_match_and_reader_access_is_limited(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Roles are provisioned separately; the test owns only its generated schemas."""
    assert os.environ["ANALYTICS_ENVIRONMENT"] == "development"
    assert os.environ["SNOWFLAKE_DATABASE"]
    assert os.environ["SNOWFLAKE_WAREHOUSE"]
    writer_role = os.environ["SNOWFLAKE_ROLE"]
    reader_role = identifier(os.environ["CUSTOMERS_READER_ROLE"].lower())
    assert reader_role != writer_role.lower()
    namespace = "customers_accept_" + uuid4().hex[:10]
    order_schema = namespace + "_raw_orders"
    customer_schema = namespace + "_raw_customers"
    marts = namespace + "_marts"

    duckdb_path = tmp_path / "warehouse.duckdb"
    write_raw(duckdb_path, GOOD_ORDERS, GOOD_LINES, GOOD_TAGS, GOOD_CODES)
    write_customers(duckdb_path)
    build(duckdb_path, tmp_path / "duckdb-target")
    plan = contracts(read_json(tmp_path / "duckdb-target" / "manifest.json"))
    assert len(plan) == 12
    assert len(restricted(plan)) == 7
    assert {"dim_customers", "dim_order_geography", "fct_order_lines"} <= {
        item.relation for item in analytical(plan)
    }
    assert [item.relation for item in analytical(plan)].count("dim_customers") == 1
    # The local build used its own namespace; rebuild under the generated one.
    local_env = dict(os.environ) | {
        "ANALYTICS_ENVIRONMENT": "development",
        "DBT_TARGET": "local",
        "DBT_NAMESPACE": namespace,
        "DUCKDB_PATH": str(duckdb_path),
        "SHOPIFY_ORDERS_DATASET": "raw",
        "SHOPIFY_CUSTOMERS_DATASET": "raw_customers",
        "SHOPIFY_CUSTOMERS_ENABLED": "true",
    }
    _ = subprocess.run(
        [
            "dbt",
            "build",
            "--project-dir",
            str(ROOT / "dbt"),
            "--profiles-dir",
            str(ROOT / "dbt"),
            "--target-path",
            str(tmp_path / "local-namespace-target"),
            "--no-partial-parse",
            "--quiet",
            "--select",
            "+path:models/marts",
        ],
        env=local_env,
        check=True,
        timeout=240,
    )

    database = os.environ["SNOWFLAKE_DATABASE"]
    # The connector's dynamic API is confined here; rows enter as objects.
    with (
        snowflake.connector.connect(  # pyright: ignore[reportUnknownMemberType]
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            role=writer_role,
            warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
            database=database,
            private_key_file=os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
            paramstyle="qmark",
            login_timeout=20,
            network_timeout=120,
            session_parameters={
                "TIMEZONE": "UTC",
                "STATEMENT_TIMEOUT_IN_SECONDS": 120,
            },
        ) as connection,
        duckdb.connect(str(duckdb_path), read_only=True) as local,
    ):

        def sql(
            statement: str,
            params: Sequence[object] = (),
            role: str | None = None,
        ) -> Rows:
            with connection.cursor() as cursor:
                _ = cursor.execute(f"use role {role or writer_role}")
                _ = cursor.execute("use secondary roles none")
                _ = cursor.execute(statement, params)
                return [tuple(row) for row in cursor.fetchall()]  # pyright: ignore[reportAny] # Connector rows are normalized before comparison.

        def columns(
            query: Callable[[str], Rows], schema: str, relation: str
        ) -> list[tuple[str, str]]:
            rows = query(
                "select lower(column_name), data_type, numeric_precision, "
                "numeric_scale from information_schema.columns "
                f"where lower(table_schema) = '{schema}' "
                f"and lower(table_name) = '{relation}' order by ordinal_position"
            )
            return [(str(r[0]), _kind(str(r[1]), r[2], r[3])) for r in rows]

        def duck(statement: str) -> Rows:
            return local.execute(statement).fetchall()

        def load(schema: str, tables: Mapping[str, Mapping[str, str]]) -> None:
            _ = sql(f"create transient schema {schema}")
            for table, definition in tables.items():
                types = [SNOWFLAKE_TYPES.get(k, k) for k in definition.values()]
                _ = sql(
                    f"create table {schema}.{table} ("
                    + ", ".join(
                        f"{n} {t}" for n, t in zip(definition, types, strict=True)
                    )
                    + ")"
                )
                source = duck(
                    "select "
                    + ", ".join(f'"{n}"' for n in definition)
                    + f" from {'raw' if schema == order_schema else 'raw_customers'}"
                    f".{table}"
                )
                for row in source:
                    _ = sql(
                        f"insert into {schema}.{table} select "
                        + ", ".join(f"?::{t}" for t in types),
                        [_bind(value) for value in row],
                    )

        def reader_denied(statement: str) -> None:
            with pytest.raises(snowflake.connector.errors.ProgrammingError):
                _ = sql(statement, role=reader_role)

        def generation(role: str | None = None) -> list[tuple[object, ...]]:
            return sql(f"select publication_id from {marts}._publication", role=role)

        def publish(fail_build: bool = False, group: str = "customers") -> str:
            def runner(
                command: Sequence[str], env: Mapping[str, str], timeout: int
            ) -> None:
                if fail_build and command[1] == "build":
                    raise subprocess.CalledProcessError(1, list(command))
                _ = subprocess.run(
                    command,
                    env=env,
                    check=True,
                    timeout=timeout,
                    stdout=subprocess.DEVNULL,
                )

            return publish_snowflake_marts(
                environment="development",
                namespace=namespace if group == "customers" else namespace + "_orders",
                publication_group=group,
                project_dir=ROOT / "dbt",
                profiles_dir=ROOT / "dbt",
                reader_role=reader_role,
                runner=runner,
            ).publication_id

        try:
            load(
                order_schema,
                {
                    "orders": ORDER_COLUMNS,
                    "orders__lines": LINE_COLUMNS,
                    "orders__tags": TAG_COLUMNS,
                    "orders__discount_codes": CODE_COLUMNS,
                },
            )
            load(customer_schema, CUSTOMER_TABLES)
            monkeypatch.setenv("SNOWFLAKE_SCHEMA", order_schema)
            monkeypatch.setenv("SHOPIFY_ORDERS_DATASET", order_schema)
            monkeypatch.setenv("SHOPIFY_CUSTOMERS_DATASET", customer_schema)
            monkeypatch.setenv("SHOPIFY_CUSTOMERS_ENABLED", "true")
            customer_plan = tuple(
                mart for mart in plan if mart.relation.startswith("dim_customer")
            )
            first = publish()
            assert generation() == [(first,)]
            _ = publish(group="orders")
            contacts = f"{namespace}_orders_marts.dim_order_contacts"
            assert sql(f"select * from {contacts}")
            reader_denied(f"select * from {contacts}")

            for mart in customer_plan:
                key = ", ".join(mart.key)
                expected_columns = columns(duck, marts, mart.relation)
                actual_columns = columns(sql, marts, mart.relation)
                names = [column.name for column in mart.columns]
                assert [c[0] for c in expected_columns] == names, mart.relation
                assert actual_columns == expected_columns, mart.relation
                assert actual_columns == [
                    (c.name, _expected(c.data_type)) for c in mart.columns
                ], mart.relation
                query = f"select * from {marts}.{mart.relation} order by {key}"
                expected = [tuple(_normal(v) for v in r) for r in duck(query)]
                actual = [tuple(_normal(v) for v in r) for r in sql(query)]
                assert expected, mart.relation
                assert actual == expected, mart.relation

            reader_rows = sql(
                f"select * from {marts}.dim_customers order by "
                + ", ".join(next(m for m in plan if m.relation == "dim_customers").key),
                role=reader_role,
            )
            assert reader_rows
            for mart in restricted(customer_plan):
                reader_denied(f"select * from {marts}.{mart.relation}")
            for schema, tables in (
                (
                    order_schema,
                    (
                        "orders",
                        "orders__lines",
                        "orders__tags",
                        "orders__discount_codes",
                    ),
                ),
                (customer_schema, tuple(CUSTOMER_TABLES)),
            ):
                for table in tables:
                    reader_denied(f"select * from {schema}.{table}")

            # A failed candidate must leave the live generation unchanged.
            with pytest.raises(subprocess.CalledProcessError):
                _ = publish(fail_build=True)
            assert generation(reader_role) == [(first,)]
        finally:
            # Remove only this run's exact generated schema prefix.
            schemas = sql(
                f"select schema_name from {database}.information_schema.schemata "
                "where startswith(schema_name, ?)",
                [namespace.upper() + "_"],
            )
            for (schema,) in schemas:
                assert isinstance(schema, str)
                assert schema.lower().startswith(namespace + "_")
                _ = sql(f'drop schema "{schema}" cascade')
