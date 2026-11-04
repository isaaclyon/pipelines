"""Opt-in check: order and line marts match on both adapters.

Ordinary checks run the full harness on two local databases and skip live access.
The live test loads the good raw fixtures into a uniquely
named disposable Snowflake schema, builds the marts there and in DuckDB, then
compares every column and value after normalizing backend representations.
"""

import json
import os
import subprocess
import sys
from collections.abc import Callable, Generator, Sequence
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import cast
from uuid import uuid4

import duckdb
import pytest
import snowflake.connector

from analytics_transformations.column_declarations import check_column_declarations
from analytics_transformations.mart_contracts import (
    contracts,
    mapping,
    read_json,
    sequence,
    text,
)
from tests.shopify_commercial_fixtures import empty_commercial_tables
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
    add_financial_raw,
    write_raw,
)

ORDER_MARTS = ("+path:models/marts/shopify_orders",)
# The eagerly selected currency test also reads raw sources outside model ancestry.
pytestmark = pytest.mark.dbt(*ORDER_MARTS, "+shopify_commercial_currencies_match")

SNOWFLAKE_TYPES = {
    "decimal(38, 9)": "number(38, 9)",
    "timestamptz": "timestamp_tz",
    "decimal(19, 0)": "number(19, 0)",
}

Rows = list[tuple[object, ...]]


def _build(env: dict[str, str], target: Path) -> None:
    _ = subprocess.run(
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
            "--select",
            *ORDER_MARTS,
        ],
        env=env,
        check=True,
        timeout=240,
    )
    manifest = read_json(target / "manifest.json")
    check_column_declarations(manifest)
    # Validate models actually built by this selection. Unselected evidence
    # staging models can exist in the manifest without source fixture tables.
    results = mapping(read_json(target / "run_results.json"))
    built = {
        text(mapping(result).get("unique_id"))
        for result in sequence(results.get("results"))
        if mapping(result).get("status") == "success"
    }
    outputs = [
        {
            "model_id": node_id,
            "sql": "select * from " + text(node.get("relation_name")),
            "columns": mapping(node.get("columns")),
        }
        for node_id, value in mapping(mapping(manifest).get("nodes")).items()
        if node_id in built and (node := mapping(value)).get("resource_type") == "model"
    ]
    _ = subprocess.run(
        [
            "dbt",
            "run-operation",
            "validate_model_outputs",
            "--project-dir",
            str(ROOT / "dbt"),
            "--profiles-dir",
            str(ROOT / "dbt"),
            "--target-path",
            str(target),
            "--args",
            json.dumps({"outputs": outputs}),
            "--quiet",
        ],
        env=env,
        check=True,
        timeout=240,
    )


def _normal(value: object) -> tuple[str, object]:
    """Type name and value: UTC instants, exact decimals, plain dates.

    Python treats ``Decimal(3) == 3`` as equal, so the type name is compared too.
    """
    if isinstance(value, datetime):
        value = value.astimezone(UTC) if value.tzinfo else value
    elif not (isinstance(value, date | str | bool | int | Decimal) or value is None):
        raise TypeError(f"Unexpected mart value type {type(value).__name__}")
    return type(value).__name__, value


def _table(
    query: Callable[[str], Rows],
    schema: str,
    relation: str,
    key: Sequence[str],
    captured_during: tuple[datetime, datetime] | None = None,
) -> tuple[list[str], Rows]:
    rows = query(f"select * from {schema}.{relation} order by {', '.join(key)}")
    columns = query(
        "select lower(column_name) from information_schema.columns "
        f"where lower(table_schema) = '{schema}' "
        f"and lower(table_name) = '{relation}' order by ordinal_position"
    )
    names = [str(row[0]) for row in columns]
    normalized: Rows = []
    for row in rows:
        values = list(row)
        if captured_during is not None:
            index = names.index("evidence_captured_at")
            captured = values[index]
            assert isinstance(captured, datetime) and captured.tzinfo is None
            start, end = captured_during
            assert start.replace(tzinfo=None) <= captured <= end.replace(tzinfo=None)
            # Runtime capture is checked against its own build, not another clock.
            # Preserve its type; every other field still compares exactly.
            values[index] = datetime(1970, 1, 1)
        normalized.append(tuple(_normal(value) for value in values))
    return names, normalized


def _utc_now(query: Callable[[str], Rows]) -> datetime:
    rows = query("select sysdate()")
    assert len(rows) == 1 and len(rows[0]) == 1
    value = rows[0][0]
    assert isinstance(value, datetime) and value.tzinfo is None
    return value


def _compare_marts(
    manifest: object,
    schema: str,
    duck: Callable[[str], Rows],
    remote: Callable[[str], Rows],
    local_window: tuple[datetime, datetime],
    remote_window: tuple[datetime, datetime],
) -> None:
    plan = contracts(manifest)
    assert {mart.relation for mart in plan} == {
        "fct_orders",
        "dim_order_contacts",
        "fct_order_lines",
        "fct_data_availability",
    }
    for mart in plan:
        capture = mart.relation == "fct_data_availability"
        expected = _table(
            duck, schema, mart.relation, mart.key, local_window if capture else None
        )
        actual = _table(
            remote, schema, mart.relation, mart.key, remote_window if capture else None
        )
        assert expected[0] == [column.name for column in mart.columns]
        assert expected[1], mart.relation
        assert actual == expected, mart.relation


def test_normalization_preserves_exact_decimal_values_and_types() -> None:
    left = Decimal("12345678901234567890123456789.123456789")
    right = Decimal("12345678901234567890123456789.123456788")
    assert _normal(left) != _normal(right)
    assert _normal(Decimal("1.00")) == _normal(Decimal("1.0"))
    assert _normal(Decimal("1")) != _normal(1)
    assert _normal(datetime(2026, 1, 1, tzinfo=UTC)) != _normal(datetime(2026, 1, 1))
    with pytest.raises(TypeError, match="Unexpected mart value type float"):
        _normal(1.0)


@pytest.mark.parametrize("inherited_features", ["false", "true"])
def test_parity_local_build_is_scoped_to_order_marts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, inherited_features: str
) -> None:
    """Exercise loading, both builds, comparisons and cleanup using two local files."""
    remote_path = tmp_path / "remote.duckdb"
    original_build = _build
    clock_reads: list[str] = []

    @contextmanager
    def cursor() -> Generator[SimpleNamespace, None, None]:
        with duckdb.connect(str(remote_path)) as connection:

            def execute(statement: str, params: Sequence[object] = ()) -> None:
                if statement == "select sysdate()":
                    clock_reads.append(statement)
                    statement = "select current_timestamp at time zone 'UTC'"
                # Translate only the fixture loader's Snowflake DDL/catalog spelling.
                statement = statement.replace(
                    "create transient schema", "create schema"
                )
                statement = statement.replace("number(", "decimal(")
                statement = statement.replace("timestamp_tz", "timestamptz")
                statement = statement.replace("ANALYTICS_DEV.", "")
                if "where startswith(schema_name, ?)" in statement:
                    statement = statement.replace(
                        "select schema_name", "select upper(schema_name)"
                    ).replace(
                        "startswith(schema_name, ?)",
                        "starts_with(upper(schema_name), ?)",
                    )
                _ = connection.execute(statement, params)

            yield SimpleNamespace(execute=execute, fetchall=connection.fetchall)

    @contextmanager
    def connect(**_kwargs: object) -> Generator[SimpleNamespace, None, None]:
        yield SimpleNamespace(cursor=cursor)

    def build(env: dict[str, str], target: Path) -> None:
        if env["DBT_TARGET"] == "snowflake-development":
            env = env | {"DBT_TARGET": "local", "DUCKDB_PATH": str(remote_path)}
        original_build(env, target)

    for flag in (
        "SHOPIFY_CUSTOMERS_ENABLED",
        "GOODDAY_INVENTORY_ENABLED",
        "LOOP_RETURNS_ENABLED",
    ):
        monkeypatch.setenv(flag, inherited_features)
    monkeypatch.setenv("SNOWFLAKE_DATABASE", "ANALYTICS_DEV")
    monkeypatch.setenv("SNOWFLAKE_WAREHOUSE", "ANALYTICS_WH_DEV")
    for name in ("ACCOUNT", "USER", "ROLE", "PRIVATE_KEY_PATH"):
        monkeypatch.setenv(f"SNOWFLAKE_{name}", "offline-unused")
    monkeypatch.setattr(snowflake.connector, "connect", connect)
    monkeypatch.setattr(sys.modules[__name__], "_build", build)
    test_order_marts_match_on_duckdb_and_snowflake(tmp_path)
    assert clock_reads == ["select sysdate()", "select sysdate()"]
    with duckdb.connect(str(remote_path)) as remote:
        assert (
            remote.execute(
                "select schema_name from information_schema.schemata "
                "where starts_with(schema_name, 'orders_accept_')"
            ).fetchall()
            == []
        )
    with duckdb.connect(str(tmp_path / "warehouse.duckdb"), read_only=True) as local:
        marts = local.execute(
            "select table_name from information_schema.tables "
            "where ends_with(table_schema, '_marts') order by table_name"
        ).fetchall()
        assert marts == [
            ("dim_order_contacts",),
            ("fct_data_availability",),
            ("fct_order_lines",),
            ("fct_orders",),
        ]
        manifest = read_json(tmp_path / "duckdb-target" / "manifest.json")
        schema_row = local.execute(
            "select table_schema from information_schema.tables "
            "where table_name = 'fct_orders'"
        ).fetchone()
        assert schema_row is not None
        schema = text(cast(object, schema_row[0]))
        window = (datetime(2000, 1, 1, tzinfo=UTC), datetime.now(UTC))

        def query(statement: str) -> Rows:
            return local.execute(statement).fetchall()

        for defect in ("value", "missing_row", "duplicate_row", "columns", "capture"):

            def corrupted(statement: str, defect: str = defect) -> Rows:
                rows = query(statement)
                if f"{schema}.fct_orders " in statement:
                    if defect == "value":
                        return [("changed", *rows[0][1:]), *rows[1:]]
                    if defect == "missing_row":
                        return rows[1:]
                    if defect == "duplicate_row":
                        return [*rows, rows[0]]
                if defect == "columns" and "table_name) = 'fct_orders'" in statement:
                    return list(reversed(rows))
                if (
                    defect == "capture"
                    and f"{schema}.fct_data_availability " in statement
                ):
                    # A future capture time must fail even though clocks are not equal.
                    return [
                        (*row[:4], datetime.now() + timedelta(days=1), *row[5:])
                        for row in rows
                    ]
                return rows

            with pytest.raises(AssertionError):
                _compare_marts(manifest, schema, query, corrupted, window, window)

        # Do not accept a missing or unexpected contract even when values match.
        nodes = mapping(mapping(manifest)["nodes"])
        availability = mapping(nodes["model.analytics.fct_data_availability"])
        changed = mapping(manifest) | {
            "nodes": nodes
            | {
                "model.analytics.fct_data_availability": availability
                | {"alias": "unexpected_mart"}
            }
        }
        with pytest.raises(AssertionError):
            _compare_marts(changed, schema, query, query, window, window)


@pytest.mark.skipif(
    os.getenv("ORDERS_LIVE_ACCEPTANCE") != "development",
    reason="explicit development acceptance only",
)
def test_order_marts_match_on_duckdb_and_snowflake(tmp_path: Path) -> None:
    assert os.environ["SNOWFLAKE_DATABASE"]
    assert os.environ["SNOWFLAKE_WAREHOUSE"]
    namespace = "orders_accept_" + uuid4().hex[:10]
    raw_schema = namespace + "_raw"

    duckdb_path = tmp_path / "warehouse.duckdb"
    write_raw(duckdb_path, GOOD_ORDERS, GOOD_LINES, GOOD_TAGS, GOOD_CODES)
    add_financial_raw(duckdb_path)
    base = dict(os.environ) | {
        "ANALYTICS_ENVIRONMENT": "development",
        "DBT_NAMESPACE": namespace,
        "SHOPIFY_CUSTOMERS_ENABLED": "false",
        "GOODDAY_INVENTORY_ENABLED": "false",
        "LOOP_RETURNS_ENABLED": "false",
    }
    local_started = datetime.now(UTC)
    _build(
        base
        | {
            "DBT_TARGET": "local",
            "DUCKDB_PATH": str(duckdb_path),
            "SHOPIFY_ORDERS_DATASET": "raw",
        },
        tmp_path / "duckdb-target",
    )

    local_finished = datetime.now(UTC)
    database = os.environ["SNOWFLAKE_DATABASE"]
    # The connector's dynamic API is confined here; rows enter as objects.
    with snowflake.connector.connect(  # pyright: ignore[reportUnknownMemberType]
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=database,
        private_key_file=os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
        paramstyle="qmark",
        login_timeout=20,
        network_timeout=120,
        session_parameters={"TIMEZONE": "UTC", "STATEMENT_TIMEOUT_IN_SECONDS": 120},
    ) as connection:

        def sql(statement: str, params: Sequence[object] = ()) -> Rows:
            with connection.cursor() as cursor:
                _ = cursor.execute(statement, params)
                return [tuple(row) for row in cursor.fetchall()]  # pyright: ignore[reportAny] # Connector rows are normalized before comparison.

        try:
            _ = sql(f"create transient schema {raw_schema}")
            raw_tables = {
                "orders": ORDER_COLUMNS,
                "orders__lines": LINE_COLUMNS,
                "orders__tags": TAG_COLUMNS,
                "orders__discount_codes": CODE_COLUMNS,
            } | empty_commercial_tables()
            for table, columns in raw_tables.items():
                with duckdb.connect(str(duckdb_path), read_only=True) as raw_db:
                    values = cast(
                        Rows,
                        raw_db.execute(
                            f"select {', '.join(columns)} from raw.{table}"
                        ).fetchall(),
                    )
                source_rows = [dict(zip(columns, row, strict=True)) for row in values]
                types = [SNOWFLAKE_TYPES.get(kind, kind) for kind in columns.values()]
                _ = sql(
                    f"create table {raw_schema}.{table} ("
                    + ", ".join(
                        f"{name} {kind}"
                        for name, kind in zip(columns, types, strict=True)
                    )
                    + ")"
                )
                for row in source_rows:
                    _ = sql(
                        f"insert into {raw_schema}.{table} select "
                        + ", ".join(f"?::{kind}" for kind in types),
                        [row.get(name) for name in columns],
                    )
            remote_started = _utc_now(sql)
            _build(
                base
                | {
                    "DBT_TARGET": "snowflake-development",
                    "SNOWFLAKE_SCHEMA": raw_schema,
                    "SHOPIFY_ORDERS_DATASET": raw_schema,
                },
                tmp_path / "snowflake-target",
            )

            remote_finished = _utc_now(sql)
            with duckdb.connect(str(duckdb_path), read_only=True) as local:
                _compare_marts(
                    read_json(tmp_path / "duckdb-target" / "manifest.json"),
                    namespace + "_marts",
                    lambda statement: local.execute(statement).fetchall(),
                    sql,
                    (local_started, local_finished),
                    (remote_started, remote_finished),
                )
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
