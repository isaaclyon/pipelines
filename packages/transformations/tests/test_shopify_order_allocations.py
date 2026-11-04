"""Shopify line and order allocations: direct precedence, weighted fallbacks, residuals.

The models call ``shopify_allocate_usd``, so each model's SQL is rendered by dbt
(``dbt compile --inline``) with its ``ref()`` calls replaced by DuckDB tables of the
same name. The rendered SQL then runs on DuckDB. Upstream rows follow the grain and
types of the commercial models. Expected values come from the rules in
``docs/specs/20261011-shopify-order-marts.md`` and the macro documentation, and were
worked out by hand.
"""

import json
import os
import re
import subprocess
from collections.abc import Sequence
from decimal import Decimal
from pathlib import Path
from typing import NamedTuple, cast

import duckdb
import pytest
import yaml

ROOT = Path(__file__).parents[1]
MODELS = ROOT / "dbt" / "models" / "intermediate" / "shopify_orders"
SCHEMA_FILE = MODELS / "_shopify_orders__allocations.yml"
LINE_MODEL = "int_shopify_order_lines__allocations"
ORDER_MODEL = "int_shopify_orders__allocations"
# Compiles these models' SQL over fixture tables (ADR 0026).
pytestmark = pytest.mark.dbt(LINE_MODEL, ORDER_MODEL)
REF = re.compile(r"\{\{\s*ref\('([^']+)'\)\s*\}\}")
MONEY = "decimal(38, 9)"
SHOP = "s1"

LINE_TABLE = "int_shopify_order_lines__commercial_components"
ORDER_TABLE = "int_shopify_orders__commercial_components"
QUANTITY_TABLE = "stg_shopify_orders__order_lines"
UPSTREAM: dict[str, dict[str, str]] = {
    LINE_TABLE: {
        "shop_id": "varchar",
        "order_id": "varchar",
        "line_item_id": "varchar",
        "is_gift_card_line": "boolean",
        "line_commercial_status": "varchar",
        "gross_merchandise_shop_amount": MONEY,
        "merchandise_discount_shop_amount": MONEY,
        "merchandise_refund_shop_amount": MONEY,
    },
    ORDER_TABLE: {
        "shop_id": "varchar",
        "order_id": "varchar",
        "merchandise_discount_shop_amount": MONEY,
        "merchandise_refund_shop_amount": MONEY,
        "unassigned_shipping_shop_amount": MONEY,
        "unassigned_shipping_refund_shop_amount": MONEY,
    },
    QUANTITY_TABLE: {
        "shop_id": "varchar",
        "order_id": "varchar",
        "line_item_id": "varchar",
        "order_line_quantity": "bigint",
    },
}


def dec(value: str | None) -> Decimal | None:
    return None if value is None else Decimal(value)


def order(
    order_id: str = "o1",
    *,
    discount: str | None = "0",
    refund: str | None = "0",
    shipping: str | None = "0",
    shipping_refund: str | None = "0",
) -> dict[str, object]:
    return {
        "shop_id": SHOP,
        "order_id": order_id,
        "merchandise_discount_shop_amount": dec(discount),
        "merchandise_refund_shop_amount": dec(refund),
        "unassigned_shipping_shop_amount": dec(shipping),
        "unassigned_shipping_refund_shop_amount": dec(shipping_refund),
    }


def line(
    order_id: str,
    line_item_id: str,
    *,
    gross: str | None = "0",
    discount: str | None = "0",
    refund: str | None = "0",
    quantity: int | None = 1,
    gift: bool | None = False,
) -> dict[str, object]:
    return {
        "shop_id": SHOP,
        "order_id": order_id,
        "line_item_id": line_item_id,
        "is_gift_card_line": gift,
        "gross_merchandise_shop_amount": dec(gross),
        "merchandise_discount_shop_amount": dec(discount),
        "merchandise_refund_shop_amount": dec(refund),
        "order_line_quantity": quantity,
    }


class Allocation(NamedTuple):
    lines: dict[tuple[str, str], dict[str, object]]
    orders: dict[str, dict[str, object]]


def render(model: str, target: Path) -> str:
    """Render the model with dbt, leaving the macro calls to dbt."""
    body = REF.sub(lambda match: match.group(1), (MODELS / f"{model}.sql").read_text())
    completed = subprocess.run(
        [
            "dbt",
            "compile",
            "--inline",
            body,
            "--output",
            "json",
            "--quiet",
            "--project-dir",
            str(ROOT / "dbt"),
            "--profiles-dir",
            str(ROOT / "dbt"),
            "--target-path",
            str(target),
            "--log-path",
            str(target / "logs"),
        ],
        env=os.environ
        | {
            "DBT_TARGET": "local",
            "DBT_NAMESPACE": "allocations",
            "DUCKDB_PATH": str(target / "unused.duckdb"),
        },
        timeout=180,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    return cast(str, json.loads(completed.stdout)["compiled"])


@pytest.fixture(scope="module")
def compiled(tmp_path_factory: pytest.TempPathFactory) -> dict[str, str]:
    target = tmp_path_factory.mktemp("allocations-target")
    return {model: render(model, target) for model in (LINE_MODEL, ORDER_MODEL)}


def _load(
    con: duckdb.DuckDBPyConnection,
    lines: Sequence[dict[str, object]],
    orders: Sequence[dict[str, object]],
) -> None:
    for table, columns in UPSTREAM.items():
        declared = ", ".join(f"{name} {kind}" for name, kind in columns.items())
        _ = con.execute(f"create table {table} ({declared})")
    for row in lines:
        _insert(con, LINE_TABLE, row)
        _insert(con, QUANTITY_TABLE, row)
    for row in orders:
        _insert(con, ORDER_TABLE, row)


def _insert(con: duckdb.DuckDBPyConnection, table: str, row: dict[str, object]) -> None:
    columns = list(UPSTREAM[table])
    placeholders = ", ".join("?" for _ in columns)
    _ = con.execute(
        f"insert into {table} values ({placeholders})",
        [row.get(column) for column in columns],
    )


def allocate(
    compiled: dict[str, str],
    orders: Sequence[dict[str, object]],
    lines: Sequence[dict[str, object]],
) -> Allocation:
    con = _database(compiled, lines, orders)
    line_rows = _rows(con, compiled[LINE_MODEL], "order_id, line_item_id")
    order_rows = _rows(con, compiled[ORDER_MODEL], "order_id")
    return Allocation(
        {(str(row["order_id"]), str(row["line_item_id"])): row for row in line_rows},
        {str(row["order_id"]): row for row in order_rows},
    )


def _database(
    compiled: dict[str, str],
    lines: Sequence[dict[str, object]],
    orders: Sequence[dict[str, object]],
) -> duckdb.DuckDBPyConnection:
    """Upstream tables, then the line model as a table, which the order model reads."""
    con = duckdb.connect(":memory:")
    _load(con, lines, orders)
    line_sql = compiled[LINE_MODEL]
    _ = con.execute(
        f"create table {LINE_MODEL} as select * from ({line_sql}) as line_model"
    )
    return con


def _rows(
    con: duckdb.DuckDBPyConnection, sql: str, order_by: str
) -> list[dict[str, object]]:
    cursor = con.execute(f"select * from ({sql}) as result order by {order_by}")
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, values, strict=True)) for values in cursor.fetchall()]


# ---------------------------------------------------------------- fallbacks


def test_shipping_weights_use_value_after_direct_discount(
    compiled: dict[str, str],
) -> None:
    # A keeps a direct $5.00 discount, so its post-discount value is $55.00, not $60.00.
    result = allocate(
        compiled,
        [order(discount="-5.00", shipping="9.50")],
        [
            line("o1", "a", gross="60", discount="-5.00"),
            line("o1", "b", gross="40"),
        ],
    )
    a, b = result.lines[("o1", "a")], result.lines[("o1", "b")]
    assert a["merchandise_discount_shop_amount"] == dec("-5.00")
    assert a["merchandise_discount_allocation_method"] == "source_only"
    assert a["shipping_shop_amount"] == dec("5.50")
    assert a["shipping_allocation_method"] == "value_weight"
    assert b["shipping_shop_amount"] == dec("4.00")
    assert result.orders["o1"]["shipping_unallocated_shop_amount"] == dec("0")
    assert result.orders["o1"]["allocation_status"] == "complete"
    assert result.orders["o1"]["is_allocation_complete"] is True


def test_direct_lines_keep_their_amounts_and_signed_refunds_allocate_after_discount(
    compiled: dict[str, str],
) -> None:
    # Remaining discount -15.00 over gross 60/40 gives -9.00/-6.00. The refund weights
    # use gross plus the final discount: 46 and 34. The remaining refund -20.00 gives
    # -11.50 and -8.50.
    result = allocate(
        compiled,
        [order(discount="-20.00", refund="-30.00")],
        [
            line("o1", "a", gross="60", discount="-5.00", refund="-10.00"),
            line("o1", "b", gross="40"),
        ],
    )
    a, b = result.lines[("o1", "a")], result.lines[("o1", "b")]
    assert a["merchandise_discount_direct_shop_amount"] == dec("-5.00")
    assert a["merchandise_discount_allocated_shop_amount"] == dec("-9.00")
    assert a["merchandise_discount_shop_amount"] == dec("-14.00")
    assert a["merchandise_discount_allocation_method"] == "value_weight"
    assert b["merchandise_discount_shop_amount"] == dec("-6.00")
    assert a["merchandise_refund_direct_shop_amount"] == dec("-10.00")
    assert a["merchandise_refund_allocated_shop_amount"] == dec("-11.50")
    assert a["merchandise_refund_shop_amount"] == dec("-21.50")
    assert b["merchandise_refund_shop_amount"] == dec("-8.50")
    assert b["merchandise_refund_allocation_method"] == "value_weight"
    assert result.orders["o1"]["merchandise_discount_unallocated_shop_amount"] == dec(
        "0"
    )
    assert result.orders["o1"]["merchandise_refund_unallocated_shop_amount"] == dec("0")
    assert result.orders["o1"]["allocation_status"] == "complete"


def test_zero_value_lines_fall_back_to_quantities(compiled: dict[str, str]) -> None:
    # The spec example A01: $10.00 over zero values with quantities 2 and 1.
    result = allocate(
        compiled,
        [order(shipping="10.00")],
        [
            line("o1", "a", gross="0", quantity=2),
            line("o1", "b", gross="0", quantity=1),
        ],
    )
    assert result.lines[("o1", "a")]["shipping_shop_amount"] == dec("6.67")
    assert result.lines[("o1", "a")]["shipping_allocation_method"] == "quantity_weight"
    assert result.lines[("o1", "b")]["shipping_shop_amount"] == dec("3.33")
    assert result.orders["o1"]["shipping_unallocated_shop_amount"] == dec("0")


def test_known_zero_value_and_quantity_split_equally(compiled: dict[str, str]) -> None:
    # Input order differs from line ID order. The tie on the extra cent goes to "a".
    result = allocate(
        compiled,
        [order(shipping="10.00")],
        [
            line("o1", "c", gross="0", quantity=0),
            line("o1", "a", gross="0", quantity=0),
            line("o1", "b", gross="0", quantity=0),
        ],
    )
    assert result.lines[("o1", "a")]["shipping_shop_amount"] == dec("3.34")
    assert result.lines[("o1", "b")]["shipping_shop_amount"] == dec("3.33")
    assert result.lines[("o1", "c")]["shipping_shop_amount"] == dec("3.33")
    assert result.lines[("o1", "a")]["shipping_allocation_method"] == "equal_share"
    assert result.orders["o1"]["allocation_status"] == "complete"


def test_zero_unit_line_keeps_its_value_weight(compiled: dict[str, str]) -> None:
    # Line a has no ordered units but $60.00 of value. It keeps its value share.
    result = allocate(
        compiled,
        [order(shipping="10.00")],
        [
            line("o1", "a", gross="60", quantity=0),
            line("o1", "b", gross="40", quantity=2),
        ],
    )
    assert result.lines[("o1", "a")]["shipping_shop_amount"] == dec("6.00")
    assert result.lines[("o1", "a")]["shipping_allocation_method"] == "value_weight"
    assert result.lines[("o1", "b")]["shipping_shop_amount"] == dec("4.00")


def test_known_zero_remainder_needs_no_fallback_weights(
    compiled: dict[str, str],
) -> None:
    # Quantities are unknown, but no remainder needs a fallback weight.
    result = allocate(
        compiled,
        [order(discount="-5.00")],
        [
            line("o1", "a", gross="60", discount="-5.00", quantity=None),
            line("o1", "b", gross="40", quantity=None),
        ],
    )
    a, b = result.lines[("o1", "a")], result.lines[("o1", "b")]
    assert a["merchandise_discount_shop_amount"] == dec("-5.00")
    assert a["merchandise_discount_allocated_shop_amount"] == dec("0")
    assert a["merchandise_discount_allocation_method"] == "source_only"
    assert b["merchandise_discount_shop_amount"] == dec("0")
    assert b["merchandise_discount_allocation_method"] == "source_only"
    assert result.orders["o1"]["allocation_status"] == "complete"


# ----------------------------------------------------------- unknown inputs


def test_missing_quantity_blocks_the_quantity_fallback(
    compiled: dict[str, str],
) -> None:
    # Line b has no quantity. It must not split the shipping as an equal share.
    result = allocate(
        compiled,
        [order(shipping="10.00")],
        [
            line("o1", "a", gross="0", quantity=1),
            line("o1", "b", gross="0", quantity=None),
        ],
    )
    for line_id in ("a", "b"):
        row = result.lines[("o1", line_id)]
        assert row["shipping_shop_amount"] is None
        assert row["shipping_allocation_method"] == "unresolved_missing_weight"
    assert result.orders["o1"]["shipping_unallocated_shop_amount"] is None
    assert result.orders["o1"]["allocation_status"] == "unresolved_allocation"
    assert result.orders["o1"]["is_allocation_complete"] is False


def test_unknown_gift_flag_blocks_shares_but_keeps_known_direct_amounts(
    compiled: dict[str, str],
) -> None:
    # Line c might be a merchandise line, so a and b cannot absorb its share.
    # Discount has a known zero remainder, so line a keeps its direct -$5.00.
    result = allocate(
        compiled,
        [order(discount="-5.00", shipping="10.00")],
        [
            line("o1", "a", gross="60", discount="-5.00"),
            line("o1", "b", gross="40"),
            line("o1", "c", gross="0", gift=None),
        ],
    )
    a = result.lines[("o1", "a")]
    assert a["merchandise_discount_shop_amount"] == dec("-5.00")
    assert a["merchandise_discount_allocation_method"] == "source_only"
    for line_id in ("a", "b", "c"):
        row = result.lines[("o1", line_id)]
        assert row["shipping_shop_amount"] is None
        assert row["shipping_allocation_method"] == "unresolved_unknown_gift_card_flag"
    assert result.lines[("o1", "c")]["is_gift_card_line"] is None
    order_row = result.orders["o1"]
    assert order_row["shipping_unallocated_shop_amount"] is None
    assert order_row["merchandise_discount_unallocated_shop_amount"] == dec("0")
    assert order_row["allocation_status"] == "unknown_gift_card_flag"


def test_unknown_source_amounts_stay_null(compiled: dict[str, str]) -> None:
    # Unresolved sales make every order component unknown. Line b keeps its known
    # direct zero discount, but its allocated discount stays NULL.
    result = allocate(
        compiled,
        [order("o1", discount=None, refund=None, shipping=None, shipping_refund=None)],
        [
            line("o1", "a", gross=None, discount=None, refund=None),
            line("o1", "b", gross="40"),
        ],
    )
    b = result.lines[("o1", "b")]
    assert b["merchandise_discount_direct_shop_amount"] == dec("0")
    assert b["merchandise_discount_shop_amount"] is None
    assert b["merchandise_discount_allocation_method"] == "unresolved_remainder"
    assert b["shipping_shop_amount"] is None
    assert result.lines[("o1", "a")]["merchandise_discount_direct_shop_amount"] is None
    order_row = result.orders["o1"]
    assert order_row["merchandise_discount_unallocated_shop_amount"] is None
    assert order_row["allocation_status"] == "unknown_source_amount"


# -------------------------------------------------------------- gift cards


def test_gift_card_lines_take_no_merchandise_or_shipping_share(
    compiled: dict[str, str],
) -> None:
    # Gift line g has $50.00 of value. Only merchandise line a takes
    # the discount and shipping.
    result = allocate(
        compiled,
        [order(discount="-10.00", shipping="10.00")],
        [
            line("o1", "a", gross="60"),
            line("o1", "g", gross="50", gift=True),
        ],
    )
    a, g = result.lines[("o1", "a")], result.lines[("o1", "g")]
    assert a["merchandise_discount_shop_amount"] == dec("-10.00")
    assert a["merchandise_discount_allocation_method"] == "value_weight"
    assert a["shipping_shop_amount"] == dec("10.00")
    assert g["merchandise_discount_shop_amount"] == dec("0")
    assert g["merchandise_discount_allocation_method"] == "not_eligible"
    assert g["shipping_shop_amount"] == dec("0")
    assert g["shipping_allocation_method"] == "not_eligible"
    assert result.orders["o1"]["allocation_status"] == "complete"


def test_only_gift_lines_retain_the_residual(compiled: dict[str, str]) -> None:
    # No eligible line exists, so the order keeps $10.00. The order with no lines keeps
    # $5.00 and gets no fake line row.
    result = allocate(
        compiled,
        [order("o1", shipping="10.00"), order("o2", shipping="5.00")],
        [
            line("o1", "g1", gross="50", gift=True),
            line("o1", "g2", gross="0", gift=True),
        ],
    )
    assert result.lines[("o1", "g1")]["shipping_shop_amount"] == dec("0")
    assert result.lines[("o1", "g1")]["shipping_allocation_method"] == "not_eligible"
    assert result.orders["o1"]["shipping_unallocated_shop_amount"] == dec("10.00")
    assert result.orders["o1"]["allocation_status"] == "no_eligible_lines"
    assert result.orders["o1"]["is_allocation_complete"] is False
    assert result.orders["o2"]["shipping_unallocated_shop_amount"] == dec("5.00")
    assert result.orders["o2"]["allocation_status"] == "no_eligible_lines"
    assert all(key[0] != "o2" for key in result.lines)


# ------------------------------------------------------------ exact cents


def test_weights_split_cents_exactly_with_largest_remainders(
    compiled: dict[str, str],
) -> None:
    # Weights 1, 2, 3 over $10.00: 1.666.., 3.333.., 5.00. The extra cent goes to the
    # largest remainder, which is line a.
    result = allocate(
        compiled,
        [order(shipping="10.00")],
        [
            line("o1", "a", gross="1"),
            line("o1", "b", gross="2"),
            line("o1", "c", gross="3"),
        ],
    )
    amounts = {
        line_id: result.lines[("o1", line_id)]["shipping_shop_amount"]
        for line_id in ("a", "b", "c")
    }
    assert amounts == {"a": dec("1.67"), "b": dec("3.33"), "c": dec("5.00")}
    assert sum(cast(Decimal, value) for value in amounts.values()) == dec("10.00")
    assert result.orders["o1"]["shipping_unallocated_shop_amount"] == dec("0")


def test_input_row_order_does_not_change_cents(compiled: dict[str, str]) -> None:
    rows = [
        line("o1", "a", gross="1"),
        line("o1", "b", gross="1"),
        line("o1", "c", gross="1"),
    ]
    forward = allocate(compiled, [order(shipping="0.01")], rows)
    reverse = allocate(compiled, [order(shipping="0.01")], list(reversed(rows)))
    assert {key: row["shipping_shop_amount"] for key, row in forward.lines.items()} == {
        key: row["shipping_shop_amount"] for key, row in reverse.lines.items()
    }
    assert forward.lines[("o1", "a")]["shipping_shop_amount"] == dec("0.01")
    assert forward.lines[("o1", "b")]["shipping_shop_amount"] == dec("0")


def test_fractional_cent_remainder_stays_unresolved(compiled: dict[str, str]) -> None:
    result = allocate(
        compiled,
        [order(shipping="0.005")],
        [line("o1", "a", gross="1"), line("o1", "b", gross="1")],
    )
    for line_id in ("a", "b"):
        row = result.lines[("o1", line_id)]
        assert row["shipping_shop_amount"] is None
        assert row["shipping_allocation_method"] == "unresolved_fractional_cents"
    assert result.orders["o1"]["shipping_unallocated_shop_amount"] is None


# ------------------------------------------------------ static contract


def _normal(kind: str) -> str:
    return kind.lower().replace(" ", "")


@pytest.mark.parametrize("model", [LINE_MODEL, ORDER_MODEL])
def test_declared_columns_and_types_match_sql(
    model: str, compiled: dict[str, str]
) -> None:
    document = cast(
        dict[str, list[dict[str, object]]], yaml.safe_load(SCHEMA_FILE.read_text())
    )
    entry = next(item for item in document["models"] if item["name"] == model)
    declared = cast(list[dict[str, str]], entry["columns"])
    con = _database(compiled, [], [])
    _ = con.execute(f"create view {model}_probe as {compiled[model]}")
    rows = cast(
        list[tuple[object, object]],
        con.execute(f"describe {model}_probe").fetchall(),
    )
    actual = {str(row[0]): str(row[1]) for row in rows}
    names = [column["name"] for column in declared]
    assert len(names) == len(set(names))
    assert sorted(names) == sorted(actual)
    for column in declared:
        assert _normal(actual[column["name"]]) == _normal(column["data_type"]), column[
            "name"
        ]
