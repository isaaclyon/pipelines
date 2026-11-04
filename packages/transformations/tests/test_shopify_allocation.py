"""Shopify USD allocation: exact cents, stable rounding, explicit unresolved states.

The tests compile ``shopify_allocate_usd`` through dbt once and run the generated
SQL on DuckDB for each case. Expected values are fixed by hand from the allocation
rules. The opt-in Snowflake test runs the same fixture read-only on the
development target.
"""

import json
import os
import random
import subprocess
from collections.abc import Mapping, Sequence
from fractions import Fraction
from pathlib import Path
from typing import cast

import duckdb
import pytest

ROOT = Path(__file__).parents[1]
# Compiles shopify_allocate_usd, whose only caller this model marks changed.
pytestmark = pytest.mark.dbt("int_shopify_order_lines__allocations")

# (shop_id, order_id, line_item_id, component, amount, value_weight, quantity_weight)
type Row = tuple[str, str, str | None, str, str | None, str | None, int | None]
# (shop_id, order_id, component, line_item_id) -> (allocated amount text, method)
type Result = dict[tuple[str, str, str, str], tuple[str | None, str]]

EXPECTED_COLUMNS = (
    "shop_id",
    "order_id",
    "line_item_id",
    "component",
    "allocated_shop_amount",
    "allocation_method",
)
SELECT_ALLOCATION = """
select
    shop_id,
    order_id,
    line_item_id,
    component,
    cast(allocated_shop_amount as varchar) as allocated_shop_amount,
    allocation_method
from ({allocation}) as allocated
order by shop_id, order_id, component, line_item_id
"""
INPUT_COLUMNS = (
    "shop_id, order_id, line_item_id, component, component_shop_amount, "
    "value_weight, quantity_weight"
)
EMPTY_INPUT = """
select * from (
    select
        cast(null as varchar) as shop_id,
        cast(null as varchar) as order_id,
        cast(null as varchar) as line_item_id,
        cast(null as varchar) as component,
        cast(null as decimal(38, 9)) as component_shop_amount,
        cast(null as decimal(38, 9)) as value_weight,
        cast(null as bigint) as quantity_weight
) as empty_input where false
"""


def line(
    line_item_id: str | None,
    *,
    amount: str | None = "10.00",
    value: str | None = "1",
    quantity: int | None = 1,
    component: str = "shipping",
    order_id: str = "o1",
    shop_id: str = "s1",
) -> Row:
    return (shop_id, order_id, line_item_id, component, amount, value, quantity)


def _cell(value: str | int | None, kind: str) -> str:
    if value is None:
        return f"cast(null as {kind})"
    text = str(value)
    assert "'" not in text, "fixture values must not contain quotes"
    return f"cast('{text}' as {kind})"


def values_sql(rows: Sequence[Row]) -> str:
    kinds = (
        "varchar",
        "varchar",
        "varchar",
        "varchar",
        "decimal(38, 9)",
        "decimal(38, 9)",
        "bigint",
    )
    body = ",\n".join(
        "("
        + ", ".join(_cell(cell, kind) for cell, kind in zip(row, kinds, strict=True))
        + ")"
        for row in rows
    )
    return f"select * from (values\n{body}\n) as v({INPUT_COLUMNS})"


def inline_query(input_sql: str) -> str:
    """Render the macro over an input query, returning typed text for every output."""
    return (
        "{% set allocation_input %}\n"
        + input_sql
        + "\n{% endset %}\n"
        + SELECT_ALLOCATION.format(
            allocation="{{ shopify_allocate_usd(allocation_input) }}"
        )
    )


def show(
    inline: str,
    target: Path,
    duckdb_path: Path,
    *,
    dbt_target: str = "local",
) -> subprocess.CompletedProcess[str]:
    env: Mapping[str, str] = os.environ | {
        "DBT_TARGET": dbt_target,
        "DBT_NAMESPACE": "allocation",
        "DUCKDB_PATH": str(duckdb_path),
    }
    return subprocess.run(
        [
            "dbt",
            "show",
            "--inline",
            inline,
            "--limit",
            "-1",
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
        env=dict(env),
        timeout=180,
        capture_output=True,
        text=True,
        check=False,
    )


# Stands in for each case's input query in the macro's compiled SQL.
INPUT_PLACEHOLDER = "__allocation_input__"


@pytest.fixture(scope="module")
def allocation(tmp_path_factory: pytest.TempPathFactory) -> str:
    """Compile the macro once; each case runs it on DuckDB without dbt."""
    target = tmp_path_factory.mktemp("allocation-target")
    result = subprocess.run(
        [
            "dbt",
            "compile",
            "--inline",
            inline_query(INPUT_PLACEHOLDER),
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
            "DBT_NAMESPACE": "allocation",
            "DUCKDB_PATH": str(target / "allocation.duckdb"),
        },
        timeout=180,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    compiled = cast(dict[str, str], json.loads(result.stdout))["compiled"]
    assert compiled.count(INPUT_PLACEHOLDER) == 1
    return compiled


def allocate_sql(input_sql: str, allocation: str) -> list[dict[str, object]]:
    with duckdb.connect() as db:
        cursor = db.execute(allocation.replace(INPUT_PLACEHOLDER, input_sql))
        columns = [column[0].lower() for column in cursor.description or ()]
        return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def parse_show(stdout: str) -> list[dict[str, object]]:
    """Rows from ``dbt show --output json``."""
    payload = cast(dict[str, list[dict[str, object]]], json.loads(stdout))
    return [
        {key.lower(): value for key, value in row.items()} for row in payload["show"]
    ]


def results(rows: Sequence[Mapping[str, object]]) -> Result:
    return {
        (
            str(item["shop_id"]),
            str(item["order_id"]),
            str(item["component"]),
            str(item["line_item_id"]),
        ): (
            None
            if item["allocated_shop_amount"] is None
            else str(item["allocated_shop_amount"]),
            str(item["allocation_method"]),
        )
        for item in rows
    }


def allocate(rows: Sequence[Row], allocation: str) -> Result:
    return results(allocate_sql(values_sql(rows), allocation))


# Half-cent split, shares of exactly half a cent: the lower line ID takes the cent.
HALF_CENT_ROWS = [
    line("line-b", amount="0.01", value="1", quantity=1),
    line("line-a", amount="0.01", value="1", quantity=1),
]

# Value weights 1, 2 and 0 for a $10.00 shipping component.
VALUE_ROWS = [
    line("line-b", amount="10.00", value="1.000000000", quantity=1),
    line("line-a", amount="10.00", value="2.000000000", quantity=2),
    line("line-c", amount="10.00", value="0.000000000", quantity=3),
]

# Known value weights are all zero, so quantities 1, 2 and 3 decide the split.
QUANTITY_ROWS = [
    line("line-a", amount="10.00", value="0", quantity=1),
    line("line-b", amount="10.00", value="0", quantity=2),
    line("line-c", amount="10.00", value="0", quantity=3),
]

# Known value and quantity weights are all zero, so the split is equal.
EQUAL_ROWS = [
    line("line-c", amount="10.00", value="0", quantity=0),
    line("line-a", amount="10.00", value="0", quantity=0),
    line("line-b", amount="10.00", value="0", quantity=0),
]


def test_value_weights_allocate_by_value_with_largest_remainder(
    allocation: str,
) -> None:
    actual = allocate(VALUE_ROWS, allocation)
    assert actual == {
        ("s1", "o1", "shipping", "line-a"): ("6.670000000", "value_weight"),
        ("s1", "o1", "shipping", "line-b"): ("3.330000000", "value_weight"),
        ("s1", "o1", "shipping", "line-c"): ("0.000000000", "value_weight"),
    }


def test_known_zero_values_fall_back_to_quantities(allocation: str) -> None:
    actual = allocate(QUANTITY_ROWS, allocation)
    assert actual == {
        ("s1", "o1", "shipping", "line-a"): ("1.670000000", "quantity_weight"),
        ("s1", "o1", "shipping", "line-b"): ("3.330000000", "quantity_weight"),
        ("s1", "o1", "shipping", "line-c"): ("5.000000000", "quantity_weight"),
    }


def test_known_zero_weights_fall_back_to_equal_shares(allocation: str) -> None:
    actual = allocate(EQUAL_ROWS, allocation)
    assert actual == {
        ("s1", "o1", "shipping", "line-a"): ("3.340000000", "equal_share"),
        ("s1", "o1", "shipping", "line-b"): ("3.330000000", "equal_share"),
        ("s1", "o1", "shipping", "line-c"): ("3.330000000", "equal_share"),
    }


def test_half_cent_shares_give_the_extra_cent_to_the_lower_line_id(
    allocation: str,
) -> None:
    actual = allocate(HALF_CENT_ROWS, allocation)
    assert actual == {
        ("s1", "o1", "shipping", "line-a"): ("0.010000000", "value_weight"),
        ("s1", "o1", "shipping", "line-b"): ("0.000000000", "value_weight"),
    }


def test_nine_digit_value_weights_are_exact(allocation: str) -> None:
    rows = [
        line("line-a", amount="1.00", value="0.000000001", quantity=0),
        line("line-b", amount="1.00", value="0.000000002", quantity=0),
    ]
    actual = allocate(rows, allocation)
    # Exact shares are 0.3333... and 0.6666...; the larger remainder gets the cent.
    assert actual == {
        ("s1", "o1", "shipping", "line-a"): ("0.330000000", "value_weight"),
        ("s1", "o1", "shipping", "line-b"): ("0.670000000", "value_weight"),
    }


def test_negative_total_is_allocated_by_magnitude(allocation: str) -> None:
    rows = [
        line(row[2], amount="-10.00", value=row[5], quantity=row[6])
        for row in VALUE_ROWS
    ]
    actual = allocate(rows, allocation)
    assert actual == {
        ("s1", "o1", "shipping", "line-a"): ("-6.670000000", "value_weight"),
        ("s1", "o1", "shipping", "line-b"): ("-3.330000000", "value_weight"),
        ("s1", "o1", "shipping", "line-c"): ("0.000000000", "value_weight"),
    }


def test_input_order_does_not_change_allocations(allocation: str) -> None:
    forward = allocate(EQUAL_ROWS, allocation)
    reversed_result = allocate(list(reversed(EQUAL_ROWS)), allocation)
    assert reversed_result == forward
    assert forward[("s1", "o1", "shipping", "line-a")] == ("3.340000000", "equal_share")


def test_missing_selected_weight_leaves_the_whole_component_unresolved(
    allocation: str,
) -> None:
    rows = [
        line("line-a", value="1", quantity=1),
        line("line-b", value=None, quantity=2),
        line("line-c", amount="5.00", value="1", quantity=1, component="tax"),
    ]
    actual = allocate(rows, allocation)
    # The known line keeps no share, and the other component is unaffected.
    assert actual == {
        ("s1", "o1", "shipping", "line-a"): (None, "unresolved_missing_weight"),
        ("s1", "o1", "shipping", "line-b"): (None, "unresolved_missing_weight"),
        ("s1", "o1", "tax", "line-c"): ("5.000000000", "value_weight"),
    }


def test_all_missing_value_weights_are_unresolved(allocation: str) -> None:
    rows = [
        line("line-a", value=None, quantity=1),
        line("line-b", value=None, quantity=1),
    ]
    actual = allocate(rows, allocation)
    assert actual == {
        ("s1", "o1", "shipping", "line-a"): (None, "unresolved_missing_weight"),
        ("s1", "o1", "shipping", "line-b"): (None, "unresolved_missing_weight"),
    }


def test_missing_value_weight_with_known_zero_stays_unresolved(allocation: str) -> None:
    # Missing weights are not known zeros; no fallback may hide them.
    rows = [
        line("line-a", amount="10.00", value="0", quantity=1),
        line("line-b", amount="10.00", value=None, quantity=3),
    ]
    actual = allocate(rows, allocation)
    assert actual == {
        ("s1", "o1", "shipping", "line-a"): (None, "unresolved_missing_weight"),
        ("s1", "o1", "shipping", "line-b"): (None, "unresolved_missing_weight"),
    }


def test_negative_selected_weight_is_unresolved(allocation: str) -> None:
    rows = [
        line("line-a", value="2", quantity=1),
        line("line-b", value="-1", quantity=1),
    ]
    actual = allocate(rows, allocation)
    assert actual == {
        ("s1", "o1", "shipping", "line-a"): (None, "unresolved_negative_weight"),
        ("s1", "o1", "shipping", "line-b"): (None, "unresolved_negative_weight"),
    }


def test_zero_total_allocates_zero_when_weights_are_valid(allocation: str) -> None:
    rows = [
        line("line-a", amount="0.00", value="1", quantity=1),
        line("line-b", amount="0.00", value="2", quantity=2),
    ]
    actual = allocate(rows, allocation)
    assert actual == {
        ("s1", "o1", "shipping", "line-a"): ("0.000000000", "value_weight"),
        ("s1", "o1", "shipping", "line-b"): ("0.000000000", "value_weight"),
    }


def test_no_eligible_rows_return_no_rows(allocation: str) -> None:
    assert allocate_sql(EMPTY_INPUT, allocation) == []


def test_noncent_total_is_unresolved(allocation: str) -> None:
    rows = [
        line("line-a", amount="0.005", value="1"),
        line("line-b", amount="0.005", value="1"),
    ]
    actual = allocate(rows, allocation)
    assert actual == {
        ("s1", "o1", "shipping", "line-a"): (None, "unresolved_fractional_cents"),
        ("s1", "o1", "shipping", "line-b"): (None, "unresolved_fractional_cents"),
    }


@pytest.mark.parametrize(
    ("rows", "reason"),
    [
        (
            [line("line-a", amount=None), line("line-b", amount=None)],
            "unresolved_missing_amount",
        ),
        (
            [line("line-a", amount="5.00"), line("line-b", amount="5.01")],
            "unresolved_inconsistent_amount",
        ),
        (
            [line("line-a", value="1"), line("line-a", value="1")],
            "unresolved_duplicate_line",
        ),
        (
            [line(None, value="1"), line("line-b", value="1")],
            "unresolved_missing_key",
        ),
    ],
    ids=["missing_amount", "inconsistent_amount", "duplicate_line", "missing_line_id"],
)
def test_invalid_component_inputs_are_unresolved(
    rows: list[Row], reason: str, allocation: str
) -> None:
    actual = allocate(rows, allocation)
    assert {method for _, method in actual.values()} == {reason}
    assert {amount for amount, _ in actual.values()} == {None}


def test_components_and_orders_are_allocated_separately(allocation: str) -> None:
    rows = [
        line("line-a", amount="1.00", value="1", component="shipping", order_id="o1"),
        line("line-b", amount="1.00", value="1", component="shipping", order_id="o1"),
        line("line-a", amount="1.00", value="1", component="shipping", order_id="o2"),
        line("line-b", amount="2.00", value="1", component="tax", order_id="o2"),
    ]
    actual = allocate(rows, allocation)
    assert actual == {
        ("s1", "o1", "shipping", "line-a"): ("0.500000000", "value_weight"),
        ("s1", "o1", "shipping", "line-b"): ("0.500000000", "value_weight"),
        ("s1", "o2", "shipping", "line-a"): ("1.000000000", "value_weight"),
        ("s1", "o2", "tax", "line-b"): ("2.000000000", "value_weight"),
    }


def money(cents: int) -> str:
    sign = "-" if cents < 0 else ""
    magnitude = abs(cents)
    return f"{sign}{magnitude // 100}.{magnitude % 100:02d}"


def nanos(weight: int) -> str:
    return f"{weight // 10**9}.{weight % 10**9:09d}"


def test_random_components_conserve_cents_and_stay_within_one_cent(
    allocation: str,
) -> None:
    generator = random.Random(20260701)
    rows: list[Row] = []
    expected_total: dict[str, Fraction] = {}
    for index in range(60):
        component = f"component-{index}"
        cents = generator.randint(-250_000, 250_000)
        amount = Fraction(cents, 100)
        count = generator.randint(1, 6)
        weights = [generator.randint(1, 999_999_999) for _ in range(count)]
        expected_total[component] = amount
        for position, weight in enumerate(weights):
            rows.append(
                line(
                    f"line-{position}",
                    amount=money(cents),
                    value=nanos(weight),
                    quantity=0,
                    component=component,
                )
            )
    actual = allocate(rows, allocation)
    assert {method for _, method in actual.values()} == {"value_weight"}

    by_component: dict[str, list[tuple[str, Fraction]]] = {}
    for (_, _, component, line_id), (amount, _) in actual.items():
        assert amount is not None
        by_component.setdefault(component, []).append((line_id, Fraction(amount)))
    for component, allocated in by_component.items():
        total = expected_total[component]
        assert sum((amount for _, amount in allocated), Fraction(0)) == total
        weights = {
            row[2]: Fraction(row[5] or "0") for row in rows if row[3] == component
        }
        weight_sum = sum(weights.values(), Fraction(0))
        for line_id, amount in allocated:
            exact = total * weights[line_id] / weight_sum
            assert abs(amount - exact) < Fraction(1, 100), (component, line_id)


def test_value_overflow_fails_instead_of_approximating(allocation: str) -> None:
    rows = [
        line("line-a", amount="100000000000000000000", value="100000000000000000000")
    ]
    with pytest.raises(duckdb.OutOfRangeException, match="Overflow"):
        _ = allocate_sql(values_sql(rows), allocation)


@pytest.mark.skipif(
    os.getenv("ALLOCATION_LIVE_ACCEPTANCE") != "development",
    reason="explicit development acceptance only",
)
@pytest.mark.parametrize(
    "fixture",
    [VALUE_ROWS, QUANTITY_ROWS, EQUAL_ROWS, HALF_CENT_ROWS],
    ids=["value", "quantity", "equal", "half_cent"],
)
def test_snowflake_matches_duckdb_on_the_same_fixture(
    fixture: list[Row], allocation: str, tmp_path: Path
) -> None:
    """Read-only check: the generated SELECT runs on the development warehouse."""
    assert os.environ["ANALYTICS_ENVIRONMENT"] == "development"
    expected = allocate(fixture, allocation)
    result = show(
        inline_query(values_sql(fixture)),
        tmp_path / "target",
        tmp_path / "unused.duckdb",
        dbt_target="snowflake-development",
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert tuple(parse_show(result.stdout)[0].keys()) == EXPECTED_COLUMNS
    assert results(parse_show(result.stdout)) == expected
