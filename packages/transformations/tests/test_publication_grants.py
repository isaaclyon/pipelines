"""Snowflake publication grants only analytical relations to the default reader.

The macro runs against stub warehouse functions so the exact SQL and its order
are checked offline. Live behavior needs an explicit development warehouse.
"""

import json
import re
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import jinja2
import pytest

from analytics_transformations.mart_contracts import MartContract, snapshot
from analytics_transformations.publication import publish_snowflake_marts

MACROS = Path(__file__).parents[1] / "dbt/macros/publication.sql"
ID = "abcdef123456789"


def contract(relation: str, access: str) -> MartContract:
    return MartContract(
        f"model.analytics.{relation}",
        relation,
        "Synthetic.",
        "One row",
        ("id",),
        (),
        (),
        "Analytics Platform",
        access,
    )


class Warehouse:
    def __init__(
        self,
        tables: Sequence[str],
        marker: str | None = None,
        exposed: Sequence[str] = (),
    ) -> None:
        self.sql: list[str] = []
        self.tables, self.marker, self.exposed = tables, marker, exposed

    def run_query(self, sql: str) -> SimpleNamespace:
        self.sql.append(sql)
        rows: list[tuple[object, ...]] = []
        if "table_type from" in sql:
            rows = [(name.upper(), "BASE TABLE") for name in self.tables]
            if self.marker is not None:
                rows.append(("_PUBLICATION", "BASE TABLE"))
        elif "count(*)" in sql and "table_name = '_PUBLICATION'" in sql:
            rows = [(1 if self.marker is not None else 0,)]
        elif "count(*)" in sql:
            rows = [(1,)]
        elif "select contracts from" in sql:
            rows = [(self.marker,)]
        elif "select publication_id from" in sql and "lease" in sql:
            rows = [(ID,)]
        elif "table_privileges" in sql:
            rows = [(name, "ANALYTICS_READER") for name in self.exposed]
        return SimpleNamespace(rows=rows)


def publish(
    warehouse: Warehouse,
    plan: tuple[MartContract, ...],
    restricted_role: str | None = "analytics_private",
    tightenings: Sequence[str] | None = None,
) -> None:
    source = MACROS.read_text()
    # dbt's return() raises; capture the value so the stub keeps jinja2 simple.
    returned: list[object] = []
    source = source.replace("{{ return(", "{% do _return(").replace(
        ") }}\n{% endmacro %}", ") %}\n{% endmacro %}"
    )
    source = re.sub(
        r"\{% set c = (publication_context\([^%]*\)) %\}",
        r"{% set _ignored = \1 %}{% set c = _last() %}",
        source,
    )

    def quote(value: str) -> str:
        return f'"{value}"'

    def env_var(_name: str) -> str:
        return "customers_candidate_" + ID[:12]

    def refuse(message: str) -> None:
        raise ValueError(message)

    environment = jinja2.Environment(extensions=["jinja2.ext.do"])
    values: dict[str, object] = {
        "execute": True,
        "target": SimpleNamespace(
            type="snowflake", name="snowflake-development", database="analytics"
        ),
        "adapter": SimpleNamespace(quote=quote),
        "modules": SimpleNamespace(re=re),
        "exceptions": SimpleNamespace(raise_compiler_error=refuse),
        "env_var": env_var,
        "fromjson": json.loads,
        "run_query": warehouse.run_query,
        "_return": returned.append,
        "_last": lambda: returned[-1],
    }
    for name, value in values.items():
        cast(dict[str, object], environment.globals)[name] = value
    module = environment.from_string(source).module
    macro = cast(Callable[..., object], vars(module)["publish_mart_candidate"])
    macro(
        "customers",
        "customers_candidate_" + ID[:12],
        ID,
        "analytics_reader",
        restricted_role,
        [item.relation for item in plan],
        snapshot(plan),
        tightenings,
    )


def grants(warehouse: Warehouse) -> list[str]:
    return [sql for sql in warehouse.sql if sql.startswith(("grant", "revoke"))]


PLAN = (contract("dim_a", "analytical"), contract("dim_pii", "restricted"))


def test_default_reader_gets_only_analytical_relations_after_a_revoke() -> None:
    warehouse = Warehouse(["dim_a", "dim_pii"])
    publish(warehouse, PLAN)
    statements = grants(warehouse)
    assert statements[0].startswith("revoke select on all tables")
    assert statements[0].endswith('"ANALYTICS_READER"')
    reader = [s for s in statements if s.endswith('to role "ANALYTICS_READER"')]
    assert [s.split(".")[-1].split(" ")[0] for s in reader if "table" in s] == [
        '"DIM_A"',
        '"_PUBLICATION"',
    ]
    private = [s for s in statements if s.endswith('to role "ANALYTICS_PRIVATE"')]
    assert [s.split(".")[-1].split(" ")[0] for s in private if "table" in s] == [
        '"DIM_PII"',
        '"_PUBLICATION"',
    ]
    assert not any("DIM_PII" in s for s in reader)
    # USAGE follows every table grant and precedes the swap.
    usage = [i for i, s in enumerate(warehouse.sql) if s.startswith("grant usage")]
    swap = next(i for i, s in enumerate(warehouse.sql) if "swap with" in s)
    last_table = max(
        i for i, s in enumerate(warehouse.sql) if s.startswith("grant select")
    )
    assert max(usage) < swap and min(usage) > last_table


def test_without_a_restricted_role_restricted_tables_have_no_consumer() -> None:
    warehouse = Warehouse(["dim_a", "dim_pii"])
    publish(warehouse, PLAN, restricted_role=None)
    assert not any("DIM_PII" in s for s in grants(warehouse))
    assert not any("ANALYTICS_PRIVATE" in s for s in warehouse.sql)


def test_stale_default_reader_grant_on_restricted_relation_blocks_swap() -> None:
    warehouse = Warehouse(["dim_a", "dim_pii"], exposed=["DIM_PII"])
    with pytest.raises(ValueError, match="readable by the default reader"):
        publish(warehouse, PLAN)
    assert not any(s.startswith("grant usage") or "swap" in s for s in warehouse.sql)


def test_reclassification_of_a_published_relation_blocks() -> None:
    old = snapshot((contract("dim_a", "analytical"), contract("dim_pii", "analytical")))
    warehouse = Warehouse(["dim_a", "dim_pii"], marker=old)
    with pytest.raises(ValueError, match="access changed"):
        publish(warehouse, PLAN)
    assert not grants(warehouse)


def test_old_marker_without_access_means_analytical() -> None:
    entries = cast(
        list[dict[str, object]],
        json.loads(snapshot((contract("dim_a", "analytical"),))),
    )
    legacy = [
        {key: value for key, value in entry.items() if key != "access"}
        for entry in entries
    ]
    warehouse = Warehouse(["dim_a", "dim_pii"], marker=json.dumps(legacy))
    publish(warehouse, PLAN)
    assert any("swap with" in sql for sql in warehouse.sql)


def test_roles_are_passed_to_every_operation_and_must_differ(tmp_path: Path) -> None:
    seen: list[dict[str, object]] = []

    def runner(command: Sequence[str], env: Mapping[str, str], timeout: int) -> None:
        if command[1] == "run-operation" and "--args" in command:
            seen.append(
                cast(
                    dict[str, object], json.loads(command[command.index("--args") + 1])
                )
            )
        if command[1] == "run-operation":
            return
        raise RuntimeError("stop before build")

    with pytest.raises(RuntimeError, match="stop before build"):
        _ = publish_snowflake_marts(
            environment="development",
            namespace="customers",
            project_dir=tmp_path,
            profiles_dir=tmp_path,
            reader_role="analytics_reader",
            restricted_reader_role="analytics_private",
            runner=runner,
        )
    assert seen[0]["restricted_reader_role"] == "analytics_private"
    for role in ("ANALYTICS_READER", "public"):
        with pytest.raises(ValueError, match="restricted"):
            _ = publish_snowflake_marts(
                environment="development",
                namespace="customers",
                project_dir=tmp_path,
                profiles_dir=tmp_path,
                reader_role="analytics_reader",
                restricted_reader_role=role,
                runner=runner,
            )


def test_reviewed_tightening_is_allowed_only_when_named() -> None:
    old = snapshot(
        (contract("dim_order_contacts", "analytical"), contract("dim_a", "analytical"))
    )
    plan = (
        contract("dim_order_contacts", "restricted"),
        contract("dim_a", "analytical"),
    )
    with pytest.raises(ValueError, match="access changed"):
        publish(Warehouse(["dim_order_contacts", "dim_a"], marker=old), plan)
    warehouse = Warehouse(["dim_order_contacts", "dim_a"], marker=old)
    publish(warehouse, plan, tightenings=["dim_order_contacts"])
    assert any("swap with" in sql for sql in warehouse.sql)
    # A relation outside the reviewed list and any loosening still fail.
    other = snapshot((contract("dim_pii", "analytical"),))
    with pytest.raises(ValueError, match="access changed"):
        publish(
            Warehouse(["dim_pii"], marker=other),
            (contract("dim_pii", "restricted"),),
            tightenings=["dim_order_contacts"],
        )
    loose = snapshot((contract("dim_order_contacts", "restricted"),))
    with pytest.raises(ValueError, match="access changed"):
        publish(
            Warehouse(["dim_order_contacts"], marker=loose),
            (contract("dim_order_contacts", "analytical"),),
            tightenings=["dim_order_contacts"],
        )
