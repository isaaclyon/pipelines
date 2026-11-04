import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import cast

import pytest

from analytics_transformations.layer_dependencies import (
    Violation,
    check,
    find_hard_coded_relations,
    parse_manifest,
    root_project,
)

FIXTURES = Path(__file__).parent / "fixtures" / "layer_dependencies"


def _parse_fixture(name: str, tmp_path: Path) -> Path:
    project = FIXTURES / name
    dbt = shutil.which("dbt") or str(Path(sys.executable).parent / "dbt")
    target = tmp_path / "target"
    _ = subprocess.run(
        [
            dbt,
            "parse",
            "--quiet",
            "--project-dir",
            str(project),
            "--profiles-dir",
            str(project),
            "--target-path",
            str(target),
            "--log-path",
            str(tmp_path / "logs"),
        ],
        check=True,
        env={
            **os.environ,
            "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
            "LAYER_FIXTURE_DUCKDB": str(tmp_path / "fixture.duckdb"),
        },
    )
    return target / "manifest.json"


def _violations(manifest: Path) -> list[Violation]:
    data = cast(object, json.loads(manifest.read_text()))
    return check(parse_manifest(data), root_project(data))


def test_permitted_layers_seeds_snapshots_and_exempt_resources_pass(
    tmp_path: Path,
) -> None:
    manifest = _parse_fixture("valid", tmp_path)

    resources = parse_manifest(cast(object, json.loads(manifest.read_text())))
    [generic] = [r for r in resources if r.name == "CustomGenericTest"]
    assert generic.alias == "CustomFailureAlias"
    assert _violations(manifest) == []


def test_backward_bypassing_and_misplaced_dependencies_fail(tmp_path: Path) -> None:
    manifest = _parse_fixture("invalid", tmp_path)

    found = {
        (violation.resource, violation.rule) for violation in _violations(manifest)
    }

    assert found == {
        ("stg_shop__orders_enriched", "forbidden-dependency"),
        ("stg_other__orders", "cross-source-staging"),
        ("int_orders__raw", "forbidden-dependency"),
        ("int_orders__countries", "forbidden-dependency"),
        ("int_orders__from_mart", "forbidden-dependency"),
        ("fct_orders", "hard-coded-relation"),
        ("stg_shop__misplaced", "prefix-mismatch"),
        ("stg_orders_dedup", "staging-name"),
        ("fct_unplaced", "unknown-layer"),
        ("orders_snapshot", "forbidden-dependency"),
        ("internal_dashboard", "forbidden-dependency"),
    }


def test_hard_coded_relations_are_reported_with_their_names(tmp_path: Path) -> None:
    manifest = _parse_fixture("invalid", tmp_path)

    details = {
        violation.detail
        for violation in _violations(manifest)
        if violation.rule == "hard-coded-relation"
    }

    assert details == {
        "reads raw.shop.customers without ref() or source()",
        "reads {{ target.schema }}.countries without ref() or source()",
        'reads "legacy"."orders" without ref() or source()',
        "reads a string literal without ref() or source()",
    }


@pytest.mark.parametrize(
    "sql",
    [
        "select * from {{ ref('o') }} join {{ source('shop', 't') }} as t on true",
        # A project macro that wraps source() for sparse local raw tables.
        "select * from {{ sparse_source('shop', 'orders__duties') }}",
        "with a as (select 1 as x), b (x) as (select x from a) select * from b, a",
        "select extract(year from at), trim(both ' ' from name) from {{ ref('o') }}",
        "select * from {{ ref('o') }} where a is not distinct from b",
        "select * from {{ ref('o') }}, lateral flatten(input => o.tags) as tag",
        "select * from table(flatten(input => parse_json('[]')))",
        "select * from {{ ref('o') }} cross join range(3) as r (n)",
        "select * from ({{ ref('o') }} as o join {{ ref('p') }} as p on true)",
        "with a as (with b as (select 1) select * from b) select * from a",
        "with recursive t as (select 1 as n union all select n from t) select * from t",
        "select * from (select 1) as one cross join unnest([1, 2]) as n (value)",
        "select * from {{ ref('o') }} where id in (select id from {{ ref('p') }})",
        "-- select * from raw.orders\nselect 'from raw.orders' from {{ ref('o') }}",
        "/* join raw.orders */ select {# from raw.orders #} 1 from {{ this }}",
        "{% if is_incremental() %} select 1 from {{ this }} {% endif %}",
    ],
)
def test_declared_and_local_relations_are_not_hard_coded(sql: str) -> None:
    assert find_hard_coded_relations(sql) == []


@pytest.mark.parametrize(
    ("sql", "relation"),
    [
        ("select * from orders", "orders"),
        ("select * from {{ ref('o') }} as o, raw.tags as t", "raw.tags"),
        (
            "select * from {{ ref('o') }} left join analytics.main.fct_x x on true",
            "analytics.main.fct_x",
        ),
        (
            "select * from {{ var('raw_schema') }}.orders",
            "{{ var('raw_schema') }}.orders",
        ),
        ("select * from 'b.csv'", "a string literal"),
        ("with a as (select * from raw.orders) select * from a", "raw.orders"),
        # Cases from the PR #74 review.
        ("select * from {{ 'raw.orders' }}", "{{ 'raw.orders' }}"),
        (
            "select * from {{ var('raw_relation', 'raw.orders') }}",
            "{{ var('raw_relation', 'raw.orders') }}",
        ),
        (
            "select * from ({{ ref('o') }} as o join raw.customers as c on true)",
            "raw.customers",
        ),
        ("select * from identifier('raw.shop.orders')", "identifier(...)"),
        ("select * from table('raw.shop.orders')", "table(...)"),
        ("select * from read_parquet('s3://raw/orders.parquet')", "read_parquet(...)"),
        ("select * from @raw_stage/orders.csv", "a stage"),
    ],
)
def test_undeclared_relations_are_hard_coded(sql: str, relation: str) -> None:
    assert find_hard_coded_relations(sql) == [relation]


def _model(
    name: str, path: str, *dependencies: str, package: str = "p"
) -> dict[str, object]:
    return {
        "resource_type": "model",
        "name": name,
        "package_name": package,
        "original_file_path": path,
        "depends_on": {"nodes": list(dependencies)},
        "raw_code": "select 1",
    }


def test_semantic_models_read_marts_only() -> None:
    manifest = {
        "nodes": {
            "model.p.int_orders__totals": _model(
                "int_orders__totals", "models/intermediate/int_orders__totals.sql"
            ),
            "model.p.fct_orders": _model("fct_orders", "models/marts/fct_orders.sql"),
        },
        "semantic_models": {
            "semantic_model.p.orders": {
                "resource_type": "semantic_model",
                "name": "orders",
                "package_name": "p",
                "depends_on": {"nodes": ["model.p.fct_orders"]},
            },
            "semantic_model.p.order_totals": {
                "resource_type": "semantic_model",
                "name": "order_totals",
                "package_name": "p",
                "depends_on": {"nodes": ["model.p.int_orders__totals"]},
            },
        },
    }

    assert check(parse_manifest(manifest), "p") == [
        Violation(
            "order_totals",
            "forbidden-dependency",
            "reads intermediate model int_orders__totals; "
            "semantic models read marts only",
        )
    ]


def test_malformed_manifest_is_rejected() -> None:
    with pytest.raises(ValueError, match="nodes.model.p.x.name"):
        _ = parse_manifest({"nodes": {"model.p.x": {"resource_type": "model"}}})


def test_installed_package_models_are_unchecked_staging_inputs() -> None:
    manifest = {
        "nodes": {
            "model.vendor.orders": _model(
                "orders",
                "models/orders.sql",
                "source.vendor.raw.orders",
                package="vendor",
            ),
            "model.p.stg_vendor__orders": _model(
                "stg_vendor__orders",
                "models/staging/vendor/stg_vendor__orders.sql",
                "model.vendor.orders",
            ),
            "model.p.fct_orders": _model(
                "fct_orders", "models/marts/fct_orders.sql", "model.vendor.orders"
            ),
        },
    }

    assert check(parse_manifest(manifest), "p") == [
        Violation(
            "fct_orders",
            "forbidden-dependency",
            "reads package model orders; marts reads staging, intermediate, "
            "and mart models",
        )
    ]


def test_exposures_reading_metrics_name_the_metric() -> None:
    manifest = {
        "metrics": {
            "metric.p.revenue": {
                "resource_type": "metric",
                "name": "revenue",
                "package_name": "p",
                "depends_on": {"nodes": ["semantic_model.p.orders"]},
            },
        },
        "exposures": {
            "exposure.p.board": {
                "resource_type": "exposure",
                "name": "board",
                "package_name": "p",
                "depends_on": {"nodes": ["metric.p.revenue"]},
            },
        },
    }

    assert check(parse_manifest(manifest), "p") == [
        Violation(
            "board",
            "forbidden-dependency",
            "reads metric revenue; exposures read marts only",
        )
    ]


@pytest.mark.parametrize(
    "name",
    [
        "int_orders",
        "int_orders__final",
        "int_orders__new",
        "int_orders__totals_copy",
        "int_orders__totals_v2",
        "int_orders__totals_v1_2",
        "int_01_orders__totals",
        "int_orders__totals_table",
    ],
)
def test_descriptive_model_naming_is_reviewed_not_blocking(name: str) -> None:
    node = _model(name, f"models/intermediate/{name}.sql")
    assert check(parse_manifest({"nodes": {"model.p.x": node}}), "p") == []


@pytest.mark.parametrize(
    "name",
    [
        "int_new_customers__totals",
        "int_orders__new_customers",
        "fct_orders__daily",
        "dim_new_customers",
    ],
)
def test_objective_model_naming_accepts_meaningful_names(name: str) -> None:
    layer = "intermediate" if name.startswith("int_") else "marts"
    assert (
        check(
            parse_manifest(
                {"nodes": {"model.p.x": _model(name, f"models/{layer}/{name}.sql")}}
            ),
            "p",
        )
        == []
    )


def _physical_node(**fields: object) -> dict[str, object]:
    node = _model("fct_orders", "models/marts/fct_orders.sql")
    node.update(
        {"alias": "fct_orders", "schema": "ci_123_marts", "database": "WAREHOUSE"}
    )
    node.update(fields)
    return node


@pytest.mark.parametrize(
    "alias,version,valid",
    [
        ("fct_purchases", None, True),
        ("int_orders__totals", None, True),
        ("dim_orders", None, True),
        ("fct_orders_v2", None, True),
        ("fct_orders_v2", 2, True),
        ("fct_orders_v1_2", 1.2, True),
        ("fct_orders_v3", 2, True),
        ("fct_orders__daily_v2", 2, True),
        ("fct_Orders", None, True),
        ("fct_" + "a" * 251, None, True),
        ("fct_" + "a" * 252, None, False),
    ],
)
def test_alias_grammar_versions_and_portable_limits(
    alias: str, version: int | float | None, valid: bool
) -> None:
    unique_id = "model.p.fct_orders" + (f".v{version}" if version else "")
    node = _physical_node(alias=alias, version=version)
    violations = check(
        parse_manifest({"nodes": {unique_id: node}}), "p", namespace="ci_123"
    )
    assert (not violations) == valid


def test_version_must_match_manifest_identity() -> None:
    node = _physical_node(alias="fct_orders_v2", version=2)
    assert "model-version" in {
        v.rule
        for v in check(parse_manifest({"nodes": {"model.p.fct_orders.v3": node}}), "p")
    }


def test_physical_collisions_ignore_case_and_include_auxiliary_resources() -> None:
    nodes = {
        "model.p.orders": _physical_node(),
        "seed.p.orders": _physical_node(
            resource_type="seed",
            name="orders",
            alias="FCT_ORDERS",
            schema="CI_123_MARTS",
        ),
    }
    assert "identifier-collision" in {
        v.rule for v in check(parse_manifest({"nodes": nodes}), "p")
    }
    nodes["seed.p.orders"]["database"] = "other"
    assert "identifier-collision" not in {
        v.rule for v in check(parse_manifest({"nodes": nodes}), "p")
    }


@pytest.mark.parametrize(
    "kind,suffix",
    [
        ("model", "marts"),
        ("seed", "seeds"),
        ("snapshot", "snapshots"),
        ("test", "test_failures"),
    ],
)
def test_namespaces_route_persisted_resources(kind: str, suffix: str) -> None:
    node = _physical_node(
        resource_type=kind, schema=f"ci_123_{suffix}", config={"store_failures": True}
    )
    resources = parse_manifest({"nodes": {f"{kind}.p.x": node}})
    assert check(resources, "p", namespace="ci_123") == []
    assert "resource-schema" in {
        v.rule for v in check(resources, "p", namespace="developer")
    }


@pytest.mark.parametrize(
    "name,alias",
    [
        ("not_null_orders_ID", "not_null_orders_ID"),
        ("AuthoredName", "AuthoredAlias"),
        ("final", "copy"),
        ("not_null_" + "a" * 256, "dbt_shortened_name"),
    ],
)
def test_generic_test_names_and_aliases_are_exempt_from_style(
    name: str, alias: str
) -> None:
    node = _physical_node(
        resource_type="test",
        name=name,
        alias=alias,
        test_metadata={"name": "not_null", "kwargs": {"column_name": "ID"}},
        config={"store_failures": True},
        schema="ci_123_test_failures",
    )
    assert (
        check(parse_manifest({"nodes": {"test.p.x": node}}), "p", namespace="ci_123")
        == []
    )


@pytest.mark.parametrize("alias", ["a" * 256, "unsafe-name", "1bad", "bad.name"])
def test_generic_stored_failures_keep_portable_identifier_limits(alias: str) -> None:
    node = _physical_node(
        resource_type="test",
        test_metadata={"name": "not_null"},
        config={"store_failures_as": "table"},
        alias=alias,
    )
    assert "identifier" in {
        v.rule for v in check(parse_manifest({"nodes": {"test.p.x": node}}), "p")
    }


def test_generic_stored_failures_keep_schema_and_collision_checks() -> None:
    first = _physical_node(
        resource_type="test",
        name="FirstTest",
        alias="SharedFailure",
        schema="ci_123_test_failures",
        test_metadata={"name": "not_null"},
        config={"store_failures": True},
    )
    second = {**first, "name": "SecondTest", "alias": "sharedfailure"}
    resources = parse_manifest({"nodes": {"test.p.x": first, "test.p.y": second}})
    assert {v.rule for v in check(resources, "p", namespace="ci_123")} == {
        "identifier-collision"
    }
    assert {v.rule for v in check(resources, "p", namespace="other")} == {
        "identifier-collision",
        "resource-schema",
    }


@pytest.mark.parametrize("kind", ["test", "unit_test"])
@pytest.mark.parametrize("metadata", [None, {"name": "not_null"}])
def test_authored_test_names_are_reviewed_not_blocking(
    kind: str, metadata: dict[str, str] | None
) -> None:
    node = _physical_node(
        resource_type=kind,
        name="AuthoredName",
        alias="AuthoredAlias",
        test_metadata=metadata,
        config={"store_failures": True},
    )
    rules = {
        v.rule for v in check(parse_manifest({"nodes": {f"{kind}.p.x": node}}), "p")
    }
    assert rules == set()


def test_ephemeral_and_unstored_tests_do_not_collide_or_require_schemas() -> None:
    nodes = {
        "model.p.x": _physical_node(config={"materialized": "ephemeral"}),
        "test.p.x": _physical_node(resource_type="test"),
    }
    assert check(parse_manifest({"nodes": nodes}), "p", namespace="developer") == []
