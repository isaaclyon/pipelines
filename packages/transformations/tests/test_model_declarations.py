"""Model declarations for every model, and test blocking."""

from typing import cast

import pytest

from analytics_transformations.model_declarations import (
    check_documentation,
    declarations,
)

MODEL = "model.analytics.stg_shop__orders"


def _test(
    name: str,
    kind: str | None,
    column: str | None = None,
    *,
    severity: str = "ERROR",
    blocking: object = True,
    model: str = MODEL,
) -> dict[str, object]:
    meta: dict[str, object] = {} if blocking is None else {"blocking": blocking}
    node: dict[str, object] = {
        "resource_type": "test",
        "package_name": "analytics",
        "name": name,
        "depends_on": {"nodes": [model]},
        "config": {"severity": severity, "meta": meta},
    }
    if kind is not None:
        node["test_metadata"] = {"name": kind, "kwargs": {"column_name": column}}
    return node


def manifest(
    meta: dict[str, object] | None = None, *tests: dict[str, object]
) -> dict[str, object]:
    declared: dict[str, object] = (
        {
            "owner": "Analytics Platform",
            "grain": "One row per order",
            "key": ["order_id"],
        }
        if meta is None
        else meta
    )
    if not tests:
        tests = (
            _test("not_null_order_id", "not_null", "order_id"),
            _test("unique_order_id", "unique", "order_id"),
        )
    return {
        "metadata": {"project_name": "analytics"},
        "nodes": {
            MODEL: {
                "resource_type": "model",
                "package_name": "analytics",
                "name": "stg_shop__orders",
                "original_file_path": "models/staging/shop/stg_shop__orders.sql",
                "description": "Shop orders, one row per order.",
                "config": {"meta": declared},
                "columns": {
                    "order_id": {"description": "Order identity."},
                    "shop_id": {"description": "Shop identity."},
                },
            },
            **{f"test.analytics.{t['name']}": t for t in tests},
        },
    }


def test_declared_single_key_with_blocking_tests_passes() -> None:
    [declaration] = declarations(manifest())
    assert declaration.key == ("order_id",)
    assert declaration.grain == "One row per order"


def _unique_combination(name: str, *columns: str) -> dict[str, object]:
    node = _test(name, None)
    node["test_metadata"] = {
        "name": "unique_combination",
        "kwargs": {"columns": list(columns)},
    }
    return node


def test_composite_key_requires_unique_check_over_the_key() -> None:
    meta: dict[str, object] = {
        "owner": "Analytics Platform",
        "grain": "One row per order",
        "key": ["shop_id", "order_id"],
    }
    tests = (
        _test("not_null_shop_id", "not_null", "shop_id"),
        _test("not_null_order_id", "not_null", "order_id"),
    )
    for unique_check in (
        None,
        _test("orders_unique", "not_null", "order_id"),
        _test("orders_unique", None),
        _unique_combination("orders_unique", "order_id"),
    ):
        extra = () if unique_check is None else (unique_check,)
        with pytest.raises(ValueError, match="unique"):
            declarations(manifest(meta, *tests, *extra))
    [declaration] = declarations(
        manifest(
            meta, *tests, _unique_combination("orders_unique", "order_id", "shop_id")
        )
    )
    assert declaration.key == ("shop_id", "order_id")


def test_reason_replaces_key_and_its_tests() -> None:
    meta: dict[str, object] = {
        "owner": "Analytics Platform",
        "grain": "One row per order observation",
        "no_unique_key_reason": "Observations repeat until deduplicated downstream.",
    }
    [declaration] = declarations(manifest(meta, _test("any_check", None)))
    assert declaration.key == ()


@pytest.mark.parametrize(
    "meta",
    [
        {"grain": "One row per order", "key": ["order_id"]},
        {"owner": "Analytics Platform", "key": ["order_id"]},
        {"owner": "Analytics Platform", "grain": "One row per order"},
        {
            "owner": "Analytics Platform",
            "grain": "One row per order",
            "key": ["order_id"],
            "no_unique_key_reason": "Both is ambiguous.",
        },
        {
            "owner": "Analytics Platform",
            "grain": "One row per order",
            "key": ["missing"],
        },
    ],
)
def test_incomplete_declaration_fails(meta: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        declarations(manifest(meta))


def test_declared_key_requires_blocking_key_tests() -> None:
    with pytest.raises(ValueError, match="not-null"):
        declarations(manifest(None, _test("unique_order_id", "unique", "order_id")))
    with pytest.raises(ValueError, match="unique"):
        declarations(manifest(None, _test("not_null_order_id", "not_null", "order_id")))
    warned = _test(
        "unique_order_id", "unique", "order_id", severity="WARN", blocking=False
    )
    with pytest.raises(ValueError, match="unique"):
        declarations(
            manifest(None, _test("not_null_order_id", "not_null", "order_id"), warned)
        )


@pytest.mark.parametrize(
    ("severity", "blocking"),
    [("ERROR", None), ("ERROR", False), ("WARN", True), ("WARN", None)],
)
def test_obsolete_blocking_metadata_does_not_override_severity(
    severity: str, blocking: object
) -> None:
    extra = _test("other_check", None, severity=severity, blocking=blocking)
    assert declarations(manifest(None, *manifest_tests(), extra))


def test_warning_that_does_not_block_is_allowed() -> None:
    extra = _test("advisory_check", None, severity="WARN", blocking=False)
    assert declarations(manifest(None, *manifest_tests(), extra))


def test_marts_need_declarations() -> None:
    value = manifest()
    nodes = cast(dict[str, dict[str, object]], value["nodes"])
    nodes["model.analytics.fct_orders"] = {
        "resource_type": "model",
        "package_name": "analytics",
        "original_file_path": "models/marts/fct_orders.sql",
        "config": {"meta": {}},
    }
    with pytest.raises(ValueError, match="fct_orders: declare meta owner and grain"):
        _ = declarations(value)


def test_package_resources_are_out_of_scope() -> None:
    value = manifest()
    nodes = cast(dict[str, dict[str, object]], value["nodes"])
    nodes["model.other.stg_other__rows"] = {
        "resource_type": "model",
        "package_name": "other",
        "original_file_path": "models/staging/stg_other__rows.sql",
        "config": {"meta": {}},
    }
    nodes["test.other.check"] = _test("check", None, blocking=None, model="x")
    nodes["test.other.check"]["package_name"] = "other"
    assert len(declarations(value)) == 1


def manifest_tests() -> tuple[dict[str, object], ...]:
    return (
        _test("not_null_order_id", "not_null", "order_id"),
        _test("unique_order_id", "unique", "order_id"),
    )


def documented() -> dict[str, object]:
    value = manifest()
    nodes = cast(dict[str, dict[str, object]], value["nodes"])
    nodes[MODEL]["config"] = {
        "schema": "staging",
        "meta": {"schema_description": "Source-shaped staging views."},
    }
    value["sources"] = {
        "source.analytics.shop.orders": {
            "resource_type": "source",
            "package_name": "analytics",
            "source_description": "Raw shop data loaded by ingestion.",
            "description": "One row per raw order.",
        },
        "source.other.shop.orders": {"package_name": "other"},
    }
    return value


def test_documented_models_and_sources_pass() -> None:
    check_documentation(documented())


@pytest.mark.parametrize(
    ("path", "message"),
    [
        (("nodes", MODEL, "description"), "model description"),
        (("nodes", MODEL, "config", "meta", "schema_description"), "schema"),
        (
            ("sources", "source.analytics.shop.orders", "source_description"),
            "source description",
        ),
        (("sources", "source.analytics.shop.orders", "description"), "table"),
    ],
)
@pytest.mark.parametrize("blank", [None, " "])
def test_missing_documentation_fails(
    path: tuple[str, ...], message: str, blank: str | None
) -> None:
    value = documented()
    parent = value
    for key in path[:-1]:
        parent = cast(dict[str, object], parent[key])
    if blank is None:
        del parent[path[-1]]
    else:
        parent[path[-1]] = blank
    with pytest.raises(ValueError, match=message):
        check_documentation(value)


def test_one_description_per_schema() -> None:
    value = documented()
    nodes = cast(dict[str, dict[str, object]], value["nodes"])
    other = dict(nodes[MODEL])
    other["config"] = {
        "schema": "staging",
        "meta": {"schema_description": "A different meaning."},
    }
    nodes["model.analytics.stg_shop__lines"] = other
    with pytest.raises(ValueError, match="one schema_description"):
        check_documentation(value)
