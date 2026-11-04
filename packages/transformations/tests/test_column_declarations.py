"""Column declarations reject semantic gaps before candidate publication."""

import pytest

from analytics_transformations.column_declarations import (
    check_column_declarations,
    normalize_data_type,
)


def column(data_type: str, **meta: object) -> dict[str, object]:
    return {
        "data_type": data_type,
        "description": "What the value means.",
        "config": {"meta": meta},
    }


def manifest(columns: dict[str, object], layer: str = "marts") -> dict[str, object]:
    return {
        "nodes": {
            "model.p.orders": {
                "resource_type": "model",
                "name": "orders",
                "package_name": "p",
                "original_file_path": f"models/{layer}/orders.sql",
                "columns": columns,
            }
        },
        "metadata": {"project_name": "p"},
    }


def test_complete_meanings() -> None:
    check_column_declarations(
        manifest(
            {
                "shopify_order_id": column(
                    "bigint", identity={"kind": "source", "source": "shopify"}
                ),
                "customer_id": column(
                    "varchar",
                    identity={
                        "kind": "analytical",
                        "meaning": "Resolved customer across stores",
                    },
                ),
                "is_paid": column("boolean"),
                "order_count": column("bigint"),
                "weight_quantity": column("decimal(18,3)"),
                "processed_at": column(
                    "timestamp", temporal={"kind": "instant", "timezone": "UTC"}
                ),
                "order_date": column(
                    "date",
                    temporal={
                        "kind": "derived_date",
                        "instant": "processed_at",
                        "timezone": "America/Los_Angeles",
                    },
                ),
                "delivery_date": column("date", temporal={"kind": "source_date"}),
                "total_amount": column(
                    "decimal(38, 2)", currency={"column": "currency_code"}
                ),
                "currency_code": column("varchar"),
            }
        )
    )


@pytest.mark.parametrize(
    ("name", "declaration"),
    [
        ("order_count", column("varchar")),
        ("line_quantity", column("varchar")),
        ("BadName", column("varchar")),
        ("x" * 256, column("varchar")),
        ("processed", column("timestamp")),
        (
            "processed_at",
            column(
                "timestamp",
                temporal={"kind": "instant", "timezone": "America/Los_Angeles"},
            ),
        ),
        ("day", column("date")),
        ("order_date", column("date")),
        (
            "order_date",
            column(
                "date",
                temporal={
                    "kind": "derived_date",
                    "instant": "missing_at",
                    "timezone": "UTC",
                },
            ),
        ),
        ("total_amount", column("float", currency={"column": "currency_code"})),
        ("total_amount", column("decimal(38,2)")),
        ("total_amount", column("decimal(38,2)", currency={"column": "missing"})),
        ("order_id", column("bigint")),
        (
            "order_id",
            column("bigint", identity={"kind": "source", "source": "shopify"}),
        ),
        ("order_id", column("bigint", identity={"kind": "analytical", "meaning": ""})),
        ("value", {}),
        ("value", column("")),
    ],
)
def test_rejects_invalid_declarations(name: str, declaration: object) -> None:
    with pytest.raises(ValueError):
        check_column_declarations(manifest({name: declaration}))


@pytest.mark.parametrize(
    "data", [None, [], {}, {"nodes": []}, {"nodes": {"model.p.x": None}}]
)
def test_boundary_errors_are_value_errors(data: object) -> None:
    with pytest.raises(ValueError):
        check_column_declarations(data)


@pytest.mark.parametrize("description", [None, "", "  "])
def test_requires_column_description(description: str | None) -> None:
    declaration = column("varchar")
    if description is None:
        del declaration["description"]
    else:
        declaration["description"] = description
    with pytest.raises(ValueError, match="description"):
        check_column_declarations(manifest({"order_name": declaration}, "staging"))


def test_requires_nonempty_columns() -> None:
    with pytest.raises(ValueError):
        check_column_declarations(manifest({}))


def test_internal_ids_do_not_claim_analytical_identity() -> None:
    check_column_declarations(manifest({"order_id": column("bigint")}, "staging"))


@pytest.mark.parametrize(
    ("declared", "physical"),
    [
        ("number(38, 2)", "DECIMAL(38,2)"),
        ("timestamp_ntz", "TIMESTAMP"),
        ("text", "VARCHAR"),
        ("bool", "BOOLEAN"),
        ("int8", "BIGINT"),
    ],
)
def test_adapter_type_aliases(declared: str, physical: str) -> None:
    assert normalize_data_type(declared) == normalize_data_type(physical)


def parent_currency_manifest() -> dict[str, object]:
    parent: dict[str, object] = {
        "resource_type": "model",
        "name": "orders",
        "package_name": "p",
        "original_file_path": "models/staging/orders.sql",
        "columns": {"order_id": column("bigint"), "currency_code": column("varchar")},
    }
    child: dict[str, object] = {
        "resource_type": "model",
        "name": "lines",
        "package_name": "p",
        "original_file_path": "models/staging/lines.sql",
        "columns": {
            "order_id": column("bigint"),
            "line_amount": column(
                "decimal(38,2)",
                currency={
                    "model": "orders",
                    "join": {"order_id": "order_id"},
                    "column": "currency_code",
                    "test": "currencies_match",
                },
            ),
        },
    }
    test: dict[str, object] = {
        "resource_type": "test",
        "name": "currencies_match",
        "depends_on": {"nodes": ["model.p.lines", "model.p.orders"]},
        "config": {"severity": "error"},
    }
    return {
        "model.p.orders": parent,
        "model.p.lines": child,
        "test.p.currencies_match": test,
    }


def test_parent_currency_correctness_link() -> None:
    check_column_declarations(
        {"metadata": {"project_name": "p"}, "nodes": parent_currency_manifest()}
    )


@pytest.mark.parametrize(
    "change",
    ["missing", "unrelated", "warn", "key_only", "mart", "bad_join", "bad_currency"],
)
def test_parent_currency_requires_real_correctness_link(change: str) -> None:
    from analytics_transformations.mart_contracts import mapping

    nodes = parent_currency_manifest()
    test = mapping(nodes["test.p.currencies_match"])
    child = mapping(nodes["model.p.lines"])
    if change == "missing":
        del nodes["test.p.currencies_match"]
    elif change == "unrelated":
        test["depends_on"] = {"nodes": ["model.p.lines"]}
    elif change == "warn":
        test["config"] = {"severity": "warn"}
    elif change == "key_only":
        test["test_metadata"] = {"name": "not_null"}
    elif change == "mart":
        child["original_file_path"] = "models/marts/lines.sql"
    else:
        columns = mapping(child["columns"])
        columns["line_amount"] = column(
            "decimal(38,2)",
            currency={
                "model": "orders",
                "join": {"missing" if change == "bad_join" else "order_id": "order_id"},
                "column": "missing" if change == "bad_currency" else "currency_code",
                "test": "currencies_match",
            },
        )
        child["columns"] = columns
    if change != "missing":
        nodes["test.p.currencies_match"] = test
    nodes["model.p.lines"] = child
    with pytest.raises(ValueError):
        check_column_declarations({"metadata": {"project_name": "p"}, "nodes": nodes})


def test_root_project_must_be_named() -> None:
    with pytest.raises(ValueError, match="project_name"):
        check_column_declarations({"nodes": parent_currency_manifest()})


def test_unrelated_resources_and_package_models_are_ignored() -> None:
    check_column_declarations(
        {
            "metadata": {"project_name": "p"},
            "nodes": {
                "model.other.x": {"resource_type": "model", "package_name": "other"},
                "test.p.auto": {"resource_type": "test", "columns": {}},
                "source.p.raw": {"resource_type": "source", "columns": {}},
            },
        }
    )


@pytest.mark.parametrize("timezone", ["Mars/Olympus", "", 9])
def test_invalid_date_timezone(timezone: object) -> None:
    with pytest.raises(ValueError):
        check_column_declarations(
            manifest(
                {
                    "processed_at": column(
                        "timestamp", temporal={"kind": "instant", "timezone": "UTC"}
                    ),
                    "order_date": column(
                        "date",
                        temporal={
                            "kind": "derived_date",
                            "instant": "processed_at",
                            "timezone": timezone,
                        },
                    ),
                }
            )
        )


def test_internal_columns_need_no_repeated_type_or_temporal_declaration() -> None:
    check_column_declarations(
        manifest(
            {
                "label": {
                    "description": "Source label.",
                    "config": {"meta": {"pii": False}},
                },
                "processed_at": column("timestamp"),
                "order_date": column("date"),
            },
            "intermediate",
        )
    )


def test_column_style_does_not_block_correct_meanings() -> None:
    check_column_declarations(
        manifest(
            {
                "paid": column("boolean"),
                "bad__name": column("varchar"),
                "trailing_": column("varchar"),
                "processed": column(
                    "timestamp", temporal={"kind": "instant", "timezone": "UTC"}
                ),
                "day": column("date", temporal={"kind": "source_date"}),
                "count": column("bigint"),
            }
        )
    )


def test_bare_published_id_still_requires_identity_meaning() -> None:
    with pytest.raises(ValueError, match="identities require explicit"):
        check_column_declarations(manifest({"id": column("bigint")}))
    check_column_declarations(
        manifest(
            {
                "id": column(
                    "bigint",
                    identity={
                        "kind": "analytical",
                        "meaning": "Stable analytical identity",
                    },
                )
            }
        )
    )
