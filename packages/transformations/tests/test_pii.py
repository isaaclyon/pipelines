"""Explicit classification is required; lineage never supplies a default."""

from typing import cast

import pytest

from analytics_transformations.pii import check_pii


def manifest(value: object) -> dict[str, object]:
    return {
        "metadata": {"project_name": "pii_fixture"},
        "nodes": {
            "model.pii_fixture.derived": {
                "resource_type": "model",
                "package_name": "pii_fixture",
                "name": "derived",
                "columns": {"customer_count": {"config": {"meta": {"pii": value}}}},
                "depends_on": {"nodes": ["source.pii_fixture.customers"]},
            }
        },
        "sources": {
            "source.pii_fixture.customers": {
                "resource_type": "source",
                "package_name": "pii_fixture",
                "columns": {"email": {"config": {"meta": {"pii": True}}}},
            }
        },
    }


@pytest.mark.parametrize("value", [None, "false", "true", 0, 1, [], {}])
def test_missing_or_non_boolean_declaration_fails(value: object) -> None:
    with pytest.raises(ValueError, match="derived.customer_count.*boolean"):
        check_pii(manifest(value))


def test_only_dbt_column_config_meta_declares_pii() -> None:
    data = manifest(False)
    model = cast(dict[str, dict[str, object]], data["nodes"])[
        "model.pii_fixture.derived"
    ]
    model["columns"] = {"customer_count": {"meta": {"pii": False}}}
    with pytest.raises(ValueError, match="customer_count.*boolean"):
        check_pii(data)


@pytest.mark.parametrize("value", [False, True])
def test_explicit_decision_is_not_inferred_from_upstream(value: bool) -> None:
    check_pii(manifest(value))


def test_disabled_sources_are_not_a_classification_escape() -> None:
    data = manifest(False)
    data["disabled"] = {
        "source.pii_fixture.disabled": [
            {
                "resource_type": "source",
                "package_name": "pii_fixture",
                "columns": {"email": {"description": "Email"}},
            }
        ]
    }
    with pytest.raises(ValueError, match="disabled.email"):
        check_pii(data)


def test_empty_column_inventory_fails() -> None:
    data = manifest(False)
    data["sources"] = {
        "source.pii_fixture.empty": {
            "resource_type": "source",
            "package_name": "pii_fixture",
            "columns": {},
        }
    }
    with pytest.raises(ValueError, match="empty.*columns"):
        check_pii(data)


def exposed(value: object) -> dict[str, object]:
    data = manifest(False)
    data["exposures"] = {
        "exposure.pii_fixture.customers": {
            "resource_type": "exposure",
            "package_name": "pii_fixture",
            "config": {
                "meta": {
                    "cube_fields": {"customers.count": ["derived.customer_count"]},
                    "cube_pii": {"customers.count": value},
                    "cube_direct_columns": {"customers.count": None},
                }
            },
        }
    }
    return data


def test_explicit_non_pii_exposure_passes() -> None:
    check_pii(exposed(False))


@pytest.mark.parametrize("value", [True, None, "false", 0])
def test_restricted_or_unclassified_cube_exposure_fails(value: object) -> None:
    with pytest.raises(ValueError, match="customers.count"):
        check_pii(exposed(value))


def test_exposure_requires_exact_field_classifications() -> None:
    data = manifest(False)
    data["exposures"] = {
        "exposure.pii_fixture.customers": {
            "resource_type": "exposure",
            "package_name": "pii_fixture",
            "config": {"meta": {"cube_fields": {"customers.count": []}}},
        }
    }
    with pytest.raises(ValueError, match="every Cube field"):
        check_pii(data)


def test_direct_cube_alias_cannot_relabel_a_restricted_mart_column() -> None:
    data = manifest(True)
    data["exposures"] = {
        "exposure.pii_fixture.customers": {
            "resource_type": "exposure",
            "package_name": "pii_fixture",
            "config": {
                "meta": {
                    "cube_fields": {"customers.value": ["derived.customer_count"]},
                    "cube_pii": {"customers.value": False},
                    "cube_direct_columns": {
                        "customers.value": "derived.customer_count",
                    },
                }
            },
        }
    }
    with pytest.raises(ValueError, match="customers.value.*classification"):
        check_pii(data)


def test_derived_cube_field_keeps_its_own_classification() -> None:
    data = exposed(False)
    data["nodes"] = manifest(True)["nodes"]
    check_pii(data)


def test_model_default_does_not_classify_a_new_field() -> None:
    data = manifest(False)
    data["nodes"] = {
        "model.pii_fixture.new": {
            "name": "new",
            "resource_type": "model",
            "package_name": "pii_fixture",
            "config": {"meta": {"pii": False}},
            "columns": {"email": {}},
        }
    }
    with pytest.raises(ValueError, match="new.email.*boolean"):
        check_pii(data)


def test_direct_alias_map_cannot_be_omitted() -> None:
    data = manifest(False)
    data["exposures"] = {
        "exposure.pii_fixture.customers": {
            "resource_type": "exposure",
            "package_name": "pii_fixture",
            "config": {
                "meta": {
                    "cube_fields": {"customers.count": []},
                    "cube_pii": {"customers.count": False},
                }
            },
        }
    }
    with pytest.raises(ValueError, match="cube_direct_columns"):
        check_pii(data)


def test_source_declaration_removal_cannot_reduce_reviewed_coverage() -> None:
    from pathlib import Path

    from analytics_transformations import pii
    from analytics_transformations.mart_contracts import (
        mapping,
        read_json,
        sequence,
        text,
    )

    inventory = mapping(read_json(Path(pii.__file__).with_name("source_fields.json")))
    sources: dict[str, object] = {
        key: {
            "resource_type": "source",
            "package_name": "analytics",
            "columns": {
                text(name): {"config": {"meta": {"pii": False}}}
                for name in sequence(columns)
            },
        }
        for key, columns in inventory.items()
    }
    data: dict[str, object] = {
        "metadata": {"project_name": "analytics"},
        "sources": sources,
    }
    check_pii(data)
    key = "source.analytics.shopify_orders.orders"
    source = mapping(sources[key])
    columns = mapping(source["columns"])
    del columns["email"]
    sources[key] = {**source, "columns": columns}
    with pytest.raises(ValueError, match="orders.*email.*inventory"):
        check_pii(data)
    del sources[key]
    with pytest.raises(ValueError, match="orders.*inventory"):
        check_pii(data)


def test_new_source_fields_must_join_reviewed_inventory() -> None:
    from pathlib import Path

    from analytics_transformations import pii
    from analytics_transformations.mart_contracts import (
        mapping,
        read_json,
        sequence,
        text,
    )

    inventory = mapping(read_json(Path(pii.__file__).with_name("source_fields.json")))
    sources: dict[str, object] = {
        key: {
            "resource_type": "source",
            "package_name": "analytics",
            "columns": {
                text(name): {"config": {"meta": {"pii": False}}}
                for name in sequence(columns)
            },
        }
        for key, columns in inventory.items()
    }
    key = "source.analytics.new_source.new_table"
    sources[key] = {
        "resource_type": "source",
        "package_name": "analytics",
        "columns": {"new_field": {"config": {"meta": {"pii": False}}}},
    }
    with pytest.raises(ValueError, match="new_field.*source_fields.json"):
        check_pii({"metadata": {"project_name": "analytics"}, "sources": sources})


def test_source_inventory_proposal_includes_disabled_sources_without_classifying() -> (
    None
):
    from analytics_transformations.pii import source_inventory

    data = manifest(None)
    data["disabled"] = {
        "source.pii_fixture.retired": [
            {
                "resource_type": "source",
                "package_name": "pii_fixture",
                "columns": {"phone": {}, "address": {}},
            }
        ],
        "source.other.ignored": [
            {
                "resource_type": "source",
                "package_name": "other",
                "columns": {"x": {}},
            }
        ],
    }
    assert source_inventory(data) == {
        "source.pii_fixture.customers": ["email"],
        "source.pii_fixture.retired": ["address", "phone"],
    }
    with pytest.raises(ValueError, match="boolean"):
        check_pii(data)
