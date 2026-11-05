"""Explicit classification is required; lineage never supplies a default."""

from pathlib import Path
from typing import cast

import pytest

from analytics_transformations import pii
from analytics_transformations.manifest_values import (
    mapping,
    read_json,
    sequence,
    text,
)
from analytics_transformations.pii import check_pii, source_inventory

TOOLKIT = "analytics_toolkit"


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


def _toolkit_sources() -> dict[str, object]:
    """Every reviewed toolkit source, as the toolkit package declares it."""
    inventory = mapping(read_json(Path(pii.__file__).with_name("source_fields.json")))
    return {
        key: {
            "resource_type": "source",
            "package_name": TOOLKIT,
            "columns": {
                text(name): {"config": {"meta": {"pii": False}}}
                for name in sequence(columns)
            },
        }
        for key, columns in inventory.items()
    }


# The toolkit's own project, and a client project that installs it.
ROOTS = pytest.mark.parametrize("root", [TOOLKIT, "client"])


@ROOTS
def test_source_declaration_removal_cannot_reduce_reviewed_coverage(
    root: str,
) -> None:
    sources = _toolkit_sources()
    data: dict[str, object] = {"metadata": {"project_name": root}, "sources": sources}
    check_pii(data)
    key = "source.analytics_toolkit.shopify_orders.orders"
    source = mapping(sources[key])
    columns = mapping(source["columns"])
    del columns["email"]
    sources[key] = {**source, "columns": columns}
    with pytest.raises(ValueError, match="orders.*email.*inventory"):
        check_pii(data)
    del sources[key]
    with pytest.raises(ValueError, match="orders.*inventory"):
        check_pii(data)


@ROOTS
def test_new_source_fields_must_join_reviewed_inventory(root: str) -> None:
    sources = _toolkit_sources()
    key = "source.analytics_toolkit.new_source.new_table"
    sources[key] = {
        "resource_type": "source",
        "package_name": TOOLKIT,
        "columns": {"new_field": {"config": {"meta": {"pii": False}}}},
    }
    with pytest.raises(ValueError, match="new_field.*source_fields.json"):
        check_pii({"metadata": {"project_name": root}, "sources": sources})


def test_client_sources_declare_pii_per_column_outside_the_inventory() -> None:
    # The inventory reviews toolkit sources. A client's own source needs a
    # boolean decision on every declared column, reviewed in its repository.
    sources = _toolkit_sources()
    columns: dict[str, object] = {"target": {"config": {"meta": {"pii": False}}}}
    sources["source.client.sheets.targets"] = {
        "resource_type": "source",
        "package_name": "client",
        "columns": columns,
    }
    data = {"metadata": {"project_name": "client"}, "sources": sources}
    check_pii(data)
    columns["owner_email"] = {"description": "Owner"}
    with pytest.raises(ValueError, match="targets.owner_email.*boolean"):
        check_pii(data)
    columns.clear()
    with pytest.raises(ValueError, match="targets.*declare columns"):
        check_pii(data)


def test_client_models_declare_pii_beside_toolkit_models() -> None:
    data = manifest(None)
    data["metadata"] = {"project_name": "client"}
    model = cast(dict[str, dict[str, object]], data["nodes"])[
        "model.pii_fixture.derived"
    ]
    model["package_name"] = "client"
    data["sources"] = _toolkit_sources()
    with pytest.raises(ValueError, match="derived.customer_count.*boolean"):
        check_pii(data)


def test_source_inventory_proposes_toolkit_sources_including_disabled() -> None:
    data = manifest(None)
    data["metadata"] = {"project_name": "client"}
    data["disabled"] = {
        "source.analytics_toolkit.shop.retired": [
            {
                "resource_type": "source",
                "package_name": TOOLKIT,
                "columns": {"phone": {}, "address": {}},
            }
        ],
        "source.client.sheets.ignored": [
            {
                "resource_type": "source",
                "package_name": "client",
                "columns": {"x": {}},
            }
        ],
    }
    assert source_inventory(data) == {
        "source.analytics_toolkit.shop.retired": ["address", "phone"],
    }
