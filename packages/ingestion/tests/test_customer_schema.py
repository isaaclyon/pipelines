"""The committed raw customer contract; transformations mirror this fixture."""

from __future__ import annotations

import json
from pathlib import Path

from analytics_ingestion.shopify import customer_model as model

FIXTURE = Path(__file__).parent / "fixtures" / "shopify_customer_raw_schema.json"


def current() -> dict[str, list[list[object]]]:
    tables = {
        model.ROOT_TABLE: model.CUSTOMER_COLUMNS,
        **{f"customers__{'__'.join(p)}": c for p, c in model.NESTED_TABLES.items()},
        model.REJECTS_TABLE: model.REJECT_COLUMNS,
        model.EVIDENCE_TABLE: model.EVIDENCE_COLUMNS,
        model.ACCESS_TABLE: model.ACCESS_COLUMNS,
        model.INVENTORY_TABLE: model.INVENTORY_COLUMNS,
        model.STATUS_TABLE: model.STATUS_COLUMNS,
    }
    return {
        name: [[c.name, c.data_type, c.nullable] for c in columns]
        for name, columns in tables.items()
    }


def test_contract_matches_the_committed_fixture() -> None:
    assert current() == json.loads(FIXTURE.read_text())


def test_every_approved_group_is_declared() -> None:
    assert set(model.FIELD_GROUPS) >= {
        "names",
        "email",
        "phone",
        "addresses",
        "summaries",
        "statistics",
        "tags",
        "metafields",
        "tax_id",
        "company_contacts",
        "external_identities",
    }
    assert "external_identities" in model.EXCLUDED_GROUPS


def test_child_tables_key_on_shop_and_customer() -> None:
    for columns in model.NESTED_TABLES.values():
        names = [c.name for c in columns[:2]]
        assert names == ["shop_id", "customer_id"]
