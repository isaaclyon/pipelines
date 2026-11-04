"""Consecutive releases stay compatible with each other's data (ADR 0020).

The raw schema snapshot records the columns that earlier releases created and
that readers may depend on. Compatible changes (new nullable columns, new tables,
or a required column becoming nullable) still require a deliberate snapshot
update so later changes are compared against them:

    UPDATE_RAW_SCHEMA_SNAPSHOT=1 uv run pytest tests/test_shopify_schema.py

Removing a column belongs to the contract step after readers have moved; edit
the snapshot by hand in that change.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import cast

from analytics_ingestion.shopify.model import (
    BATCH_EVIDENCE_COLUMNS,
    BATCH_EVIDENCE_TABLE,
    NESTED_TABLES,
    ORDER_COLUMNS,
    ROOT_TABLE,
    SOURCE_STATUS_COLUMNS,
    SOURCE_STATUS_TABLE,
)
from analytics_ingestion.shopify.pipeline import column_hints

SNAPSHOT = Path(__file__).parent / "fixtures" / "shopify_raw_schema.json"
Schema = dict[str, dict[str, dict[str, object]]]


def declared_schema() -> Schema:
    tables = {
        ROOT_TABLE: ORDER_COLUMNS,
        SOURCE_STATUS_TABLE: SOURCE_STATUS_COLUMNS,
        BATCH_EVIDENCE_TABLE: BATCH_EVIDENCE_COLUMNS,
        **{
            "__".join((ROOT_TABLE, *path)): cols for path, cols in NESTED_TABLES.items()
        },
    }
    return {
        table: {
            name: {key: value for key, value in hint.items() if key != "name"}
            for name, hint in column_hints(columns).items()
        }
        for table, columns in sorted(tables.items())
    }


def breaking_changes(previous: Schema, current: Schema) -> list[str]:
    problems: list[str] = []
    for table, columns in previous.items():
        if table not in current:
            problems.append(f"{table}: table removed")
            continue
        for name, hint in columns.items():
            now = current[table].get(name)
            if now is None:
                problems.append(f"{table}.{name}: column removed or renamed")
                continue
            for key in sorted((set(hint) | set(now)) - {"nullable"}):
                if hint.get(key) != now.get(key):
                    problems.append(
                        f"{table}.{name}: {key} changed from "
                        f"{hint.get(key)} to {now.get(key)}"
                    )
            if hint["nullable"] and not now["nullable"]:
                problems.append(f"{table}.{name}: became required")
        for name, hint in current[table].items():
            if name not in columns and not hint["nullable"]:
                problems.append(f"{table}.{name}: new column must be nullable")
    return problems


def test_raw_schema_changes_are_backward_compatible() -> None:
    previous = cast(Schema, json.loads(SNAPSHOT.read_text()))
    current = declared_schema()

    assert breaking_changes(previous, current) == []
    if os.environ.get("UPDATE_RAW_SCHEMA_SNAPSHOT") == "1":
        _ = SNAPSHOT.write_text(json.dumps(current, indent=2, sort_keys=True) + "\n")
        previous = current
    assert previous == current, (
        "Compatible raw schema change: rerun with UPDATE_RAW_SCHEMA_SNAPSHOT=1"
    )


def test_breaking_changes_are_reported() -> None:
    previous: Schema = {
        "orders": {
            "kept": {"data_type": "text", "nullable": True},
            "dropped": {"data_type": "text", "nullable": True},
            "amount": {
                "data_type": "decimal",
                "nullable": False,
                "precision": 38,
                "scale": 9,
            },
        },
        "gone": {},
    }
    current: Schema = {
        "orders": {
            "kept": {"data_type": "text", "nullable": False},
            "amount": {
                "data_type": "decimal",
                "nullable": True,
                "precision": 38,
                "scale": 6,
            },
            "added": {"data_type": "text", "nullable": False},
            "optional": {"data_type": "text", "nullable": True},
        },
        "new_table": {"id": {"data_type": "text", "nullable": False}},
    }

    assert breaking_changes(previous, current) == [
        "orders.kept: became required",
        "orders.dropped: column removed or renamed",
        "orders.amount: scale changed from 9 to 6",
        "orders.added: new column must be nullable",
        "gone: table removed",
    ]
