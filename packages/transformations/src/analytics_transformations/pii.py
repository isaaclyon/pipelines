"""Require authored PII decisions, including disabled definitions.

This is a declaration check, not a detector or lineage inference engine.
There are no legacy exceptions: every declared column of a governed model,
source, seed or snapshot (``governed_projects``: the root project and the
toolkit package) carries a boolean ``config.meta.pii``.

``source_fields.json`` is the reviewed inventory of toolkit source fields,
keyed by the toolkit package, so it applies unchanged in a client project.
A client's own sources are outside it: the client declares their columns, each
with a decision, and reviews them in its repository. An undeclared column is
not loaded by any staging model, so the per-column rule covers what it reads.
"""

import json
import sys
from pathlib import Path

from analytics_transformations.layer_dependencies import (
    TOOLKIT_PROJECT,
    governed_projects,
)
from analytics_transformations.manifest_values import mapping, read_json, sequence, text


def check_pii(data: object) -> None:
    """Reject missing or non-boolean PII classifications."""
    manifest = mapping(data)
    projects = governed_projects(data)
    resources = {
        **mapping(manifest.get("nodes", {})),
        **mapping(manifest.get("sources", {})),
    }
    entries = list(resources.items())
    for key, values in mapping(manifest.get("disabled", {})).items():
        entries.extend((key, value) for value in sequence(values))
    if any(
        mapping(value).get("package_name") == TOOLKIT_PROJECT for _, value in entries
    ):
        inventory = mapping(read_json(Path(__file__).with_name("source_fields.json")))
        declared_sources = dict(entries)
        for key, names in inventory.items():
            source = mapping(declared_sources.get(key, {}))
            columns = mapping(source.get("columns", {}))
            missing = sorted({text(name) for name in sequence(names)} - columns.keys())
            if missing:
                raise ValueError(
                    f"{key}: {', '.join(missing)} missing from reviewed source "
                    "inventory; "
                    "retire fields explicitly in source_fields.json only after "
                    "updating ingestion and consumers"
                )
        for key, raw in entries:
            source = mapping(raw)
            if source.get("resource_type") != "source":
                continue
            if source.get("package_name") != TOOLKIT_PROJECT:
                continue
            extra = set(mapping(source.get("columns", {}))) - {
                text(name) for name in sequence(inventory.get(key, []))
            }
            if extra:
                raise ValueError(
                    f"{key}: add {', '.join(sorted(extra))} to source_fields.json "
                    "reviewed source inventory"
                )
    for key, value in entries:
        node = mapping(value)
        if node.get("package_name") not in projects:
            continue
        kind = node.get("resource_type")
        if kind in ("model", "source", "seed", "snapshot"):
            columns = mapping(node.get("columns", {}))
            if not columns:
                raise ValueError(f"{key}: declare columns and their PII status")
            for name, raw in columns.items():
                column = mapping(raw)
                config = mapping(column.get("config", {}))
                meta = mapping(config.get("meta", {}))
                if type(meta.get("pii")) is not bool:
                    raise ValueError(f"{key}.{name}: declare boolean config.meta.pii")


def source_inventory(data: object) -> dict[str, list[str]]:
    """Propose the toolkit's source-field inventory; never infer privacy or
    approve retirement."""
    manifest = mapping(data)
    entries = list(mapping(manifest.get("sources", {})).items())
    for key, values in mapping(manifest.get("disabled", {})).items():
        entries.extend((key, value) for value in sequence(values))
    inventory: dict[str, set[str]] = {}
    for key, raw in entries:
        node = mapping(raw)
        if (
            node.get("resource_type") == "source"
            and node.get("package_name") == TOOLKIT_PROJECT
        ):
            inventory.setdefault(key, set()).update(mapping(node.get("columns", {})))
    return {key: sorted(names) for key, names in sorted(inventory.items())}


if __name__ == "__main__":
    print(json.dumps(source_inventory(read_json(Path(sys.argv[1]))), indent=2))
