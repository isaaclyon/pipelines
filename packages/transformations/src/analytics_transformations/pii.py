"""Require authored PII decisions, including disabled definitions.

This is a declaration check, not a detector or lineage inference engine.
There are no legacy exceptions: every declared model/source column is covered.
"""

import json
import sys
from pathlib import Path

from analytics_transformations.layer_dependencies import root_project
from analytics_transformations.mart_contracts import mapping, read_json, sequence, text


def check_pii(data: object) -> None:
    """Reject missing/non-boolean classifications and exposed restricted fields."""
    manifest = mapping(data)
    project = root_project(data)
    resources = {
        **mapping(manifest.get("nodes", {})),
        **mapping(manifest.get("sources", {})),
        **mapping(manifest.get("exposures", {})),
    }
    entries = list(resources.items())
    for key, values in mapping(manifest.get("disabled", {})).items():
        entries.extend((key, value) for value in sequence(values))
    if project == "analytics":
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
            if source.get("package_name") != project:
                continue
            extra = set(mapping(source.get("columns", {}))) - {
                text(name) for name in sequence(inventory.get(key, []))
            }
            if extra:
                raise ValueError(
                    f"{key}: add {', '.join(sorted(extra))} to source_fields.json "
                    "reviewed source inventory"
                )
    models = {
        text(node["name"]): node
        for _, raw in entries
        if (node := mapping(raw)).get("resource_type") == "model"
        and node.get("package_name") == project
    }
    for key, value in entries:
        node = mapping(value)
        if node.get("package_name") != project:
            continue
        kind = node.get("resource_type")
        if kind in ("model", "source", "seed", "snapshot"):
            columns = mapping(node.get("columns", {}))
            if not columns:
                raise ValueError(f"{key}: declare columns and their PII status")
            for name, raw in columns.items():
                column = mapping(raw)
                config = mapping(column.get("config", {}))
                meta = mapping(config.get("meta", column.get("meta", {})))
                if type(meta.get("pii")) is not bool:
                    raise ValueError(f"{key}.{name}: declare boolean config.meta.pii")
        elif kind == "exposure":
            meta = mapping(mapping(node.get("config", {})).get("meta", {}))
            fields = mapping(meta.get("cube_fields", {}))
            if not fields:
                continue
            classifications = mapping(meta.get("cube_pii", {}))
            if fields.keys() != classifications.keys():
                raise ValueError(f"{key}: declare cube_pii for every Cube field")
            for name, pii in classifications.items():
                if type(pii) is not bool:
                    raise ValueError(f"{name}: declare boolean PII status")
                if pii:
                    raise ValueError(
                        f"{name}: restricted PII cannot be exposed in Cube"
                    )
            direct = mapping(meta.get("cube_direct_columns", {}))
            if fields.keys() != direct.keys():
                raise ValueError(
                    f"{key}: declare cube_direct_columns for every Cube field"
                )
            # Direct aliases name the same field, not a derived classification.
            # Aggregates and expressions keep their independent author decision.
            for name, reference in direct.items():
                if reference is None:
                    continue
                model, _, column = text(reference).partition(".")
                declared = mapping(mapping(models.get(model, {})).get("columns", {}))
                field = mapping(declared.get(column, {}))
                config = mapping(field.get("config", {}))
                source_meta = mapping(config.get("meta", field.get("meta", {})))
                if (
                    name not in classifications
                    or source_meta.get("pii") is not classifications[name]
                ):
                    raise ValueError(
                        f"{name}: direct column classification differs from {reference}"
                    )


def source_inventory(data: object) -> dict[str, list[str]]:
    """Propose a source-field inventory; never infer privacy or approve retirement."""
    manifest = mapping(data)
    project = root_project(data)
    entries = list(mapping(manifest.get("sources", {})).items())
    for key, values in mapping(manifest.get("disabled", {})).items():
        entries.extend((key, value) for value in sequence(values))
    inventory: dict[str, set[str]] = {}
    for key, raw in entries:
        node = mapping(raw)
        if (
            node.get("resource_type") == "source"
            and node.get("package_name") == project
        ):
            inventory.setdefault(key, set()).update(mapping(node.get("columns", {})))
    return {key: sorted(names) for key, names in sorted(inventory.items())}


if __name__ == "__main__":
    print(json.dumps(source_inventory(read_json(Path(sys.argv[1]))), indent=2))
