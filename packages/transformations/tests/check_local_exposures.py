"""Probe the actual local-enabled manifest after each declaration-check parse."""

import sys
from pathlib import Path

from analytics_transformations.mart_contracts import mapping, read_json
from analytics_transformations.model_declarations import check_exposures


def check_missing_measure_dependency(path: Path) -> None:
    manifest = mapping(read_json(path))
    nodes = mapping(manifest.get("nodes"))
    mart_id, mart = next(
        (identifier, mapping(value))
        for identifier, value in nodes.items()
        if mapping(value).get("name") == "fct_order_lines"
    )
    # This column is consumed by local-only line measures, not a Cube dimension.
    # Mutate memory only; never the parsed artifact or authored declarations.
    columns = mapping(mart.get("columns"))
    columns.pop("net_ordered_quantity")
    mart["columns"] = columns
    nodes[mart_id] = mart
    manifest["nodes"] = nodes
    try:
        check_exposures(manifest)
    except ValueError as error:
        if "fct_order_lines has no column net_ordered_quantity" not in str(error):
            raise
    else:
        raise AssertionError(
            "Local-only measure dependency escaped the declaration gate"
        )


if __name__ == "__main__":
    check_missing_measure_dependency(Path(sys.argv[1]))
    print("Local-enabled measure dependency refusal passed")
