"""Run every parsed-manifest rule as one gate.

The package check runs it on each parsed manifest variant:

- ADR 0022 layers, names and physical schemas (``layer_dependencies``);
- model declarations and documentation (``model_declarations``);
- output column meanings (``column_declarations``);
- PII decisions and the reviewed source inventory (``pii``).

Each rule governs the root project, which is the toolkit's or a client's dbt
project, and the toolkit package it installs (``governed_projects``).

Run after ``dbt parse``, with the namespace the manifest was parsed with:

    DBT_NAMESPACE=check python -m analytics_transformations.manifest_checks \
        dbt/target/manifest.json
"""

import os
import sys
from pathlib import Path

from analytics_transformations.column_declarations import check_column_declarations
from analytics_transformations.layer_dependencies import (
    check,
    governed_projects,
    parse_manifest,
)
from analytics_transformations.manifest_values import read_json
from analytics_transformations.model_declarations import (
    check_documentation,
    declarations,
)
from analytics_transformations.pii import check_pii


def check_manifest(data: object, *, namespace: str) -> None:
    """Raise ValueError on the first failed rule."""
    violations = check(
        parse_manifest(data), governed_projects(data), namespace=namespace
    )
    if violations:
        raise ValueError("Naming/layer violations: " + "; ".join(map(str, violations)))
    _ = declarations(data)
    check_column_declarations(data)
    check_documentation(data)
    check_pii(data)


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit(
            "Usage: DBT_NAMESPACE=<namespace> "
            "python -m analytics_transformations.manifest_checks MANIFEST"
        )
    check_manifest(read_json(Path(sys.argv[1])), namespace=os.environ["DBT_NAMESPACE"])
    print("Manifest checks passed")


if __name__ == "__main__":
    main()
