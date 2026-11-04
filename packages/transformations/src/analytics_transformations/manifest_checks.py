"""Run every parsed-manifest rule as one gate.

The package check runs it on each parsed manifest variant, and publication runs
it on the candidate's manifest, so both apply the same rules:

- ADR 0022 layers, names and physical schemas (``layer_dependencies``);
- published mart contracts (``mart_contracts``);
- staging and intermediate declarations and documentation
  (``model_declarations``);
- output column meanings (``column_declarations``);
- PII decisions and the reviewed source inventory (``pii``).

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
    parse_manifest,
    root_project,
)
from analytics_transformations.mart_contracts import (
    MartContract,
    contracts,
    read_json,
)
from analytics_transformations.model_declarations import (
    check_documentation,
    declarations,
)
from analytics_transformations.pii import check_pii


def check_manifest(data: object, *, namespace: str) -> tuple[MartContract, ...]:
    """Raise ValueError on the first failed rule; return the mart contracts."""
    violations = check(parse_manifest(data), root_project(data), namespace=namespace)
    if violations:
        raise ValueError("Naming/layer violations: " + "; ".join(map(str, violations)))
    plan = contracts(data)
    _ = declarations(data)
    check_column_declarations(data)
    check_documentation(data)
    check_pii(data)
    return plan


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit(
            "Usage: DBT_NAMESPACE=<namespace> "
            "python -m analytics_transformations.manifest_checks MANIFEST"
        )
    plan = check_manifest(
        read_json(Path(sys.argv[1])), namespace=os.environ["DBT_NAMESPACE"]
    )
    print(f"Manifest checks passed: {len(plan)} published mart(s)")


if __name__ == "__main__":
    main()
