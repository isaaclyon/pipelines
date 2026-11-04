#!/bin/sh
set -eu

uv pip check
ruff format --check src tests
ruff check src tests
basedpyright
pytest
dg check toml
dg check defs

# Verify that strict typing and the package boundary remain enforced.
probe=$(mktemp ./check-probe-XXXXXX.py)
trap 'rm -f "$probe" "$probe.log"' EXIT HUP INT TERM
cat > "$probe" <<'PY'
from typing import Any

def unsafe(value: Any) -> int:
    return value.missing

def unknown(value):
    return value
PY
if basedpyright --project pyproject.toml "$probe" > "$probe.log" 2>&1; then
    echo "Unsafe typing probe unexpectedly passed" >&2
    exit 1
fi
grep -q reportExplicitAny "$probe.log"
grep -q reportAny "$probe.log"
grep -q reportUnknownParameterType "$probe.log"

printf 'import %s\n' "${FORBIDDEN_IMPORT:?Set the sibling import to test}" > "$probe"
if ruff check "$probe" > "$probe.log" 2>&1; then
    echo "Undeclared sibling import unexpectedly passed" >&2
    exit 1
fi
grep -q TID251 "$probe.log"

if [ -d dbt ]; then
    uv pip check --python "$(dirname "${COLIBRI_EXECUTABLE:?Set the isolated lineage CLI}")/python"
    dbt --version
    # Parsing resolves model schemas (namespace) and the sqlfluff dbt templater
    # opens the local target, so both steps share these settings.
    export DBT_NAMESPACE=check DBT_TARGET=local DUCKDB_PATH=/tmp/check.duckdb
    dbt parse --project-dir dbt --profiles-dir dbt --quiet
    python -m analytics_transformations.layer_dependencies dbt/target/manifest.json
    python -m analytics_transformations.mart_contracts dbt/target/manifest.json dbt/mart-contracts.json
    python -m analytics_transformations.model_declarations dbt/target/manifest.json
    # Pull requests lint only the model SQL that differs from the base, unless
    # the macros that template every model changed (ADR 0026). Configuration
    # changes already select every check.
    if [ "${DBT_CHECKS:-all}" = changed ] &&
        diff -rq dbt/macros "${DBT_BASE_PROJECT:?Set the base project}/dbt/macros" >/dev/null; then
        set --
        for model in $(find dbt/models -name '*.sql' | sort); do
            cmp -s "$model" "$DBT_BASE_PROJECT/$model" || set -- "$@" "$model"
        done
        if [ $# -gt 0 ]; then
            sqlfluff lint "$@"
        fi
    else
        sqlfluff lint dbt/models
    fi
    # Customer models are disabled by default; check their declarations and
    # descriptions too.
    SHOPIFY_CUSTOMERS_ENABLED=true dbt parse --project-dir dbt --profiles-dir dbt \
        --quiet --target-path target/customers
    python -m analytics_transformations.model_declarations \
        dbt/target/customers/manifest.json
    # Local-only governed measures have additional exposure column references.
    for customers in false true; do
        ANALYTICS_ENVIRONMENT=local GOVERNED_ORDER_METRICS_ENABLED=true \
            SHOPIFY_CUSTOMERS_ENABLED=$customers dbt parse --project-dir dbt --profiles-dir dbt \
            --quiet --target-path "target/local-governed-$customers"
        python -m analytics_transformations.model_declarations \
            "dbt/target/local-governed-$customers/manifest.json"
        python tests/check_local_exposures.py "dbt/target/local-governed-$customers/manifest.json"
    done
fi
