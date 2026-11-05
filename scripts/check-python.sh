#!/bin/sh
set -eu

uv pip check
# Dagster definitions read the client configuration at load; each Dagster
# project keeps a check copy beside its tests.
if [ -z "${ANALYTICS_CLIENT_CONFIG:-}" ] && [ -f tests/client.yaml ]; then
    export ANALYTICS_CLIENT_CONFIG="$PWD/tests/client.yaml"
fi
ruff format --check src tests
ruff check src tests
basedpyright
pytest
# Dagster projects declare [tool.dg]; the client configuration library does not.
if grep -q '^\[tool\.dg\]' pyproject.toml; then
    dg check toml
    dg check defs
fi

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
    # opens the local target, so both steps share these settings. dbt reads
    # the client's sources and time zone only through analytics-dbt, which
    # derives them from ANALYTICS_CLIENT_CONFIG (the check client above).
    export DBT_NAMESPACE=check DBT_TARGET=local DUCKDB_PATH=/tmp/check.duckdb
    analytics-dbt parse --project-dir dbt --profiles-dir dbt --quiet
    python -m analytics_transformations.manifest_checks dbt/target/manifest.json
    # A client that enables every source, so the manifest checks and the lint
    # see every model.
    all_sources="$PWD/tests/client-all-sources.yaml"
    ANALYTICS_CLIENT_CONFIG="$all_sources" analytics-dbt parse --project-dir dbt \
        --profiles-dir dbt --quiet --target-path target/all-sources
    python -m analytics_transformations.manifest_checks \
        dbt/target/all-sources/manifest.json
    # Pull requests lint only the model SQL that differs from the base, unless
    # the macros that template every model changed (ADR 0026). Configuration
    # changes already select every check.
    if [ "${DBT_CHECKS:-all}" = changed ] &&
        diff -rq dbt/macros "${DBT_BASE_PROJECT:?Set the base project}/dbt/macros" >/dev/null; then
        set --
        for model in $(find dbt/models -name '*.sql' | sort); do
            cmp -s "$model" "$DBT_BASE_PROJECT/$model" || set -- "$@" "$model"
        done
    else
        set -- dbt/models
    fi
    if [ $# -gt 0 ]; then
        ANALYTICS_CLIENT_CONFIG="$all_sources" \
            python -m analytics_transformations.client_dbt sqlfluff lint "$@"
    fi
fi
