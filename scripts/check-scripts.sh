#!/bin/sh
# Lint, format-check and strictly type-check the shared Python scripts with the
# same pinned tools as the packages. GitHub runners provide pipx; hosts with
# uv can use uvx instead.
set -eu

pinned() {
    tool=$1 version=$2
    shift 2
    if command -v uvx >/dev/null 2>&1; then
        uvx --from "$tool==$version" "$tool" "$@"
    else
        pipx run --spec "$tool==$version" "$tool" "$@"
    fi
}

# Take the versions from the ingestion package so the pins cannot drift.
version() {
    sed -n "s/.*\"$1==\([^\"]*\)\".*/\1/p" packages/ingestion/pyproject.toml
}
ruff_version=$(version ruff)
pinned ruff "$ruff_version" check scripts
pinned ruff "$ruff_version" format --check scripts
pinned basedpyright "$(version basedpyright)" -p scripts
