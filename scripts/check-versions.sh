#!/bin/sh
set -eu

for dependency in dagster dagster-docker dagster-postgres sqlalchemy; do
    expected=$(sed -n "s/.*\"$dependency==\([^\"]*\)\".*/\1/p" deploy/dagster/pyproject.toml)
    test -n "$expected"
    for project in packages/ingestion packages/transformations; do
        actual=$(sed -n "s/.*\"$dependency==\([^\"]*\)\".*/\1/p" "$project/pyproject.toml")
        if [ "$actual" != "$expected" ]; then
            echo "$dependency differs in $project: $actual (control plane: $expected)" >&2
            exit 1
        fi
    done
done
echo "Dagster runtime and storage pins match across all Python projects."
