#!/bin/sh
# Run in the destination checkout; the argument is the primary checkout.
set -eu

if [ "$#" -ne 1 ] || [ ! -d "$1" ]; then
    echo 'Usage: sh scripts/worktree-setup.sh /path/to/primary-checkout' >&2
    exit 2
fi
setup_primary=$(CDPATH='' cd -- "$1" && pwd)
setup_destination=$(pwd -P)
if [ ! -f packages/ingestion/pyproject.toml ] || [ ! -f packages/metrics/package-lock.json ]; then
    echo 'Run worktree setup from the destination repository root' >&2
    exit 2
fi
for setup_tool in uv npm; do
    if ! command -v "$setup_tool" >/dev/null 2>&1; then
        echo "Worktree setup requires $setup_tool on PATH" >&2
        exit 2
    fi
done

# Explicit local-only allowlist: do not copy remote credentials or runtime state.
for setup_env in .env deploy/compose/.env.local packages/ingestion/.env \
    packages/transformations/.env packages/metrics/.env; do
    setup_source="$setup_primary/$setup_env"
    setup_target="$setup_destination/$setup_env"
    if [ -f "$setup_source" ] && [ ! -e "$setup_target" ] && [ ! -L "$setup_target" ]; then
        # Reject symlinks so the allowlist cannot point at a remote env file.
        if [ -L "$setup_source" ]; then
            echo "Refusing to copy symlinked env file: $setup_env" >&2
            exit 2
        fi
        (umask 077; cp "$setup_source" "$setup_target")
        chmod 600 "$setup_target"
        printf 'Copied %s\n' "$setup_env"
    fi
done

# Override inherited environment locations; every checkout owns its dependencies.
for setup_project in packages/client-config packages/ingestion packages/transformations \
    packages/transformations/lineage deploy/dagster; do
    UV_PROJECT_ENVIRONMENT="$setup_destination/$setup_project/.venv" \
        uv sync --project "$setup_project" --frozen
done
npm ci --prefix packages/metrics
echo 'Worktree development dependencies ready'
