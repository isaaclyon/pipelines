# shellcheck shell=sh
# summary: Browse local DuckDB data in the DuckDB UI (needs the duckdb CLI)
# targets: local

# The databases live in Docker volumes the host cannot open, and the stack may
# be writing them. Copy them to a private temporary folder, open the copies
# read-only in the DuckDB UI, and delete them on exit: they hold private data.
# The warehouse is the main database. Each raw file is attached under the alias
# dbt uses (raw_<source folder>), so staging views resolve too.
if ! command -v duckdb >/dev/null 2>&1; then
    echo "Install the DuckDB CLI first, for example: brew install duckdb" >&2
    exit 2
fi
snapshot=$(mktemp -d "${TMPDIR:-/tmp}/analytics-browse.XXXXXX")
chmod 700 "$snapshot"
trap 'rm -rf "$snapshot"' EXIT INT TERM

# shellcheck disable=SC2016 # The container shell expands these.
compose run --rm --no-deps --volume "$snapshot:/out" permissions sh -c '
    set -eu
    cp /state/working/warehouse.duckdb /out/ 2>/dev/null || true
    for file in /state/ingestion/*/local/*.duckdb; do
        [ -f "$file" ] || continue
        source=${file#/state/ingestion/}
        mkdir -p /out/raw
        cp "$file" "/out/raw/raw_${source%%/*}.duckdb"
    done
'

attach=""
for file in "$snapshot"/raw/*.duckdb; do
    [ -f "$file" ] || continue
    alias=$(basename "$file" .duckdb)
    attach="$attach attach '$file' as $alias (read_only);"
    echo "Attached $alias"
done
# Attach every file read-only to an in-memory session. Opening the session
# read-only (-readonly) would stop the UI from creating its own state database.
if [ -f "$snapshot/warehouse.duckdb" ]; then
    attach="$attach attach '$snapshot/warehouse.duckdb' as warehouse (read_only); use warehouse;"
    echo "Opened the warehouse (dbt models: local_staging, local_marts, ...)"
elif [ -n "$attach" ]; then
    echo "No warehouse yet; run ./scripts/platform local dbt build to create it"
else
    echo "No local DuckDB data yet; load a source on the local stack first" >&2
    exit 1
fi

echo "The UI opens at http://localhost:4213. Type .exit here to close it."
duckdb -cmd "$attach" -ui
