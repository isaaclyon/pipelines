#!/bin/sh
# Offline worktree bootstrap contract; dependency commands are fixtures.
set -eu
setup_source=$(pwd)/scripts/worktree-setup.sh
setup_fixture=$(mktemp -d)
setup_fixture=$(CDPATH='' cd -- "$setup_fixture" && pwd -P)
trap 'rm -rf "$setup_fixture"' EXIT
setup_primary="$setup_fixture/primary checkout"
setup_destination="$setup_fixture/new checkout"
mkdir -p "$setup_primary/deploy/compose" "$setup_destination/deploy/compose" \
    "$setup_destination/packages/ingestion" "$setup_destination/packages/transformations" \
    "$setup_destination/packages/metrics" "$setup_destination/deploy/dagster" "$setup_fixture/bin"
: > "$setup_destination/packages/ingestion/pyproject.toml"
: > "$setup_destination/packages/metrics/package-lock.json"
printf 'local fixture\n' > "$setup_primary/.env"
printf 'production fixture\n' > "$setup_primary/deploy/compose/.env.production"
printf 'local compose fixture\n' > "$setup_primary/deploy/compose/.env.local"
printf 'keep existing\n' > "$setup_destination/deploy/compose/.env.local"
cat > "$setup_fixture/bin/uv" <<'SH'
#!/bin/sh
printf 'uv %s %s\n' "$*" "$UV_PROJECT_ENVIRONMENT" >> "$SETUP_TRACE"
[ "${SETUP_FAIL:-0}" = 0 ] || exit 42
SH
cat > "$setup_fixture/bin/npm" <<'SH'
#!/bin/sh
printf 'npm %s\n' "$*" >> "$SETUP_TRACE"
SH
chmod +x "$setup_fixture/bin/uv" "$setup_fixture/bin/npm"
export PATH="$setup_fixture/bin:$PATH" SETUP_TRACE="$setup_fixture/trace"
run_setup() (
    cd "$setup_destination"
    sh "$setup_source" "$setup_primary"
)
run_setup
cmp "$setup_primary/.env" "$setup_destination/.env"
case "$(uname -s)" in
    Darwin) setup_permissions=$(stat -f '%Lp' "$setup_destination/.env") ;;
    *) setup_permissions=$(stat -c '%a' "$setup_destination/.env") ;;
esac
test "$setup_permissions" = 600
test "$(cat "$setup_destination/deploy/compose/.env.local")" = 'keep existing'
test ! -e "$setup_destination/deploy/compose/.env.production"
for setup_project in packages/ingestion packages/transformations \
    packages/transformations/lineage deploy/dagster; do
    printf 'uv sync --project %s --frozen %s/%s/.venv\n' \
        "$setup_project" "$setup_destination" "$setup_project"
done > "$setup_fixture/expected"
printf 'npm ci --prefix packages/metrics\n' >> "$setup_fixture/expected"
diff "$setup_fixture/expected" "$SETUP_TRACE"
printf 'keep edited\n' > "$setup_destination/.env"
run_setup
test "$(cat "$setup_destination/.env")" = 'keep edited'
# A failed install must stop before later packages run.
: > "$SETUP_TRACE"
export SETUP_FAIL=1
run_setup && setup_rc=0 || setup_rc=$?
test "$setup_rc" = 42
test "$(wc -l < "$SETUP_TRACE" | tr -d ' ')" = 1
# Do not follow an allowlisted local filename to another credentials file.
mkdir -p "$setup_primary/packages/metrics"
ln -s "$setup_primary/deploy/compose/.env.production" "$setup_primary/packages/metrics/.env"
run_setup && setup_rc=0 || setup_rc=$?
test "$setup_rc" = 2
test ! -e "$setup_destination/packages/metrics/.env"
echo 'Worktree setup copies local env, preserves edits, isolates dependencies, and stops on failure'
