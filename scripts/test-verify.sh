#!/bin/sh
# Exercise failure cleanup without starting Docker.
set -eu
verify_source=$(pwd)/scripts/platform.d/verify.sh
verify_fixture=$(mktemp -d)
trap 'rm -rf "$verify_fixture"' EXIT
mkdir -p "$verify_fixture/scripts"
cat > "$verify_fixture/scripts/validate-scripts.sh" <<'SH'
#!/bin/sh
echo scripts >> "$VERIFY_TRACE"
exit "${VERIFY_SCRIPTS_STATUS:-0}"
SH
cat > "$verify_fixture/scripts/platform" <<'SH'
#!/bin/sh
echo "$2" >> "$VERIFY_TRACE"
[ "$2" != smoke ] || exit 42
SH
cat > "$verify_fixture/scripts/test-local-cube.sh" <<'SH'
#!/bin/sh
echo local-cube >> "$VERIFY_TRACE"
exit "${VERIFY_CUBE_STATUS:-0}"
SH
chmod +x "$verify_fixture/scripts/platform" "$verify_fixture/scripts/test-local-cube.sh"
export VERIFY_TRACE="$verify_fixture/trace"
export DOCKER_SOCKET_GID=0 ANALYTICS_PROJECT=verify-test
run_verify() (
    cd "$verify_fixture"
    # Called by the sourced verification command.
    # shellcheck disable=SC2317,SC2329
    compose() { echo "$*" >> "$VERIFY_TRACE"; }
    # shellcheck disable=SC2317,SC2329
    docker() {
        echo "docker $*" >> "$VERIFY_TRACE"
        [ "$1" != ps ] || echo run-1
    }
    set --
    # shellcheck source=/dev/null
    . "$verify_source"
)
run_verify && verify_rc=0 || verify_rc=$?
test "$verify_rc" = 42
expect_cleanup() {
    printf 'logs --no-color\ndocker ps -aq --filter label=analytics/project=%s\n' "$ANALYTICS_PROJECT"
    printf 'docker rm -f run-1\n%s\n' "$1"
}
{ printf 'scripts\nconfig\ncheck\nlocal-cube\nup\nsmoke\n'; expect_cleanup 'down --volumes'; } > "$verify_fixture/expected"
diff "$verify_fixture/expected" "$VERIFY_TRACE"
: > "$VERIFY_TRACE"
export VERIFY_CUBE_STATUS=44
run_verify && verify_rc=0 || verify_rc=$?
test "$verify_rc" = 44
{ printf 'scripts\nconfig\ncheck\nlocal-cube\n'; expect_cleanup 'down --volumes'; } > "$verify_fixture/expected"
diff "$verify_fixture/expected" "$VERIFY_TRACE"
unset VERIFY_CUBE_STATUS
: > "$VERIFY_TRACE"
export VERIFY_SCRIPTS_STATUS=43
run_verify && verify_rc=0 || verify_rc=$?
test "$verify_rc" = 43
{ printf 'scripts\n'; expect_cleanup 'down --volumes'; } > "$verify_fixture/expected"
diff "$verify_fixture/expected" "$VERIFY_TRACE"
# The default project keeps its named volumes between runs.
: > "$VERIFY_TRACE"
export ANALYTICS_PROJECT=analytics-ci
run_verify && verify_rc=0 || verify_rc=$?
test "$verify_rc" = 43
{ printf 'scripts\n'; expect_cleanup down; } > "$verify_fixture/expected"
diff "$verify_fixture/expected" "$VERIFY_TRACE"
echo 'Verify failure preserves status, captures logs, and cleans up'
