# shellcheck shell=sh
# summary: Run full CI checks with service logs and automatic cleanup
# targets: ci

if [ "$#" -ne 0 ]; then
    echo 'Usage: scripts/platform ci verify' >&2
    exit 2
fi

if [ "$(uname -s)" = Linux ] && [ -z "${DOCKER_SOCKET_GID:-}" ]; then
    DOCKER_SOCKET_GID=$(stat -c '%g' /var/run/docker.sock)
    export DOCKER_SOCKET_GID
fi

verify_cleanup() {
    verify_rc=$?
    trap - EXIT
    compose logs --no-color || :
    # Dagster launches run containers outside compose; remove them so that
    # compose can also remove the volumes they mount.
    verify_runs=$(docker ps -aq --filter "label=analytics/project=$ANALYTICS_PROJECT") || verify_runs=
    # shellcheck disable=SC2086 # One container ID per word.
    [ -z "$verify_runs" ] || docker rm -f $verify_runs >/dev/null || :
    # Only the default project keeps named volumes for reuse.
    verify_down=--volumes
    [ "$ANALYTICS_PROJECT" != analytics-ci ] || verify_down=
    # shellcheck disable=SC2086 # Empty when volumes are kept.
    compose down $verify_down || { [ "$verify_rc" -ne 0 ] || verify_rc=1; }
    exit "$verify_rc"
}
trap verify_cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

sh scripts/validate-scripts.sh || exit $?
./scripts/platform ci config --quiet || exit $?
./scripts/platform ci check || exit $?
./scripts/test-local-cube.sh || exit $?
./scripts/platform ci up || exit $?
./scripts/platform ci smoke || exit $?
