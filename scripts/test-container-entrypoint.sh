#!/bin/sh
# Exercise capacity validation without starting a container.
set -eu
entrypoint_source=$(pwd)/scripts/container-entrypoint.sh
entrypoint_fixture=$(mktemp -d)
trap 'rm -rf "$entrypoint_fixture"' EXIT
mkdir "$entrypoint_fixture/bin"
for entrypoint_command in dagster dagster-webserver dagster-daemon; do
    printf '#!/bin/sh\necho executed\n' > "$entrypoint_fixture/bin/$entrypoint_command"
    chmod +x "$entrypoint_fixture/bin/$entrypoint_command"
done
export PATH="$entrypoint_fixture/bin:$PATH"

for entrypoint_command in dagster dagster-webserver dagster-daemon; do
    for entrypoint_environment in local ci development production; do
        for entrypoint_capacity in '' 0 -1 1 2 3 invalid; do
            entrypoint_expected=2
            case "$entrypoint_environment:$entrypoint_capacity" in
                local:1|ci:1|development:1|development:2|production:1|production:2)
                    entrypoint_expected=0 ;;
            esac
            env ANALYTICS_ENVIRONMENT="$entrypoint_environment" \
                DAGSTER_MAX_CONCURRENT_RUNS="$entrypoint_capacity" \
                sh "$entrypoint_source" "$entrypoint_command" \
                > "$entrypoint_fixture/output" 2>&1 && entrypoint_rc=0 || entrypoint_rc=$?
            test "$entrypoint_rc" = "$entrypoint_expected"
            if [ "$entrypoint_expected" = 0 ]; then
                grep -qx executed "$entrypoint_fixture/output"
            else
                if grep -qx executed "$entrypoint_fixture/output"; then
                    echo 'Invalid capacity reached the workload' >&2
                    exit 1
                fi
            fi
        done
    done
done
env ANALYTICS_ENVIRONMENT=invalid DAGSTER_MAX_CONCURRENT_RUNS=1 \
    sh "$entrypoint_source" dagster > "$entrypoint_fixture/output" 2>&1 \
    && entrypoint_rc=0 || entrypoint_rc=$?
test "$entrypoint_rc" = 2
# Database initialization and ordinary diagnostics do not use Dagster capacity.
env -u ANALYTICS_ENVIRONMENT -u DAGSTER_MAX_CONCURRENT_RUNS \
    sh "$entrypoint_source" true
echo 'Container entrypoint preserves bounded target-specific capacity'
