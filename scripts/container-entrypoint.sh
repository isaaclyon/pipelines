#!/bin/sh
set -eu

case "${1:-}" in
    dagster|dagster-webserver|dagster-daemon)
        case "${DAGSTER_MAX_CONCURRENT_RUNS:-}" in
            1|2) ;;
            *) echo 'DAGSTER_MAX_CONCURRENT_RUNS must be 1 or 2' >&2; exit 2 ;;
        esac
        case "${ANALYTICS_ENVIRONMENT:-}" in
            local|ci)
                if [ "$DAGSTER_MAX_CONCURRENT_RUNS" != 1 ]; then
                    echo 'Local and CI require DAGSTER_MAX_CONCURRENT_RUNS=1' >&2
                    exit 2
                fi
                ;;
            development|production) ;;
            *) echo 'Select an explicit ANALYTICS_ENVIRONMENT' >&2; exit 2 ;;
        esac
        ;;
esac

if [ "${1:-}" = dagster ] && [ "${2:-}" = api ]; then
    run_dir=$(mktemp -d /tmp/analytics-run-XXXXXX)
    cd "$run_dir"
fi

exec "$@"
