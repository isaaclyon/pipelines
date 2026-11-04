#!/bin/sh
# Check concurrency, failure propagation, and prebuilt-image execution offline.
set -eu
check_source=$(pwd)/scripts/platform.d/check.sh
fixture=$(mktemp -d)
trap 'rm -rf "$fixture"' EXIT
mkdir -p "$fixture/scripts" "$fixture/bin"
printf '#!/bin/sh\nexit 0\n' > "$fixture/scripts/check-versions.sh"
cat > "$fixture/bin/docker" <<'SH'
#!/bin/sh
test "$1 $2" = 'image inspect'
test "${MISSING_IMAGE:-}" != "$3"
SH
chmod +x "$fixture/bin/docker"
export PATH="$fixture/bin:$PATH"
export CHECK_TRACE="$fixture/trace"
export CHECK_STATE="$fixture/state"

run_check() (
    cd "$fixture"
    # Invoked indirectly by the sourced command, including in child processes.
    # shellcheck disable=SC2317,SC2329
    compose() {
        printf '%s\n' "$*" >> "$CHECK_TRACE"
        shift 2 # --profile checks
        case "$1" in
            config)
                shift 2 # config --images
                printf '%s\n' "$@"
                ;;
            build) test "${BUILD_FAILURE:-0}" = 0 ;;
            run)
                for service do :; done
                touch "$CHECK_STATE/$service.started"
                if [ "${EXPECT_PARALLEL:-0}" = 1 ]; then
                    case "$service" in
                        ingestion-check)
                            attempts=0
                            until [ -f "$CHECK_STATE/transformations-check.started" ]; do
                                attempts=$((attempts + 1))
                                [ "$attempts" -lt 40 ] || return 90
                                sleep 0.05
                            done
                            ;;
                        metrics-check)
                            test -f "$CHECK_STATE/ingestion-check.finished" || return 91
                            test -f "$CHECK_STATE/transformations-check.finished" || return 92
                            ;;
                    esac
                fi
                touch "$CHECK_STATE/$service.finished"
                case ":${FAIL_SERVICES:-}:" in
                    *":$service:"*) return 42 ;;
                esac
                ;;
            *) return 93 ;;
        esac
    }
    # shellcheck source=/dev/null
    . "$check_source"
)

reset_fixture() {
    # macOS sh can retain assignments made before shell-function calls.
    unset EXPECT_PARALLEL FAIL_SERVICES MISSING_IMAGE BUILD_FAILURE DBT_CHECKS DBT_BASE_PROJECT
    rm -rf "$CHECK_STATE"
    mkdir "$CHECK_STATE"
    : > "$CHECK_TRACE"
}

reset_fixture
EXPECT_PARALLEL=1 run_check --no-build
test "$(find "$CHECK_STATE" -name '*.finished' | wc -l)" -eq 3
if grep -q ' build' "$CHECK_TRACE"; then exit 1; fi

for failures in ingestion-check transformations-check ingestion-check:transformations-check metrics-check; do
    reset_fixture
    if EXPECT_PARALLEL=1 FAIL_SERVICES="$failures" run_check --no-build; then
        echo "Lost failure: $failures" >&2
        exit 1
    fi
    test "$(find "$CHECK_STATE" -name '*.finished' | wc -l)" -eq 3
done

reset_fixture
run_check metrics --no-build
test -f "$CHECK_STATE/metrics-check.finished"
test "$(find "$CHECK_STATE" -name '*.started' | wc -l)" -eq 1

reset_fixture
if MISSING_IMAGE=ingestion-check run_check --no-build; then
    echo 'Missing prebuilt image unexpectedly passed' >&2
    exit 1
fi
if grep -q ' run ' "$CHECK_TRACE"; then exit 1; fi

reset_fixture
if BUILD_FAILURE=1 run_check; then
    echo 'Build failure unexpectedly passed' >&2
    exit 1
fi
if grep -q ' run ' "$CHECK_TRACE"; then exit 1; fi

reset_fixture
run_check metrics --no-cache
grep -q 'build metrics-check --no-cache' "$CHECK_TRACE"
# Only the dbt package check receives the dbt selection (ADR 0026).
reset_fixture
run_check --no-build
grep -q 'run --rm --no-deps --pull never -T -e DBT_CHECKS=all transformations-check' "$CHECK_TRACE"
grep -q 'run --rm --no-deps --pull never -T ingestion-check' "$CHECK_TRACE"

reset_fixture
mkdir -p "$fixture/base/dbt"
DBT_CHECKS=changed DBT_BASE_PROJECT="$fixture/base" run_check transformations --no-build
grep -q -- "-e DBT_CHECKS=changed -e DBT_BASE_PROJECT=/base -v $(cd "$fixture/base" && pwd):/base:ro transformations-check" "$CHECK_TRACE"

# Non-dbt packages do not need the base checkout set only for transformations.
for package in ingestion metrics; do
    reset_fixture
    DBT_CHECKS=changed run_check "$package" --no-build
    test -f "$CHECK_STATE/$package-check.finished"
    if grep -q -- '-e DBT_CHECKS' "$CHECK_TRACE"; then exit 1; fi
done

for selection in changed:missing unknown:; do
    reset_fixture
    if DBT_CHECKS=${selection%%:*} DBT_BASE_PROJECT="$fixture/${selection#*:}" run_check transformations --no-build 2>/dev/null; then
        echo "Invalid dbt selection unexpectedly passed: $selection" >&2
        exit 1
    fi
    if grep -q ' run ' "$CHECK_TRACE"; then exit 1; fi
done

echo 'Package checks preserve failures, run at most two at once, require prebuilt images, and pass the dbt selection'
