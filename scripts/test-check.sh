#!/bin/sh
# Check concurrency, failure propagation, and prebuilt-image execution offline.
set -eu
check_source=$(pwd)/scripts/platform.d/check.sh
fixture=$(mktemp -d)
trap 'rm -rf "$fixture"' EXIT
# The checked repository; traces and stubs stay outside it, so they are not changes.
repo="$fixture/repo"
mkdir -p "$repo/scripts" "$repo/packages/transformations/dbt/models" "$fixture/bin"
printf '#!/bin/sh\nexit 0\n' > "$repo/scripts/check-versions.sh"
cp scripts/ci_docs.py "$repo/scripts/"
printf 'select 1\n' > "$repo/packages/transformations/dbt/models/orders.sql"
printf '# Guide\n' > "$repo/README.md"
git_fixture() {
    git -C "$repo" -c user.email=ci@example.invalid -c user.name='CI test' \
        -c commit.gpgsign=false "$@" >/dev/null
}
git_fixture init
git_fixture add .
git_fixture commit -m base
base=$(git -C "$repo" rev-parse HEAD)
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
    cd "$repo"
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
                for service do
                    case "$service" in
                        *:/base:ro)
                            printf '%s\n' "${service%:/base:ro}" > "$CHECK_STATE/base-path"
                            cp -R "${service%:/base:ro}" "$CHECK_STATE/base"
                            ;;
                    esac
                done
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
    unset EXPECT_PARALLEL FAIL_SERVICES MISSING_IMAGE BUILD_FAILURE
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
# Without a base, every package and every dbt test runs (ADR 0026), and only
# the dbt package check receives the dbt selection.
reset_fixture
run_check --no-build
grep -q 'run --rm --no-deps --pull never -T -e DBT_CHECKS=all transformations-check' "$CHECK_TRACE"
grep -q 'run --rm --no-deps --pull never -T ingestion-check' "$CHECK_TRACE"

# A dbt model change checks the changed dbt tests against the extracted base.
reset_fixture
printf 'select 2\n' > "$repo/packages/transformations/dbt/models/orders.sql"
run_check --base "$base" --no-build
test "$(find "$CHECK_STATE" -name '*.started' | wc -l)" -eq 1
grep -q -- '-e DBT_CHECKS=changed -e DBT_BASE_PROJECT=/base -v .*:/base:ro transformations-check' "$CHECK_TRACE"
grep -qx 'select 1' "$CHECK_STATE/base/dbt/models/orders.sql"
test ! -e "$(cat "$CHECK_STATE/base-path")"
git_fixture checkout -- .

# The router chooses packages unless one is named.
mkdir -p "$repo/packages/ingestion/src"
printf 'pass\n' > "$repo/packages/ingestion/src/source.py"
reset_fixture
run_check --base "$base" --no-build
test -f "$CHECK_STATE/ingestion-check.finished"
test "$(find "$CHECK_STATE" -name '*.started' | wc -l)" -eq 1
reset_fixture
run_check transformations --base "$base" --no-build
grep -q -- '-T -e DBT_CHECKS=none transformations-check' "$CHECK_TRACE"
rm -r "$repo/packages/ingestion"

# A documentation-only change builds and runs no package, but checks its links.
printf '# Guide\n\nSee [setup](docs/setup.md).\n' > "$repo/README.md"
reset_fixture
if run_check --base "$base" 2>/dev/null; then
    echo 'Broken documentation link unexpectedly passed' >&2
    exit 1
fi
test ! -s "$CHECK_TRACE"
mkdir "$repo/docs"
printf '# Setup\n' > "$repo/docs/setup.md"
reset_fixture
run_check --base "$base"
test ! -s "$CHECK_TRACE"
git_fixture checkout -- .
rm -r "$repo/docs"

for arguments in --base 'unknown --base'; do
    reset_fixture
    # Word splitting is intended: each case is an argument list.
    # shellcheck disable=SC2086
    if run_check $arguments 2>/dev/null; then
        echo "Invalid arguments unexpectedly passed: $arguments" >&2
        exit 1
    fi
    test ! -s "$CHECK_TRACE"
done

echo 'Package checks preserve failures, run at most two at once, require prebuilt images, and follow the change router'
