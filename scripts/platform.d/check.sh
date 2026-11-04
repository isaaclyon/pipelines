# shellcheck shell=sh
# summary: Run offline package checks: check [ingestion|transformations|metrics] [--no-build]
# targets: local ci

sh scripts/check-versions.sh || exit $?

check_services="ingestion-check transformations-check metrics-check"
case "${1:-}" in
    ingestion|transformations|metrics) check_services="$1-check"; shift ;;
    ''|-*) ;;
    *)
        echo "Unknown package: $1 (expected ingestion, transformations, or metrics)" >&2
        exit 2
        ;;
esac

if [ "${1:-}" = --no-build ]; then
    shift
    if [ "$#" -ne 0 ]; then
        echo '--no-build cannot be combined with build arguments' >&2
        exit 2
    fi
else
    # Word splitting is intended: check_services is a service list.
    # shellcheck disable=SC2086
    compose --profile checks build $check_services "$@" || exit $?
fi

# Compose run can implicitly build a missing image. Require all images first,
# so --no-build cannot silently fall back to a different builder or pull a tag.
# shellcheck disable=SC2086
check_images=$(compose --profile checks config --images $check_services) || exit $?
for check_image in $check_images; do
    docker image inspect "$check_image" >/dev/null || exit $?
done

check_logs=$(mktemp -d)
check_pending=
check_failed=0
trap 'rm -rf "$check_logs"' EXIT
# Called by signal traps rather than directly.
# shellcheck disable=SC2317,SC2329
check_interrupt() {
    for check_job in $check_pending; do
        kill "${check_job%%:*}" 2>/dev/null || :
    done
    for check_job in $check_pending; do
        wait "${check_job%%:*}" 2>/dev/null || :
    done
    # Stop only check services; a local runtime stack may still be in use.
    # shellcheck disable=SC2086
    compose --profile checks rm --stop --force $check_services || :
    exit "$1"
}
trap 'check_interrupt 130' INT
trap 'check_interrupt 143' TERM

check_wait() {
    for check_job in $check_pending; do
        check_name=${check_job#*:}
        check_rc=0
        wait "${check_job%%:*}" || check_rc=$?
        printf '\n--- %s (exit %s) ---\n' "$check_name" "$check_rc"
        cat "$check_logs/$check_name.log"
        [ "$check_rc" -eq 0 ] || check_failed=1
    done
    check_pending=
}

# Two independent Python environments run together (at most 4 GiB combined).
# Metrics runs after that pair. Wait for every result, even when one fails.
# DBT_CHECKS (all, changed, or none) chooses which dbt project tests run; changed
# compares with the base checkout of packages/transformations in DBT_BASE_PROJECT.
# See ADR 0026.
# Other package jobs may inherit DBT_CHECKS without a dbt base checkout.
case " $check_services " in
    *" transformations-check "*)
        case "${DBT_CHECKS:=all}" in
            all|none) ;;
            changed)
                if [ ! -d "${DBT_BASE_PROJECT:-}/dbt" ]; then
                    echo 'DBT_CHECKS=changed needs DBT_BASE_PROJECT: a base checkout of packages/transformations' >&2
                    exit 2
                fi
                ;;
            *) echo "DBT_CHECKS must be all, changed, or none, not $DBT_CHECKS" >&2; exit 2 ;;
        esac
        ;;
esac

check_count=0
for check_service in $check_services; do
    printf 'Starting %s\n' "$check_service"
    if [ "$check_service" = transformations-check ]; then
        set -- -e "DBT_CHECKS=$DBT_CHECKS"
        if [ "$DBT_CHECKS" = changed ]; then
            set -- "$@" -e DBT_BASE_PROJECT=/base -v "$(cd "$DBT_BASE_PROJECT" && pwd):/base:ro"
        fi
    else
        set --
    fi
    compose --profile checks run --rm --no-deps --pull never -T "$@" "$check_service" \
        > "$check_logs/$check_service.log" 2>&1 &
    check_pending="$check_pending $!:$check_service"
    check_count=$((check_count + 1))
    if [ "$check_count" -eq 2 ]; then
        check_wait
        check_count=0
    fi
done
check_wait
exit "$check_failed"
