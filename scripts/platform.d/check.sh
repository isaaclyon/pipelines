# shellcheck shell=sh
# summary: Run offline package checks: check [ingestion|transformations|metrics] [--base REV] [--no-build]
# targets: local ci

sh scripts/check-versions.sh || exit $?

check_package=
case "${1:-}" in
    ingestion|transformations|metrics) check_package=$1; shift ;;
    ''|-*) ;;
    *)
        echo "Unknown package: $1 (expected ingestion, transformations, or metrics)" >&2
        exit 2
        ;;
esac

check_base=
if [ "${1:-}" = --base ]; then
    if [ "$#" -lt 2 ]; then
        echo '--base needs a commit, such as origin/main' >&2
        exit 2
    fi
    check_base=$2
    shift 2
fi

# One decision for the whole check (ADR 0026). The router compares the base
# commit with the working tree; it checks changed documentation links and
# chooses the packages and the dbt tests to run. Without a base, everything runs.
# shellcheck disable=SC2086 # The base is one word or absent.
check_route=$(python3 scripts/ci_docs.py ${check_base:+"$check_base"}) || exit $?
check_dbt=$(printf '%s\n' "$check_route" | sed -n 's/^dbt_checks=//p')
if [ -n "$check_package" ]; then
    check_packages=$check_package
else
    check_packages=$(printf '%s\n' "$check_route" | sed -n 's/^packages=//p' | tr -d '[]",')
fi
printf 'Checking packages: %s; dbt tests: %s\n' "${check_packages:-none}" "$check_dbt"
if [ -n "$check_base" ] && printf '%s\n' "$check_route" | grep -qx 'integration=true'; then
    echo 'This change also needs integration checks: ./scripts/platform ci verify'
fi
check_services=
for check_package in $check_packages; do
    check_services="$check_services $check_package-check"
done
[ -n "$check_services" ] || exit 0

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

# Package logs and the extracted base project.
check_work=$(mktemp -d)
check_pending=
check_failed=0
trap 'rm -rf "$check_work"' EXIT
mkdir "$check_work/base"
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
        cat "$check_work/$check_name.log"
        [ "$check_rc" -eq 0 ] || check_failed=1
    done
    check_pending=
}

# Two independent Python environments run together (at most 4 GiB combined).
# Metrics runs after that pair. Wait for every result, even when one fails.
# Changed dbt tests compare with the base commit's packages/transformations.
case " $check_services " in
    *" transformations-check "*)
        if [ "$check_dbt" = changed ]; then
            git archive -o "$check_work/base.tar" "$check_base" packages/transformations || exit $?
            tar -xf "$check_work/base.tar" -C "$check_work/base" || exit $?
        fi
        ;;
esac

check_count=0
for check_service in $check_services; do
    printf 'Starting %s\n' "$check_service"
    if [ "$check_service" = transformations-check ]; then
        set -- -e "DBT_CHECKS=$check_dbt"
        if [ "$check_dbt" = changed ]; then
            set -- "$@" -e DBT_BASE_PROJECT=/base -v "$check_work/base/packages/transformations:/base:ro"
        fi
    else
        set --
    fi
    compose --profile checks run --rm --no-deps --pull never -T "$@" "$check_service" \
        > "$check_work/$check_service.log" 2>&1 &
    check_pending="$check_pending $!:$check_service"
    check_count=$((check_count + 1))
    if [ "$check_count" -eq 2 ]; then
        check_wait
        check_count=0
    fi
done
check_wait
exit "$check_failed"
