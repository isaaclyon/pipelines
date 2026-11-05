#!/bin/sh
# Exercise release guards and the build call without Docker.
set -eu
release_source=$(pwd)/scripts/platform.d/release.sh
release_fixture=$(mktemp -d)
trap 'rm -rf "$release_fixture"' EXIT
export RELEASE_STATE="$release_fixture/state" ANALYTICS_PROJECT=release-test
git init -q "$release_fixture/repo"

commit() (
    cd "$release_fixture/repo"
    printf '%s\n' "$1" > VERSION
    printf '# Changelog\n\n%s\n' "$2" > CHANGELOG.md
    mkdir -p deploy/compose
    : > deploy/compose/compose.yaml
    git add -A
    git -c user.name=test -c user.email=test@example.com commit -qm "$1"
    git rev-parse HEAD
)

run_release() (
    cd "$release_fixture/repo"
    # Called by the sourced release command.
    # shellcheck disable=SC2317,SC2329
    docker() {
        echo "docker $*" >> "$RELEASE_STATE/trace"
        case "$1" in
            image)
                image=$(eval "printf %s \"\${$#}\"")
                label="$RELEASE_STATE/$(printf %s "$image" | tr ':/' '__')"
                [ -f "$label" ] || return 1
                case "$*" in
                    *'.Id'*) echo "sha256:$image" ;;
                    *) cat "$label" ;;
                esac
                ;;
            compose)
                test -f "$3/compose.yaml"
                for component in control-plane ingestion transformations metrics; do
                    printf '%s\n' "$TOOLKIT_REVISION" > \
                        "$RELEASE_STATE/analytics-${component}_$TOOLKIT_VERSION"
                done
                echo "built $TOOLKIT_VERSION $TOOLKIT_REVISION" >> "$RELEASE_STATE/trace"
                ;;
            *) return 93 ;;
        esac
    }
    # shellcheck source=/dev/null
    . "$release_source"
)

reset_state() {
    rm -rf "$RELEASE_STATE"
    mkdir "$RELEASE_STATE"
}

expect_failure() {
    reset_state
    if run_release > "$RELEASE_STATE/out" 2>&1; then
        echo "Release unexpectedly passed: $1" >&2
        exit 1
    fi
    grep -q "$1" "$RELEASE_STATE/out"
}

commit 1.2 '## 1.2' > /dev/null
expect_failure 'VERSION must be MAJOR.MINOR.PATCH'

commit 0.1.0 '## 0.0.9' > /dev/null
expect_failure "no '## 0.1.0' section"

first=$(commit 0.1.0 '## 0.1.0')
reset_state
run_release > /dev/null
grep -qx "built 0.1.0 $first" "$RELEASE_STATE/trace"
grep -q -- '-f .*/deploy/compose/release.yaml build webserver ingestion transformations cube-api' \
    "$RELEASE_STATE/trace"

# A second build of the same commit changes nothing.
: > "$RELEASE_STATE/trace"
run_release | grep -q 'already exist'
if grep -q built "$RELEASE_STATE/trace"; then exit 1; fi

# The same version from another commit is refused.
second=$(commit 0.1.0 '## 0.1.0
Changed notes.')
if run_release > "$RELEASE_STATE/out" 2>&1; then
    echo "Rebuilt an existing version from another commit" >&2
    exit 1
fi
grep -q "built from $first, not $second" "$RELEASE_STATE/out"

# shellcheck source=/dev/null
if (cd "$release_fixture/repo" && set -- no-such-rev && . "$release_source") \
    > "$RELEASE_STATE/out" 2>&1; then
    echo "Accepted an unknown revision" >&2
    exit 1
fi
grep -q 'Unknown revision: no-such-rev' "$RELEASE_STATE/out"
echo "Release guards hold: version format, changelog section, immutable tags."
