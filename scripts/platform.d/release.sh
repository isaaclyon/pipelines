# shellcheck shell=sh
# summary: Build and tag versioned toolkit images from a commit: release [REV]
# targets: local

# ADR 0036: a toolkit version names one set of images built from one commit.
# The build reads VERSION and CHANGELOG.md from that commit and builds from an
# export of it, so uncommitted files never reach a release image. A version
# that already has images from another commit is never rebuilt.

if [ "$#" -gt 1 ]; then
    echo 'Usage: scripts/platform local release [REV]' >&2
    exit 2
fi
release_rev=$(git rev-parse --verify --quiet "${1:-HEAD}^{commit}") || {
    echo "Unknown revision: ${1:-HEAD}" >&2
    exit 2
}
release_version=$(git show "$release_rev:VERSION" 2>/dev/null) || {
    echo "VERSION is missing at $release_rev" >&2
    exit 1
}
if ! printf '%s\n' "$release_version" | grep -Eqx '(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)'; then
    echo "VERSION must be MAJOR.MINOR.PATCH, found: $release_version" >&2
    exit 1
fi
if ! git show "$release_rev:CHANGELOG.md" 2>/dev/null | grep -Fqx "## $release_version"; then
    echo "CHANGELOG.md at $release_rev has no '## $release_version' section" >&2
    exit 1
fi

release_components="control-plane ingestion transformations metrics"
release_missing=
for release_component in $release_components; do
    release_image="analytics-$release_component:$release_version"
    if release_built=$(docker image inspect --format \
        '{{ index .Config.Labels "org.opencontainers.image.revision" }}' \
        "$release_image" 2>/dev/null); then
        if [ "$release_built" != "$release_rev" ]; then
            echo "$release_image was built from ${release_built:-an unknown commit}, not $release_rev." >&2
            echo "Images are immutable: bump VERSION and add its CHANGELOG.md section." >&2
            exit 1
        fi
    else
        release_missing="$release_missing $release_component"
    fi
done
if [ -z "$release_missing" ]; then
    echo "Toolkit $release_version images already exist for $release_rev."
    exit 0
fi

release_tree=$(mktemp -d)
release_cleanup() {
    release_rc=$?
    trap - EXIT
    rm -rf "$release_tree"
    exit "$release_rc"
}
trap release_cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
git archive "$release_rev" | tar -x -C "$release_tree"

# Webserver and daemon share the control-plane image; cube-api owns metrics.
TOOLKIT_VERSION=$release_version TOOLKIT_REVISION=$release_rev docker compose \
    --project-directory "$release_tree/deploy/compose" \
    --project-name "$ANALYTICS_PROJECT-release" \
    --env-file "$release_tree/deploy/compose/.env.local.example" \
    -f "$release_tree/deploy/compose/compose.yaml" \
    -f "$release_tree/deploy/compose/release.yaml" \
    build webserver ingestion transformations cube-api || exit

for release_component in $release_components; do
    release_image="analytics-$release_component:$release_version"
    release_built=$(docker image inspect --format \
        '{{ index .Config.Labels "org.opencontainers.image.revision" }}' \
        "$release_image") || exit
    if [ "$release_built" != "$release_rev" ]; then
        echo "$release_image is missing its revision label" >&2
        exit 1
    fi
    echo "$release_image $(docker image inspect --format '{{ .Id }}' "$release_image")"
done
echo "Built toolkit $release_version from $release_rev."
