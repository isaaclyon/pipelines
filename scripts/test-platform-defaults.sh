#!/bin/sh
# Check the dispatcher's default env file, project name and client per target.
set -eu
platform_source=$(pwd)/scripts/platform
defaults_fixture=$(mktemp -d)
trap 'rm -rf "$defaults_fixture"' EXIT
checkout="$defaults_fixture/My Change.2"
mkdir -p "$checkout/scripts/platform.d" "$checkout/deploy/compose" \
    "$checkout/examples/sample-client" "$defaults_fixture/client"
cp "$platform_source" "$checkout/scripts/platform"
cat > "$checkout/scripts/platform.d/print.sh" <<'SH'
# shellcheck shell=sh
# summary: Print resolved defaults
# targets: local ci
echo "$env_file $ANALYTICS_PROJECT $CLIENT_DIR"
SH

resolved() {
    env -u ANALYTICS_ENV_FILE -u ANALYTICS_PROJECT -u CLIENT_DIR "$@"
}
expect() {
    if [ "$1" != "$2" ]; then
        echo "Expected '$2', got '$1'" >&2
        exit 1
    fi
}

sample="$checkout/examples/sample-client"

# Without a local env file both targets use the offline example, and both run
# the sample client.
expect "$(resolved sh "$checkout/scripts/platform" local print)" \
    "deploy/compose/.env.local.example analytics-my-change-2 $sample"
expect "$(resolved sh "$checkout/scripts/platform" ci print)" \
    "deploy/compose/.env.local.example analytics-ci $sample"

# A local env file is the local default; CI never reads it.
touch "$checkout/deploy/compose/.env.local"
expect "$(resolved sh "$checkout/scripts/platform" local print)" \
    "deploy/compose/.env.local analytics-my-change-2 $sample"
expect "$(resolved sh "$checkout/scripts/platform" ci print)" \
    "deploy/compose/.env.local.example analytics-ci $sample"

# Explicit settings still win.
expect "$(ANALYTICS_ENV_FILE=other.env ANALYTICS_PROJECT=chosen \
    CLIENT_DIR="$defaults_fixture/client" \
    sh "$checkout/scripts/platform" local print)" "other.env chosen $defaults_fixture/client"

# A relative client folder resolves from the repository root; a missing one
# stops before any command runs.
mkdir "$checkout/clients"
cp -R "$sample" "$checkout/clients/brand"
expect "$(CLIENT_DIR=clients/brand \
    env -u ANALYTICS_ENV_FILE -u ANALYTICS_PROJECT \
    sh "$checkout/scripts/platform" local print)" \
    "deploy/compose/.env.local analytics-my-change-2 $checkout/clients/brand"
if CLIENT_DIR="$defaults_fixture/missing" sh "$checkout/scripts/platform" local print \
    >/dev/null 2>"$defaults_fixture/missing.err"; then
    echo 'A missing client folder must fail' >&2
    exit 1
fi
grep -q 'CLIENT_DIR' "$defaults_fixture/missing.err"
echo 'Platform defaults PASS'
