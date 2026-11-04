#!/bin/sh
# Check the dispatcher's default env file and project name per target.
set -eu
platform_source=$(pwd)/scripts/platform
defaults_fixture=$(mktemp -d)
trap 'rm -rf "$defaults_fixture"' EXIT
checkout="$defaults_fixture/My Change.2"
mkdir -p "$checkout/scripts/platform.d" "$checkout/deploy/compose"
cp "$platform_source" "$checkout/scripts/platform"
cat > "$checkout/scripts/platform.d/print.sh" <<'SH'
# shellcheck shell=sh
# summary: Print resolved defaults
# targets: local ci
echo "$env_file $ANALYTICS_PROJECT"
SH

resolved() {
    env -u ANALYTICS_ENV_FILE -u ANALYTICS_PROJECT "$@"
}
expect() {
    if [ "$1" != "$2" ]; then
        echo "Expected '$2', got '$1'" >&2
        exit 1
    fi
}

# Without a local env file both targets use the offline example.
expect "$(resolved sh "$checkout/scripts/platform" local print)" \
    'deploy/compose/.env.local.example analytics-my-change-2'
expect "$(resolved sh "$checkout/scripts/platform" ci print)" \
    'deploy/compose/.env.local.example analytics-ci'

# A local env file is the local default; CI never reads it.
touch "$checkout/deploy/compose/.env.local"
expect "$(resolved sh "$checkout/scripts/platform" local print)" \
    'deploy/compose/.env.local analytics-my-change-2'
expect "$(resolved sh "$checkout/scripts/platform" ci print)" \
    'deploy/compose/.env.local.example analytics-ci'

# Explicit settings still win.
expect "$(ANALYTICS_ENV_FILE=other.env ANALYTICS_PROJECT=chosen \
    sh "$checkout/scripts/platform" local print)" 'other.env chosen'
echo 'Platform defaults PASS'
