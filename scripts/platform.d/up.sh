# shellcheck shell=sh
# summary: Start services and wait until they are healthy
# targets: local ci

up_build=--build
if [ "${1:-}" = --no-build ]; then
    up_build=--no-build
    shift
fi
compose up "$up_build" --detach --wait --wait-timeout 240 "$@"
