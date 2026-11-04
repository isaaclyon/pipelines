# shellcheck shell=sh
# summary: Show service logs
# targets: local ci

compose logs "$@"
