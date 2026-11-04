# shellcheck shell=sh
# summary: Stop and remove services
# targets: local ci

compose down "$@"
