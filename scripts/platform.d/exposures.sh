# shellcheck shell=sh
# summary: Regenerate the dbt exposures for the Cube views from the metrics model
# targets: local ci

exposures_file=packages/transformations/dbt/models/exposures/_cube__exposures.yml
exposures_tmp=$(mktemp) || exit $?
trap 'rm -f "$exposures_tmp"' EXIT
# The check image holds the pinned Cube compiler; rebuild to pick up model edits.
compose --profile checks build metrics-check >&2 || exit $?
compose --profile checks run --rm --no-deps -T --entrypoint node \
    metrics-check exposures.cjs > "$exposures_tmp" || exit $?
cat "$exposures_tmp" > "$exposures_file"
echo "Wrote $exposures_file"
