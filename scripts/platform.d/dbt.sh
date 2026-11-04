# shellcheck shell=sh
# summary: Run dbt for this target using the checked-out dbt project
# targets: local ci

# Model schemas route through DBT_NAMESPACE. Local defaults to `local`.
# Shared targets need an explicit one, so a direct build never writes a shared
# client schema by accident.
# shellcheck disable=SC2154 # target is set by scripts/platform.
if [ "$target" = local ]; then
    DBT_NAMESPACE=${DBT_NAMESPACE:-local}
fi

# Mount the checkout so model edits take effect without rebuilding the image.
# The image still supplies dbt and its adapters; rebuild after dependency changes.
compose run --rm \
    --volume "$PWD/packages/transformations/dbt:/workspace/dbt" \
    --env DBT_PROJECT_DIR=/workspace/dbt \
    --env DBT_PROFILES_DIR=/workspace/dbt \
    ${DBT_NAMESPACE:+--env "DBT_NAMESPACE=$DBT_NAMESPACE"} \
    transformations dbt "$@"
