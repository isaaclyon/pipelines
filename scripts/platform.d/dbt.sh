# shellcheck shell=sh
# summary: Run dbt on the client's dbt project with the checked-out toolkit package
# targets: local ci

# Model schemas route through DBT_NAMESPACE. Local defaults to `local`.
# Shared targets need an explicit one, so a direct build never writes a shared
# client schema by accident.
# shellcheck disable=SC2154 # target is set by scripts/platform.
if [ "$target" = local ]; then
    DBT_NAMESPACE=${DBT_NAMESPACE:-local}
fi

# The client's project (CLIENT_DIR/dbt) is the root project, as in the client's
# image, and installs the checkout's toolkit package. Both are mounted
# read-only, so edits to either take effect without rebuilding the image; the
# image still supplies dbt and its adapters. Each run copies the client project
# and installs its packages inside the container, so nothing is written to the
# checkout. analytics-dbt reads the client's sources and reporting time zone
# from the client.yaml that the service mounts.
# shellcheck disable=SC2016 # The inner shell expands "$@".
compose run --rm \
    --volume "$CLIENT_DIR/dbt:/workspace/client-dbt:ro" \
    --volume "$PWD/packages/transformations/dbt:/workspace/toolkit-dbt:ro" \
    --env ANALYTICS_TOOLKIT_DBT=/workspace/toolkit-dbt \
    --env DBT_PROFILES_DIR=/workspace/toolkit-dbt \
    --env DBT_PROJECT_DIR=/tmp/client-dbt \
    ${DBT_NAMESPACE:+--env "DBT_NAMESPACE=$DBT_NAMESPACE"} \
    transformations sh -c '
        cp -R /workspace/client-dbt /tmp/client-dbt &&
            analytics-dbt deps --quiet &&
            exec analytics-dbt "$@"' analytics-dbt "$@"
