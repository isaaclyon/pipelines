# shellcheck shell=sh
# summary: Check code locations and Cube, then launch scaffold runs
# targets: local ci

# Mounted secret files are readable only by UID 10001.
for smoke_service in ingestion transformations webserver daemon cube-api; do
    smoke_uid=$(compose exec -T "$smoke_service" id -u)
    if [ "$smoke_uid" != 10001 ]; then
        echo "$smoke_service runs as UID $smoke_uid; services must run as UID 10001" >&2
        exit 1
    fi
done
for smoke_control in webserver daemon; do
    compose exec -T "$smoke_control" python /opt/dagster/check-concurrency.py
done
compose exec -T cube-api node /cube/conf/smoke-platform.cjs
# The fixture password matches DOCS_PASSWORD_HASH in .env.local.example.
compose exec -T -e DOCS_SMOKE_PASSWORD=local-fixture-docs-password \
    cube-api node /cube/conf/smoke-dagster.cjs
