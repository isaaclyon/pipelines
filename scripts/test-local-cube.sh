#!/bin/sh
# Executed synthetic lifecycle acceptance; never reads an existing source DB.
set -eu
repo_root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"
key=$(python3 -c 'import hashlib,os; print(hashlib.sha256(os.getcwd().encode()).hexdigest()[:12])')
name="analytics-local-cube-$key"
image="analytics-metrics-local:$key"
# Refuse to interrupt a user's existing local service for a test.
[ -z "$(docker container ls -a --filter "name=^/$name$" --format '{{.Names}}')" ] || { echo 'Stop the worktree local service before testing' >&2; exit 1; }
scripts/platform local cube build
umask 077
mkdir -p .scratch
input=$(mktemp -d "$repo_root/.scratch/local-cube-fixture.XXXXXX")
held=
holder_container=
cleanup() {
    if [ -n "$held" ]; then kill "$held" 2>/dev/null || :; wait "$held" 2>/dev/null || :; fi
    if [ -n "$holder_container" ]; then docker rm -f "$holder_container" >/dev/null 2>&1 || :; fi
    scripts/platform local cube stop
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM
CUBEJS_API_SECRET=$(openssl rand -hex 32)
export CUBEJS_API_SECRET LOCAL_SOURCE_STOPPED=true
LOCAL_CUBE_PORT=${LOCAL_CUBE_PORT:-0}
export LOCAL_CUBE_PORT
docker run --rm --network none --user "$(id -u):$(id -g)" --entrypoint node \
    --mount "type=bind,source=$input,target=/fixture" "$image" tests/local-lifecycle-fixture.cjs /fixture
# Docker Desktop does not share the host kernel's file locks. A host reader
# must be refused before the container can copy the source.
python3 -c 'import pathlib,sys,time; f=open(sys.argv[1],"rb"); pathlib.Path(sys.argv[2]).touch(); time.sleep(30)' "$input/source.duckdb" "$input/held" &
held=$!
for _attempt in $(seq 1 20); do [ ! -f "$input/held" ] || break; sleep 0.1; done
[ -f "$input/held" ]
if scripts/platform local cube refresh "$input/source.duckdb" ci_marts "$input/privacy.json"; then
    echo 'Host-held source unexpectedly accepted' >&2; exit 1
fi
kill "$held"; wait "$held" 2>/dev/null || :
held=
# A bind-mounted source held in a different container also lacks reliable
# cross-container locks on Docker Desktop. Refuse its mount before copying.
holder_container=$(docker run -d --rm --user "$(id -u):$(id -g)" --entrypoint node \
    --mount "type=bind,source=$input,target=/source" "$image" -e \
    "const {DuckDBDriver}=require('@cubejs-backend/duckdb-driver'); const d=new DuckDBDriver({databasePath:'/source/source.duckdb'}); d.query('SELECT 1').then(()=>console.log('holding')); setTimeout(()=>process.exit(0),30000);")
for _attempt in $(seq 1 20); do
    if docker logs "$holder_container" 2>&1 | grep -q '^holding$'; then break; fi
    sleep 0.2
done
docker logs "$holder_container" 2>&1 | grep -q '^holding$'
if scripts/platform local cube refresh "$input/source.duckdb" ci_marts "$input/privacy.json" > "$input/container-refusal.log" 2>&1; then
    echo 'Container-held source unexpectedly accepted' >&2; exit 1
fi
# Linux lsof can see this container's descriptor directly; Docker Desktop
# requires the mount check instead. Both must refuse before copying.
if ! grep -Eq 'Source has open host handles|Source is mounted by a running container' "$input/container-refusal.log"; then
    echo 'Container-held source did not trigger a pre-copy host/container refusal' >&2
    exit 1
fi
docker rm -f "$holder_container" >/dev/null
holder_container=
scripts/platform local cube refresh "$input/source.duckdb" ci_marts "$input/privacy.json"
address=$(docker port "$name" 4000/tcp)
scripts/platform local cube query | python3 -c 'import json,sys; x=json.load(sys.stdin); assert float(x["orders.net_sales_usd"]) == 70; assert float(x["orders.aov_usd"]) == 48; assert float(x["order_lines.asp_usd"]) == 9.5; print("Synthetic copy/export/start/query PASS: net sales=70, AOV=48, ASP=9.5")'
[ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "http://$address/cubejs-api/v1/meta")" = 403 ]
docker inspect "$name" | python3 -c 'import json,sys; x=json.load(sys.stdin)[0]; m=x["Mounts"]; assert len(m)==1 and not m[0]["RW"] and m[0]["Destination"]=="/serving/marts.duckdb"; assert all(p["HostIp"]=="127.0.0.1" for ports in x["HostConfig"]["PortBindings"].values() for p in ports)'
python3 -c 'import json,sys; x=json.load(open(sys.argv[1])); del x["fct_orders"]["net_revenue_shop_amount"]; json.dump(x,open(sys.argv[2],"w"))' "$input/privacy.json" "$input/unknown.json"
if scripts/platform local cube refresh "$input/source.duckdb" ci_marts "$input/unknown.json"; then
    echo 'Unknown privacy unexpectedly accepted' >&2; exit 1
fi
[ -z "$(docker container ls --filter "name=^/$name$" --format '{{.Names}}')" ]
if CUBEJS_API_SECRET=short scripts/platform local cube refresh "$input/source.duckdb" ci_marts "$input/privacy.json"; then
    echo 'Short secret unexpectedly accepted' >&2; exit 1
fi
[ -z "$(docker container ls --filter "name=^/$name$" --format '{{.Names}}')" ]
echo 'Synthetic auth/mount/privacy/refusal/failure-stays-stopped PASS. No real source accessed.'
