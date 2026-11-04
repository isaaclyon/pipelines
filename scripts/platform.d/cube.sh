# shellcheck shell=sh
# summary: Build, refresh, query or stop the bounded worktree-local Cube service
# targets: local

repo_root=${repo_root:?Run through scripts/platform}
key=$(python3 -c 'import hashlib,os; print(hashlib.sha256(os.getcwd().encode()).hexdigest()[:12])')
name="analytics-local-cube-$key"
image="analytics-metrics-local:$key"
action=${1:-help}
[ "$#" -eq 0 ] || shift

case "$action" in
    refresh|stop)
        if [ -L "$repo_root/.scratch" ] || [ -L "$repo_root/.scratch/local-cube" ]; then
            echo 'Worktree data directory must not be a symlink' >&2
            exit 1
        fi
        umask 077
        mkdir -p "$repo_root/.scratch/local-cube"
        lock="$repo_root/.scratch/local-cube/operation.lock"
        mkdir "$lock" || { echo 'Another local Cube operation is active; refusing concurrency' >&2; exit 1; }
        trap 'rmdir "$lock"' EXIT
        ;;
esac

stop_cube() {
    existing=$(docker container ls -a --filter "name=^/$name$" --format '{{.Names}}')
    [ -n "$existing" ] || return 0
    owner=$(docker inspect --format '{{index .Config.Labels "analytics.local-worktree"}}' "$name")
    [ "$owner" = "$repo_root" ] || { echo 'Refusing a container owned by another worktree' >&2; return 1; }
    docker stop -t 10 "$name" >/dev/null
    docker rm "$name" >/dev/null
}

case "$action" in
    build)
        docker build --target checks -f packages/metrics/Dockerfile -t "$image" .
        ;;
    stop) stop_cube ;;
    query) docker exec "$name" node local-server.cjs query ;;
    refresh)
        # Stop first. Every later failure leaves serving stopped; no old-file fallback.
        stop_cube
        trap 'stop_cube; rmdir "$lock"' EXIT
        trap 'exit 1' HUP INT TERM
        [ "$#" -eq 3 ] || { echo 'Use: cube refresh SOURCE_DB MARTS_SCHEMA PRIVACY_JSON' >&2; exit 2; }
        [ "${LOCAL_SOURCE_STOPPED:-}" = true ] || { echo 'Source owner must checkpoint and close every client, then set LOCAL_SOURCE_STOPPED=true' >&2; exit 2; }
        source=$(python3 - "$1" <<'PY'
import pathlib
import sys

try:
    path = pathlib.Path(sys.argv[1])
    assert path.is_absolute() and str(path.resolve(strict=True)) == str(path)
    assert path.is_file()
except (OSError, AssertionError):
    raise SystemExit("Source owner must supply an existing checkpointed, closed database at an absolute non-symlink path")
print(path)
PY
        )
        schema=$2
        privacy=$(python3 -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).resolve(strict=True))' "$3")
        # Pin the image for this entire refresh, even if another build starts.
        image_id=$(docker image inspect --format '{{.Id}}' "$image")
        docker run --rm --network none -e CUBEJS_API_SECRET --entrypoint node "$image_id" \
            -e "require('./local-server.cjs').privateSecret()"
        umask 077
        mkdir -p "$repo_root/.scratch/local-cube"
        run=$(mktemp -d "$repo_root/.scratch/local-cube/run.XXXXXX")
        # Docker Desktop does not share DuckDB locks with host applications.
        # Inspect host descriptors before allowing a container to copy.
        command -v lsof >/dev/null || { echo 'Install host lsof; source-holder verification is required' >&2; exit 1; }
        set -- "$source"
        [ ! -e "$source.wal" ] || set -- "$@" "$source.wal"
        holder_status=0
        holders=$(lsof -nP -t -- "$@" 2> "$run/source-handles.log") || holder_status=$?
        if [ "$holder_status" -gt 1 ] || [ -s "$run/source-handles.log" ]; then
            echo "Cannot establish host source quiescence; see $run/source-handles.log" >&2; exit 1
        fi
        python3 - "$holders" <<'PY'
import subprocess
import sys

# macOS keeps passive filesystem-transport descriptors after a container exits.
# This protected OS executable is not a database client. Running containers
# mounting the source are separately refused below; do not trust guest locks.
transport = "/System/Library/Frameworks/Virtualization.framework/Versions/A/XPCServices/com.apple.Virtualization.VirtualMachine.xpc/Contents/MacOS/com.apple.Virtualization.VirtualMachine"
for pid in sys.argv[1].split():
    if sys.platform == "darwin":
        executable = subprocess.run(
            ["ps", "-p", str(int(pid)), "-o", "comm="],
            check=True, capture_output=True, text=True, timeout=5,
        ).stdout.strip()
        if executable == transport:
            continue
    raise SystemExit("Source has open host handles; stop every source client")
PY
        # Bind-mount locks are not reliable across Docker Desktop containers
        # either. Refuse every running container that can reach this source.
        running_containers=$(docker ps -q)
        for container in $running_containers; do
            mounts=$(docker inspect --format '{{json .Mounts}}' "$container")
            python3 "$repo_root/scripts/source_mounts.py" "$source" "$container" "$mounts"
        done
        docker run --rm --network none --user "$(id -u):$(id -g)" \
            -e ANALYTICS_ENVIRONMENT=local -e LOCAL_SOURCE_STOPPED=true --entrypoint node \
            --mount "type=bind,source=$(dirname "$source"),target=/input,readonly" \
            --mount "type=bind,source=$privacy,target=/privacy.json,readonly" \
            --mount "type=bind,source=$run,target=/output" "$image_id" \
            local-prepare.cjs "/input/$(basename "$source")" /output "$schema" /privacy.json
        python3 -c 'import datetime,json,os,sys; s=os.stat(sys.argv[1]); json.dump({"source_file":sys.argv[1],"source_size":s.st_size,"source_mtime_ns":s.st_mtime_ns,"worktree":sys.argv[2],"branch":sys.argv[3],"copied_at":datetime.datetime.now(datetime.timezone.utc).isoformat()},open(sys.argv[4],"x"))' \
            "$source" "$repo_root" "$(git branch --show-current)" "$run/provenance.json"
        file=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["file"])' "$run/receipt.json")
        publication=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["publicationId"])' "$run/receipt.json")
        # Only the analytical file enters the service. No working/raw directory mount.
        docker run -d --name "$name" --label "analytics.local-worktree=$repo_root" \
            --user "$(id -u):$(id -g)" -p "127.0.0.1:${LOCAL_CUBE_PORT:-14061}:4000" \
            -e ANALYTICS_ENVIRONMENT=local -e CUBEJS_API_SECRET --entrypoint node \
            --mount "type=bind,source=$run/$file,target=/serving/marts.duckdb,readonly" \
            "$image_id" local-server.cjs serve /serving/marts.duckdb "$schema" "$publication" >/dev/null
        ready=false
        for _attempt in $(seq 1 30); do
            if docker exec "$name" node -e "fetch('http://127.0.0.1:4000/readyz',{signal:AbortSignal.timeout(1000)}).then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))" 2>/dev/null; then
                ready=true; break
            fi
            sleep 1
        done
        [ "$ready" = true ] || { docker logs "$name" >&2; exit 1; }
        rmdir "$lock"
        trap - EXIT HUP INT TERM
        printf 'Local Cube ready on %s for up to 180 seconds. Working copy: %s/working.duckdb\n' "$(docker port "$name" 4000/tcp)" "$run"
        ;;
    *) echo 'Use: scripts/platform local cube build|refresh SOURCE_DB MARTS_SCHEMA PRIVACY_JSON|query|stop' >&2; exit 2 ;;
esac
