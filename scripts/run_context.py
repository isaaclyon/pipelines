"""Derive Dagster run configuration from resolved Compose services."""

import json
import subprocess
import sys
import tempfile
from pathlib import PurePosixPath
from typing import cast

# These are supplied by the launcher, not persisted in location metadata.
LAUNCHER_ENV = {
    "DAGSTER_POSTGRES_PASSWORD",
    "ANALYTICS_ENVIRONMENT",
    "DBT_TARGET",
    "DAGSTER_MAX_CONCURRENT_RUNS",
    "DAGSTER_CURRENT_IMAGE",
}


def mapping(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError("Expected a Compose mapping")
    entries = cast(dict[object, object], value)
    if not all(isinstance(key, str) for key in entries):
        raise ValueError("Expected string Compose keys")
    return cast(dict[str, object], entries)


def string(value: object) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError("Expected a nonempty Compose string")
    return value


def add_run_contexts(config: dict[str, object]) -> None:
    volumes = mapping(config.get("volumes", {}))
    for name, value in mapping(config.get("services", {})).items():
        service = mapping(value)
        env = mapping(service.get("environment", {}))
        if "DAGSTER_CURRENT_IMAGE" not in env:
            continue
        if "DAGSTER_CONTAINER_CONTEXT" in env:
            raise ValueError(f"{name}: DAGSTER_CONTAINER_CONTEXT is generated")
        mounts: list[str] = []
        targets: list[PurePosixPath] = []
        raw_mounts = service.get("volumes", [])
        if not isinstance(raw_mounts, list):
            raise ValueError(f"{name}: expected a volume list")
        for raw in cast(list[object], raw_mounts):
            mount = mapping(raw)
            kind = string(mount.get("type"))
            source, target = string(mount.get("source")), string(mount.get("target"))
            if kind == "volume":
                if source not in volumes:
                    raise ValueError(f"{name}: undeclared volume")
                source = string(mapping(volumes[source]).get("name"))
            elif kind != "bind":
                raise ValueError(f"{name}: unsupported run mount type")
            # Do not silently lose mount options in translation to Docker binds.
            options = mapping(mount.get(kind, {}))
            if set(options) - {"create_host_path"}:
                raise ValueError(f"{name}: unsupported run mount options")
            mode = "ro" if mount.get("read_only") else "rw"
            mounts.append(f"{source}:{target}:{mode}")
            targets.append(PurePosixPath(target))
        forwarded: list[str] = []
        for key, raw in env.items():
            if key in LAUNCHER_ENV:
                continue
            if not isinstance(raw, str):
                raise ValueError(f"{name}: {key} must have an explicit value")
            if raw and key.endswith(("_PATH", "_ROOT")):
                path = PurePosixPath(raw)
                if (
                    not path.is_absolute()
                    or ".." in path.parts
                    or not any(
                        path == target or target in path.parents for target in targets
                    )
                ):
                    raise ValueError(f"{name}: {key} requires a run-container mount")
            forwarded.append(f"{key}={raw}")
        env["DAGSTER_CONTAINER_CONTEXT"] = json.dumps(
            {
                "docker": {
                    "env_vars": forwarded,
                    "container_kwargs": {
                        "labels": {"analytics/project": string(config.get("name"))},
                        "volumes": mounts,
                    },
                }
            }
        )


def main() -> int:
    # Arguments before -- are Compose global options; the rest is the command.
    split = sys.argv.index("--")
    base = ["docker", "compose", *sys.argv[1:split]]
    result = subprocess.run(
        [*base, "--profile", "*", "config", "--format", "json"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode:
        sys.stderr.write(result.stderr)
        return result.returncode
    try:
        # Compose escapes literal dollars in canonical output for reuse as input.
        # Decode that pass before embedding values inside the new JSON context.
        config = mapping(cast(object, json.loads(result.stdout.replace("$$", "$"))))
        add_run_contexts(config)
    except ValueError as error:
        print(f"Run configuration: {error}", file=sys.stderr)
        return 2
    # NamedTemporaryFile is owner-only. Escape dollars against a second Compose
    # interpolation pass; JSON encoding preserves quotes and embedded newlines.
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json") as rendered:
        _ = rendered.write(json.dumps(config).replace("$", "$$"))
        rendered.flush()
        return subprocess.call(
            [
                "docker",
                "compose",
                "--project-name",
                string(config.get("name")),
                "--env-file",
                sys.argv[sys.argv.index("--env-file") + 1],
                "-f",
                rendered.name,
                *sys.argv[split + 1 :],
            ]
        )


if __name__ == "__main__":
    sys.exit(main())
