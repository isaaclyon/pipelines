"""Refuse a local Cube source that a running container can reach.

Usage: source_mounts.py SOURCE CONTAINER MOUNTS_JSON
"""

import json
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import cast

# Docker Desktop reports some host bind sources under this VM prefix.
HOST_MNT = "/host_mnt/"


def mount_reaches(source: Path, mount: Mapping[str, object]) -> bool:
    mounted = mount.get("Source")
    if not isinstance(mounted, str) or not Path(mounted).is_absolute():
        return False
    if mounted.startswith(HOST_MNT):
        mounted = mounted[len(HOST_MNT) - 1 :]
    parent = Path(mounted).resolve()
    if source == parent or parent in source.parents:
        return True
    if mount.get("Type") != "bind":
        return False
    # Filesystem identity also catches case aliases and macOS firmlinks.
    # A bind source missing from the host is a Docker Desktop VM path, such as
    # its socket proxy, so it cannot be the host source. Any other inspection
    # error cannot establish quiescence and propagates.
    try:
        return any(parent.samefile(item) for item in (source, *source.parents))
    except FileNotFoundError:
        return False


def main(source: str, container: str, mounts: str) -> None:
    for mount in cast(list[dict[str, object]], json.loads(mounts) or []):
        try:
            reaches = mount_reaches(Path(source), mount)
        except OSError as error:
            message = "Cannot inspect running container mount " + container
            raise SystemExit(message) from error
        if reaches:
            raise SystemExit("Source is mounted by a running container " + container)


if __name__ == "__main__":
    main(*sys.argv[1:4])
