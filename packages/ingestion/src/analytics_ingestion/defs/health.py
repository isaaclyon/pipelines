import os
from pathlib import Path

# Dagster's job decorator exposes an untyped third-party metadata signature.
from dagster import job, op  # pyright: ignore[reportUnknownVariableType]


@op
def check_process() -> None:
    if os.getuid() == 0:
        raise RuntimeError("Pipeline runs must use a non-root user")
    if not Path.cwd().name.startswith("analytics-run-"):
        raise RuntimeError("Pipeline runs need an isolated working directory")
    if Path("/var/run/docker.sock").exists():
        raise RuntimeError("Pipeline runs must not receive the Docker socket")


@job
def scaffold_check() -> None:
    """Exercise run launching without reading or writing business data."""
    check_process()
