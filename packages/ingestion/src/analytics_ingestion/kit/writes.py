"""The one write policy for every dlt pipeline in this package.

Every source load and reconciliation writes through ``open_writes``. It owns:

1. Loader settings. A failed job must never count as loaded, and a package
   must stay retryable, so dlt must run with ``raise_on_failed_jobs=true`` and
   ``auto_abort_on_terminal_error=false``. Then a failed job leaves its package
   pending instead of loaded.
2. Pending packages. dlt's ``run`` loads a pending package and exits without
   extracting the data it was given, so no write may start while one exists.
   A load finishes it first (``pending="finish"``) and then reads the source
   again. A reconciliation refuses (``pending="refuse"``): its marks rest on
   the raw data loaded so far, and must load in the run that decided them.
   A reconciliation also refuses a loaded package with failed jobs, which only
   earlier unsafe settings can leave, since the raw data it checks is
   incomplete.
3. Payload files. A loaded package keeps no job files, since several sources
   hold restricted personal data. Failed and pending packages keep theirs,
   so recovery still works.

Callers serialize each pipeline (one exclusive-resource tag).
"""

import os
from collections.abc import Sequence
from typing import Literal

from dlt.common.configuration import resolve_configuration
from dlt.common.configuration.resolve import inject_section
from dlt.common.configuration.specs import ConfigSectionContext
from dlt.common.pipeline import LoadInfo
from dlt.load.configuration import LoaderConfiguration
from dlt.pipeline.pipeline import Pipeline


class Writes:
    """Write access to one pipeline under the package write policy."""

    def __init__(self, pipeline: Pipeline) -> None:
        self.pipeline = pipeline

    def run(self, data: object) -> LoadInfo:
        """Load ``data`` in a new package, then remove its payload files."""
        if self.pipeline.has_pending_data:
            raise ValueError(
                f"{self.pipeline.pipeline_name} has a pending load package; "
                "dlt would load it instead of this data"
            )
        info = self.pipeline.run(data)
        info.raise_on_failed_jobs()
        _remove_payload(self.pipeline, info.loads_ids)
        return info


def open_writes(
    pipeline: Pipeline, *, pending: Literal["finish", "refuse"] = "finish"
) -> Writes:
    """Apply the write policy before any source read or write."""
    # Resolve the same loader configuration as dlt. In 1.31.0 the loader's
    # environment configuration can override even an explicit load() argument.
    with inject_section(
        ConfigSectionContext(pipeline_name=pipeline.pipeline_name, sections=("load",))
    ):
        config = resolve_configuration(LoaderConfiguration(), accept_partial=True)
    if not config.raise_on_failed_jobs or config.auto_abort_on_terminal_error:
        raise ValueError(
            "Writing requires dlt raise_on_failed_jobs=true and "
            "auto_abort_on_terminal_error=false for safe completion and recovery"
        )
    if pipeline.has_pending_data:
        if pending == "refuse":
            raise ValueError(
                f"{pipeline.pipeline_name} has a pending load package; "
                "run its load job to finish it first"
            )
        pipeline.normalize()
        info = pipeline.load(raise_on_failed_jobs=True)
        info.raise_on_failed_jobs()
    loaded = pipeline.list_completed_load_packages()
    if pending == "refuse" and any(
        pipeline.list_failed_jobs_in_package(load_id) for load_id in loaded
    ):
        raise ValueError(
            f"{pipeline.pipeline_name} has a loaded package with failed jobs; "
            "its raw data is incomplete"
        )
    _remove_payload(pipeline, loaded)
    return Writes(pipeline)


def _remove_payload(pipeline: Pipeline, load_ids: Sequence[str]) -> None:
    for load_id in load_ids:
        jobs = pipeline.get_load_package_info(load_id).jobs
        if jobs.get("failed_jobs"):
            continue  # dlt keeps every file of a package with a failed job
        for job in jobs.get("completed_jobs", []):
            os.remove(job.file_path)
