"""Maintained dbt graph/event translation around the indivisible publisher.

The stock component permits model subsets. Publication instead owns a single
candidate lifecycle, so its multi-asset uses the Pythonic integration's specs
and events without allowing partial publication or independent check retries.
"""

import logging
import os
import shutil
import subprocess
import sys
import tempfile
import traceback
from collections.abc import Iterator, Mapping, Sequence
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from threading import RLock, Timer

import dagster as dg
from dagster_dbt import (
    DagsterDbtTranslator,
    DagsterDbtTranslatorSettings,
    DbtCliResource,
    DbtProject,
    dbt_assets,
)

from analytics_transformations.mart_contracts import mapping, read_json, text
from analytics_transformations.publication import run_dbt

Event = dg.Output[None] | dg.AssetCheckResult | dg.AssetObservation
_LOCK = RLock()
SELECT = "+fqn:*.marts.*"


class CandidateTranslator(DagsterDbtTranslator):
    def __init__(
        self, group: str | None = None, shared_models: frozenset[str] = frozenset()
    ) -> None:
        super().__init__(
            DagsterDbtTranslatorSettings(enable_duplicate_source_asset_keys=True)
        )
        self.group = group
        self.shared_models = shared_models

    def get_asset_spec(
        self,
        manifest: Mapping[str, object],
        unique_id: str,
        project: DbtProject | None,
    ) -> dg.AssetSpec:
        spec = super().get_asset_spec(manifest, unique_id, project)
        # Definition-time parsing is offline. Its DuckDB schema is not a real
        # candidate or consumer location, especially on Snowflake deployments.
        # Dagster leaves the unused automation-condition generic unspecified.
        return spec.replace_attributes(  # pyright: ignore[reportUnknownMemberType]
            metadata={
                key: value
                for key, value in mapping(spec.metadata).items()
                if key not in ("dagster/table_name", "dagster/storage_kind")
            },
            kinds={"dbt"},
        )

    def get_asset_key(self, dbt_resource_props: Mapping[str, object]) -> dg.AssetKey:
        # Candidate schemas change on every run. Logical model keys must not.
        if dbt_resource_props.get("resource_type") == "source":
            source = text(dbt_resource_props["source_name"])
            producers = {
                "shopify_orders": ["raw", "shopify", "shopify_orders_raw"],
                "shopify_customers": ["raw", "shopify", "shopify_customers_raw"],
                "loop_return_headers": ["raw", "loop_returns", "loop_returns_raw"],
                "goodday": ["raw", "goodday", "goodday_inventory_raw"],
            }
            if source in producers:
                return dg.AssetKey(producers[source])
            return dg.AssetKey(
                [
                    text(dbt_resource_props["source_name"]),
                    text(dbt_resource_props["name"]),
                ]
            )
        name = "alias" if dbt_resource_props.get("version") else "name"
        key = dg.AssetKey([text(dbt_resource_props[name])])
        # Shared ancestors are separate candidate copies, not one materialization
        # owned by two multi-assets. Keep existing keys for disjoint models.
        if self.group and dbt_resource_props.get("unique_id") in self.shared_models:
            return key.with_prefix(self.group)
        return key


TRANSLATOR = CandidateTranslator()


def project_manifest(
    project: Path, profiles: Path, env: Mapping[str, str]
) -> Mapping[str, object]:
    """Parse current feature flags offline, without connecting to a warehouse.

    No baked default-only manifest: enabled customer/inventory models must also
    appear in code-location definitions. Failed parses retain their evidence.
    """
    root = Path(env.get("DBT_TARGET_PATH", str(project / "target")))
    root.mkdir(parents=True, exist_ok=True)
    target = Path(tempfile.mkdtemp(prefix="definitions-", dir=root)).resolve()
    run_dbt(
        [
            "dbt",
            "parse",
            "--project-dir",
            str(project),
            "--profiles-dir",
            str(profiles),
            "--target-path",
            str(target),
            "--no-partial-parse",
        ],
        {**env, "DBT_NAMESPACE": "dagster", "DBT_TARGET": "local"},
        120,
    )
    manifest = mapping(read_json(target / "manifest.json"))
    shutil.rmtree(target)
    return manifest


def model_definition(
    manifest: Mapping[str, object], translator: CandidateTranslator = TRANSLATOR
) -> dg.AssetsDefinition:
    # Reuse dbt's selector and Dagster's check mapping rather than walking the
    # graph or generating check names here. This definition supplies specs only.
    @dbt_assets(manifest=manifest, select=SELECT, dagster_dbt_translator=translator)
    def candidate_models() -> Iterator[dg.Output[None]]:
        yield from ()

    return candidate_models


class CandidateRunner:
    """Use dagster-dbt for builds; publication-only macros retain the CLI runner.

    Events are yielded by the enclosing multi-asset even when publication fails.
    Raw stdout and adapter diagnostics never enter shared Dagster compute logs.
    The in-process publication job is serialized; the lock also protects the
    brief environment/stdio changes from another runner in the same process.
    """

    def __init__(
        self,
        context: dg.AssetExecutionContext,
        translator: CandidateTranslator = TRANSLATOR,
    ) -> None:
        self.context = context
        self.translator = translator
        self.events: list[Event] = []

    def __call__(
        self, command: Sequence[str], env: Mapping[str, str], timeout: int
    ) -> None:
        if command[1] != "build":
            run_dbt(command, env, timeout)
            return
        target = Path(command[command.index("--target-path") + 1])
        project = Path(command[command.index("--project-dir") + 1])
        profiles = Path(command[command.index("--profiles-dir") + 1])
        target.mkdir(mode=0o700, parents=True, exist_ok=True)
        target.chmod(0o700)
        self.context.log.info(f"Building dbt candidate; private artifacts: {target}")
        output_names = self.context.assets_def.keys_by_output_name
        failed = False
        timed_out = False
        failures: set[str] = set()
        with _LOCK, (target / "console.log").open("w") as output:
            previous = dict(os.environ)
            logger = logging.getLogger("dagster.builtin")
            disabled = logger.disabled
            try:
                os.environ.update(env)
                # The integration prints CLI output and may log adapter errors.
                # Keep both out of the shared event/compute log, including secrets
                # that dbt itself cannot recognize as credentials.
                logger.disabled = True
                with redirect_stdout(output), redirect_stderr(output):
                    resource = DbtCliResource(
                        project_dir=str(project),
                        profiles_dir=str(profiles),
                        dbt_executable=str(Path(sys.executable).with_name("dbt")),
                    )
                    manifest = mapping(
                        read_json(target.parent / "parse" / "manifest.json")
                    )
                    invocation = resource.cli(
                        list(command[1:]),
                        manifest=manifest,
                        dagster_dbt_translator=self.translator,
                        target_path=target,
                        raise_on_error=False,
                    )
                    # dagster-dbt leaves Popen generic; its stdout is bytes.
                    process: subprocess.Popen[bytes] = invocation.process  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]

                    def terminate() -> None:
                        nonlocal timed_out
                        timed_out = True
                        process.kill()

                    timer = Timer(timeout, terminate)
                    timer.start()
                    try:
                        # Third-party Output is generic over Any. Normalize it at
                        # this boundary; no dbt output carries a Python value.
                        for raw_event in invocation.stream_raw_events():
                            # Only known node IDs and fixed status values may leave
                            # the private console log; never log info.msg or SQL.
                            data = mapping(mapping(raw_event.raw_event).get("data", {}))
                            node = mapping(data.get("node_info", {}))
                            node_id = node.get("unique_id")
                            status = node.get("node_status")
                            if (
                                isinstance(node_id, str)
                                and node_id in mapping(manifest["nodes"])
                                and status in ("error", "fail", "runtime error")
                            ):
                                failures.add(node_id)
                            for event in raw_event.to_default_asset_events(  # pyright: ignore[reportUnknownVariableType, reportUnknownMemberType]
                                manifest=manifest,
                                dagster_dbt_translator=self.translator,
                                context=self.context,
                            ):
                                if isinstance(event, dg.Output):
                                    if event.output_name not in output_names:
                                        raise ValueError("Unexpected dbt output")
                                    self.events.append(
                                        dg.Output(
                                            None,
                                            output_name=event.output_name,
                                            metadata=event.metadata,
                                        )
                                    )
                                elif isinstance(
                                    event, dg.AssetCheckResult | dg.AssetObservation
                                ):
                                    self.events.append(event)
                        failed = not invocation.is_successful()
                    finally:
                        timer.cancel()
                        timer.join()
                        if process.poll() is None:
                            process.kill()
                        _ = process.wait(timeout=10)
            except Exception:
                traceback.print_exc(file=output)
                failed = True
            finally:
                os.environ.clear()
                os.environ.update(previous)
                logger.disabled = disabled
        self.context.log.info(
            f"dbt candidate {'failed' if failed else 'completed'}; artifacts: {target}"
        )
        if failed:
            summary = (
                f"timeout after {timeout}s"
                if timed_out
                else ", ".join(sorted(failures)) or "see diagnostics"
            )
            raise RuntimeError(
                f"dbt build failed ({summary}); private diagnostics: {target}"
            ) from None
