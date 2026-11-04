"""Publish each consistency group after a successful write to its declared inputs."""

import os
from collections import Counter
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path

import dagster as dg

from analytics_transformations.dbt_execution import (
    CandidateRunner,
    CandidateTranslator,
    Event,
    model_definition,
    project_manifest,
)
from analytics_transformations.mart_contracts import mapping
from analytics_transformations.publication import (
    publication_manifest,
    publish_snowflake_marts,
)

PUBLICATION = dg.AssetKey(["marts", "development_publication"])
JOB_NAME = "development_marts_publication"
MARTS_SUFFIX = "_marts"
INPUT_TAG = "analytics/publication-input"
GROUP_SETTINGS = {
    "orders": (JOB_NAME, PUBLICATION, "ANALYTICS_MARTS_SCHEMA", "development-marts"),
    "customers": (
        "shopify_customers_publication",
        dg.AssetKey(["marts", "shopify_customers_publication"]),
        "ANALYTICS_CUSTOMERS_MARTS_SCHEMA",
        "development-customers",
    ),
    "inventory": (
        "goodday_inventory_publication",
        dg.AssetKey(["marts", "goodday_inventory_publication"]),
        "ANALYTICS_INVENTORY_MARTS_SCHEMA",
        "development-inventory",
    ),
}


class PublicationConfig(dg.Config):
    # Prepublish to a new destination before moving consumers during migration.
    namespace: str | None = None


# STARTING runs may already hold their input locks. Do not let them consume a
# later success event without requesting another publication.
WAITING = [dg.DagsterRunStatus.QUEUED, dg.DagsterRunStatus.NOT_STARTED]


@dataclass(frozen=True)
class PublicationInput:
    source: str
    group: str
    assets: tuple[dg.AssetKey, ...]
    jobs: tuple[str, ...]
    enabled_env: str | None = None


INPUTS = (
    PublicationInput(
        "shopify_orders",
        "orders",
        (
            dg.AssetKey(["raw", "shopify", "shopify_orders_raw"]),
            dg.AssetKey(["raw", "shopify", "shopify_order_source_status"]),
        ),
        ("shopify_orders_dlt_load", "shopify_orders_reconcile_counts"),
    ),
    PublicationInput(
        "shopify_customers",
        "customers",
        (
            dg.AssetKey(["raw", "shopify", "shopify_customers_raw"]),
            dg.AssetKey(["raw", "shopify", "shopify_customer_source_status"]),
        ),
        ("shopify_customers_dlt_load", "shopify_customers_reconcile"),
        "SHOPIFY_CUSTOMERS_ENABLED",
    ),
    PublicationInput(
        "loop_return_headers",
        "orders",
        (dg.AssetKey(["raw", "loop_returns", "loop_returns_raw"]),),
        ("loop_returns_dlt_snapshot",),
        "LOOP_RETURNS_ENABLED",
    ),
    PublicationInput(
        "goodday",
        "inventory",
        (dg.AssetKey(["raw", "goodday", "goodday_inventory_raw"]),),
        ("goodday_inventory_capture",),
        "GOODDAY_INVENTORY_ENABLED",
    ),
)


def publication_inputs(
    env: Mapping[str, str], group: str | None = "orders"
) -> tuple[PublicationInput, ...]:
    """Use exactly the enablement rules used by the dbt project."""
    return tuple(
        item
        for item in INPUTS
        if (group is None or item.group == group)
        and (item.enabled_env is None or env.get(item.enabled_env, "false") == "true")
    )


def publication_settings(
    env: Mapping[str, str], group: str = "orders"
) -> tuple[str, str]:
    """Derive the namespace from Cube's schema and require its reader role."""
    setting = GROUP_SETTINGS[group][2]
    schema = env.get(setting, "").lower()
    if not schema.endswith(MARTS_SUFFIX) or len(schema) == len(MARTS_SUFFIX):
        raise ValueError(f"{setting} must name a <namespace>_marts schema")
    schemas = [env.get(settings[2], "").lower() for settings in GROUP_SETTINGS.values()]
    if schemas.count(schema) != 1:
        raise ValueError("Publication groups require distinct schemas")
    reader = env.get("ANALYTICS_MARTS_READER_ROLE", "")
    if not reader:
        raise ValueError("Set ANALYTICS_MARTS_READER_ROLE to the Cube reader role")
    return schema.removesuffix(MARTS_SUFFIX), reader


def publication_request(
    instance: dg.DagsterInstance,
    run: dg.DagsterRun,
    inputs: tuple[PublicationInput, ...],
    group: str = "orders",
) -> dg.RunRequest | dg.SkipReason:
    if run.status != dg.DagsterRunStatus.SUCCESS or not any(
        run.job_name in item.jobs for item in inputs
    ):
        return dg.SkipReason("Not a successful write to an enabled publication input")
    job_name = GROUP_SETTINGS[group][0]
    if instance.get_run_records(
        filters=dg.RunsFilter(job_name=job_name, statuses=WAITING), limit=1
    ):
        return dg.SkipReason("A waiting publication will read this input")
    return dg.RunRequest(run_key=f"{job_name}:{run.run_id}")


def group_definitions(
    env: Mapping[str, str],
    group: str,
    manifest: Mapping[str, object],
    shared_models: frozenset[str],
) -> dg.Definitions:
    inputs = publication_inputs(env, group)
    configured_inputs = publication_inputs(env, None)
    job_name, publication, schema_setting, input_lock = GROUP_SETTINGS[group]
    translator = CandidateTranslator(group, shared_models)
    models = model_definition(manifest, translator)

    @dg.multi_asset(
        name="__".join(publication.path),
        specs=[
            *models.specs,
            dg.AssetSpec(
                publication,
                deps=[*models.keys, *(key for item in inputs for key in item.assets)],
                description=(
                    f"Validated {group} marts published together for development."
                ),
            ),
        ],
        check_specs=list(models.check_specs),
        can_subset=False,
    )
    def development_publication(
        context: dg.AssetExecutionContext,
        config: PublicationConfig,
    ) -> Iterator[Event | dg.MaterializeResult[None]]:
        if publication_inputs(os.environ, None) != configured_inputs:
            raise ValueError(
                "Publication input enablement changed; reload the code location"
            )
        runtime_env = dict(os.environ)
        if config.namespace is not None:
            runtime_env[schema_setting] = config.namespace + MARTS_SUFFIX
        namespace, reader = publication_settings(runtime_env, group)
        project = Path(os.environ.get("DBT_PROJECT_DIR", "dbt"))
        runner = CandidateRunner(context, translator)
        try:
            result = publish_snowflake_marts(
                environment=os.environ.get("ANALYTICS_ENVIRONMENT", ""),
                namespace=namespace,
                publication_group=group,
                project_dir=project,
                profiles_dir=Path(os.environ.get("DBT_PROFILES_DIR", str(project))),
                reader_role=reader,
                runner=runner,
                expected_sources=frozenset(item.source for item in inputs),
            )
        finally:
            # Preserve model and check events even when promotion fails.
            yield from runner.events
        context.log.info(
            f"Published {namespace}{MARTS_SUFFIX}: {result.publication_id}"
        )
        yield dg.MaterializeResult(
            asset_key=publication,
            value=None,
            metadata={
                "artifacts_directory": str(result.artifacts_directory),
                "namespace": namespace,
                "publication_id": result.publication_id,
                "published_at": result.published_at.isoformat(),
                "restricted_relations_unpublished": list(
                    result.restricted_relations_unpublished
                ),
            },
        )

    # Dagster's optional metadata parameter contains an untyped PathLike.
    job = dg.define_asset_job(  # pyright: ignore[reportUnknownMemberType]
        job_name,
        selection=[development_publication],
        executor_def=dg.in_process_executor,
        tags={
            INPUT_TAG: input_lock,
            "dagster/max_retries": "0",
            "dagster/retry_on_asset_or_op_failure": "false",
        },
    )

    @dg.run_status_sensor(  # pyright: ignore[reportUnknownMemberType]
        name=f"{job_name}_sensor",
        run_status=dg.DagsterRunStatus.SUCCESS,
        monitored_jobs=[
            dg.JobSelector(location_name="ingestion", job_name=name)
            for item in inputs
            for name in item.jobs
        ],
        request_job=job,
        default_status=dg.DefaultSensorStatus.STOPPED,
    )
    def publication_sensor(
        context: dg.RunStatusSensorContext,
    ) -> dg.RunRequest | dg.SkipReason:
        return publication_request(context.instance, context.dagster_run, inputs, group)

    return dg.Definitions(
        assets=[development_publication], jobs=[job], sensors=[publication_sensor]
    )


def build_definitions(env: Mapping[str, str]) -> dg.Definitions:
    project = Path(env.get("DBT_PROJECT_DIR", "dbt")).resolve()
    manifest = project_manifest(
        project, Path(env.get("DBT_PROFILES_DIR", str(project))).resolve(), env
    )
    groups = {
        group: publication_manifest(manifest, group)
        for group in GROUP_SETTINGS
        if publication_inputs(env, group)
    }
    owners = Counter(
        node_id
        for scoped in groups.values()
        for node_id, node in mapping(scoped["nodes"]).items()
        if mapping(node).get("resource_type") == "model"
    )
    shared_models = frozenset(node_id for node_id, count in owners.items() if count > 1)
    return dg.Definitions.merge(
        *(
            group_definitions(env, group, scoped, shared_models)
            for group, scoped in groups.items()
        )
    )


@dg.definitions
def defs() -> dg.Definitions:
    # Loading parses the dbt project, so importing this module does not.
    return build_definitions(os.environ)
