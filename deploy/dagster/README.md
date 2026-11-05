# Dagster control plane

Owns the independent control-plane dependency project, instance configuration,
and registration of the ingestion and transformation code locations.

`dagster.yaml` uses PostgreSQL metadata storage, a queued run coordinator, and
DockerRunLauncher. Each run uses its code location's image, a non-root user,
bounded memory/CPU, and a fresh temporary directory. Only the webserver and
daemon receive the Docker socket. Code-location container contexts declare the
volumes and additional environment values their runs receive.

One shared instance configuration reads `DAGSTER_MAX_CONCURRENT_RUNS` from the
selected deployment. Local/CI use one run because they share working DuckDB
files. Remote Snowflake targets can select two runs; the remote env example
selects two. Existing environments without the setting retain one until older
untagged write jobs have drained. Dagster startup accepts only one or two,
and rejects two for local/CI. Run containers receive the same capacity setting.

Jobs that share writable state declare the `analytics/exclusive-resource` run
tag with the same value. The queued coordinator admits only one run per unique
resource value, including named jobs launched manually. Read-only checks and
jobs with different resource values can use the remaining remote slot. Declare
these tags in the owning package; instance configuration stays source-agnostic.
Use the existing queue priorities to choose the next waiting run; running work
is not preempted. Long snapshots can still violate the reporting deadline; serialization is not
a freshness guarantee.

Dagster is the only entry point for Shopify loads, so the shared tag on its
load and reconcile jobs is what keeps them from writing the same tables at
once. A package that adds an entry point outside Dagster must protect its state
itself, as required by
[ADR 0021](../../docs/adrs/0021-run-data-changes-as-orchestrated-pipeline-work.md).

Smoke checks exercise the installed coordinator's configuration and admission
counter in both control services without connecting to storage or launching
runs. Artifacts and compute logs use the persistent artifacts volume.

Dagster pins must match across all three Python projects; package checks verify
this. SQLAlchemy is pinned to 2.0.54 because Dagster 1.13.25's storage failed
under 2.1 during actual run submission.

Before a Dagster upgrade that migrates metadata storage, back up the PostgreSQL
volume and drain queued and running jobs.

```sh
./scripts/platform local up
./scripts/platform local smoke
```

See [code-location isolation](../../docs/adrs/0003-isolate-dagster-code-locations.md)
and [isolated run containers](../../docs/adrs/0004-launch-dagster-runs-in-isolated-containers.md).
