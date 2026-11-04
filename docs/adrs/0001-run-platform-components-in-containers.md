# 0001: Run platform components in containers

Date: 2026-10-07

## Status

Accepted.

## Context

The platform combines Python ingestion and orchestration, dbt transformations,
and Cube's semantic services. Developers need to run it locally, CI needs clean
test environments, and production will initially run on a Hetzner VM.

Host-installed dependencies make those environments drift. Adding containers
after the platform grows would require finding and removing assumptions about
host interpreters, paths, and installed tools.

## Decision

Use Docker-built OCI images as the supported execution contract for platform
services, pipeline runs, and development checks from the initial scaffold.
Starting the local stack and running its standard checks must require only
Docker with Compose and a checkout; Python and Node installations on the host
must be optional.

Build images around deployable runtime boundaries. Install each runtime's
declared dependencies inside its image. Keep application code and dependencies
separate from persistent state, and supply environment configuration and
credentials at runtime.

Run Dagster, its metadata database, and Cube locally. Run ingestion and dbt as
containerized workloads. DuckDB is an embedded database used by those workloads;
Snowflake, BigQuery, and vendor services remain external connections. Offline
fixtures supply local equivalents of external data.

Use Docker as the supported runtime initially. OCI artifacts leave room for a
different container runtime later, subject to integration testing.

## Consequences

- Local development and CI exercise the same dependency and operating-system
  boundaries as deployment.
- Developers can use native editors and optional host tools while containers
  remain the reproducible path for execution.
- Image builds and source-mount development workflows need attention to keep
  iteration fast. Docker availability and disk usage become prerequisites.
- Containerization does not emulate Snowflake or guarantee data correctness.
  Database compatibility and ingestion behavior require their own tests.
- State must survive container replacement. Backups, recovery, and service
  supervision remain operational responsibilities.
- Service topology, dependency isolation, run lifecycle, and release promotion
  are specified separately in [0002](0002-operate-services-with-docker-compose.md),
  [0003](0003-isolate-dagster-code-locations.md),
  [0004](0004-launch-dagster-runs-in-isolated-containers.md), and
  [0005](0005-promote-immutable-release-images.md).
