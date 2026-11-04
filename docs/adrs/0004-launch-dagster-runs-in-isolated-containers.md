# 0004: Launch Dagster runs in isolated containers

Date: 2026-10-07

## Status

Accepted.

## Context

Code-location servers expose definitions and serve metadata. Executing pipeline
work inside those long-running services would couple a job's resource use and
process state to their availability. Reusing worker processes can also make a
run depend on files or in-memory state left by an earlier run.

The platform needs local runs to exercise the same execution boundary as remote
development and production.

## Decision

Use Dagster's DockerRunLauncher to launch each run in a fresh container built
from its code location's image. Retain that image's identity in the location
configuration so metadata loading and run execution use the same code and
dependencies.

Apply this lifecycle in the local stack as well as remote environments. Each
run receives an explicit environment, resource budget, and access to only its
required credentials and persistent storage. Give temporary files a run-specific
working directory.

Keep recoverable pipeline state and committed outputs outside the container's
writable layer. Failed runs must report failure to Dagster and support retry
without relying on that layer. Publish outputs only after the relevant writes
and checks succeed.

Limit Docker daemon access to the Dagster control-plane services that require
it for launching and managing runs. Pipeline and semantic-service containers
must not receive the Docker socket. Treat launcher access as host-level authority.

Configure bounded run concurrency. Serialize local runs that write to the same
DuckDB database file, including writers in different code locations.

## Consequences

- Runs start from a known dependency and process state, and failures are contained
  within their execution containers.
- Container startup adds latency. Dagster must manage container termination,
  cancellation, logs, and cleanup.
- Fresh containers require durable checkpoints and deliberate retry behavior.
  They do not make partially completed loads automatically safe to repeat.
- Sharing a DuckDB file across containers requires storage coordination; separate
  containers do not remove the database's process-locking constraints.
- Docker daemon authority makes the control plane a privileged part of the
  deployment and must be accounted for in its access configuration.

## References

- [Dagster Docker deployment and run launcher](https://docs.dagster.io/deployment/oss/deployment-options/docker)
