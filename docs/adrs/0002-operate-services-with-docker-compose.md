# 0002: Operate services with Docker Compose

Date: 2026-10-07

## Status

Accepted.

## Context

[Container execution](0001-run-platform-components-in-containers.md) requires a
repeatable way to connect services, allocate storage, and start the platform.
The initial deployment targets a single Hetzner VM. Local development needs to
exercise the same service relationships without maintaining a second topology.

Host scripts would leave service ordering and networking implicit. A multi-host
scheduler would add operations work before the platform needs its capabilities.

## Decision

Use Docker Compose to define and operate the initial service topology locally,
in integration CI, and on the VM. Keep a shared base definition with explicit
local, CI, remote-development, and production overrides. Use profiles for
optional development and check services.

The topology includes:

- Dagster webserver and daemon, using a shared control-plane image.
- PostgreSQL for Dagster run, event, and scheduling metadata.
- The Dagster code-location servers defined in
  [0003](0003-isolate-dagster-code-locations.md).
- Cube API and refresh services, with Cube Store services required by the
  supported production configuration. Start with the smallest supported
  single-host topology and exercise it locally.

Dagster launches short-lived run containers as described in
[0004](0004-launch-dagster-runs-in-isolated-containers.md). Compose manages the
long-running services around those runs.

Define explicit networks, health checks, readiness dependencies, restart
policies, resource limits, and persistent storage. Supply credentials at
runtime. Publish only the necessary user-facing endpoints; bind local endpoints
to loopback by default. Production images contain application code, while local
overrides may mount source for iteration.

## Consequences

- The same topology can be started on a developer machine or a fresh VM.
- Overrides reduce duplication but must be checked as fully resolved Compose
  configurations to catch accidental inheritance.
- Readiness checks govern startup. Applications still need retries for later
  connection failures; a failed health check alone does not restart a service.
- A single-host deployment has a host-level failure boundary. Restoration from
  backups and redeployment are required when that host is lost.
- Multi-host scheduling and automatic failover require a later decision if
  availability or scale demands them.

## References

- [Compose in production](https://docs.docker.com/compose/how-tos/production/)
- [Compose startup order](https://docs.docker.com/compose/how-tos/startup-order/)
- [Cube Core deployment](https://github.com/cube-js/cube/blob/master/docs-mintlify/cube-core/deployment.mdx)
