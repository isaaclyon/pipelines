# 0028: Develop locally before shared Snowflake release

Date: 2026-11-01

## Status

Accepted by the owner on 2026-11-01.
Supersedes [0025](0025-use-duckdb-for-deterministic-tests.md)'s restriction of
DuckDB to tests and Snowflake-only serving. Its deterministic-test discipline
and refusal to equate local and Snowflake evidence remain.
For single-user local experiments, this also supersedes
[0021](0021-run-data-changes-as-orchestrated-pipeline-work.md)'s requirement that
every data change run through Dagster. Shared-environment rules remain unchanged.
Implementation is specified separately; this decision does not claim a local
serving command already exists.
On 2026-11-04, [0031](0031-serve-each-client-from-motherduck-or-snowflake.md)
replaced the Snowflake serving target with a per-client MotherDuck or Snowflake
warehouse, and [0036](0036-upgrade-clients-one-at-a-time-on-pinned-toolkit-versions.md)
replaced the reference to 0020 release compatibility. Local DuckDB rules remain.

## Context

One person currently develops and uses the analytical answers. Requiring remote
publication and warehouse permissions for every iteration makes that work slower
without serving another user. The owner chooses local development until others
need the shared Snowflake environment, with separate development database copies
for branch experiments. Existing dbt and Cube fixtures already execute the same
models and metric definitions on DuckDB.

Restoring transparent file replacement, continuous refresh or multi-user
coordination would recreate machinery the current need does not justify.
Existing main-branch releases still automatically deploy code to development;
changing the development policy does not turn that automation off.

## Decision

Use DuckDB and the same maintained Cube definitions for single-user local
analytics development. Retain Snowflake as the shared serving target under
[0010](0010-use-snowflake-as-the-warehouse.md), but defer executed Snowflake
parity, publication and reader-readiness acceptance until shared activation.
Local checks never count as that evidence.

- Each worktree owns its writable database copies and ingestion state. Stop all
  readers/writers, checkpoint and close the source database, then copy it. Never
  copy a live database or WAL and call it a consistent snapshot. Copies are
  private, ignored data, not Git artifacts or shared writable files.
- Keep raw/restricted working data separate from an **analytical-only serving
  file**. Cube receives only approved analytical marts/columns and matching
  publication metadata, with read-only filesystem/database access. A DuckDB
  schema or hidden Cube member is not an access boundary.
- Use **stop, refresh, validate, restart**. Stop Cube and close every handle;
  perform the local refresh/build and required validation; prepare and validate
  the analytical serving file; restart only after success. Failure leaves Cube
  stopped. No hot replacement, background writer, reconnect watcher or
  last-good-availability guarantee is required.
- Bind the service to loopback and require a non-fixture API secret. Local
  single-user access is not public access or approval to expose restricted PII.
  Source access and acquisition of real data still require their own authority.
- Keep one set of dbt row semantics and Cube metrics. Local selection changes
  connection/file handling, not eligibility, time zones, currency or grain.
  Deterministic fixture CI stays separate from private real-data experiments.
- Default off new consumer fields that are absent from remote publications.
  Before such code merges into the automatically deployed branch, implement and
  test a local-only activation boundary that preserves the remote model surface.
  An ADR, draft PR or missing remote credentials is not that boundary.

The [implementation specification](../specs/20261101-local-first-development.md)
separates the policy PR, remote-safety gate and local lifecycle implementation.
No remote deployment, disabling of services, grant changes, new source fetch or
warehouse publication is authorized by this decision.

## Consequences

Local iteration and branch experiments no longer wait for shared warehouse
acceptance. A stopped service during refresh is an accepted single-user cost.
Private copies consume local storage and need deliberate provenance and access
controls. A failed refresh is visible as unavailable, not stale or empty success.

Before shared use, execute the existing Snowflake type/value, publication,
predecessor compatibility, reader-denial/privacy and intended-user readiness
checks with explicit target and compute approval. Keep consecutive shared
releases compatible under [0020](0020-keep-consecutive-releases-compatible.md).
Local success does not establish warehouse grants, shared availability or live
source completeness. Existing remote automation remains unchanged until a
separately reviewed implementation changes it.
