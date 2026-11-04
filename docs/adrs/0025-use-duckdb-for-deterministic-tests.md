# 0025: Use DuckDB for deterministic tests

Date: 2026-10-29

## Status

Serving restriction superseded on 2026-11-01 by
[0028](0028-develop-locally-before-shared-snowflake-release.md). Deterministic tests
and the separation of local evidence from Snowflake readiness remain applicable.
The original decision below is retained for history.

Originally accepted. Supersedes [0015](0015-support-duckdb-and-snowflake.md) and the
local serving-equivalence requirements in [0006](0006-isolate-development-and-production-environments.md)
and [0014](0014-govern-metrics-with-cube-core.md).

## Context

Offline transformation tests and offline end-to-end serving are different needs.
No demonstrated requirement justifies maintaining a second serving backend.
The previous promise requires separate publication, filesystem permissions,
locking, snapshot replacement, reader reconnection and lifecycle acceptance.
Those mechanisms do not verify Snowflake publication or privileges.

## Decision

Use DuckDB primarily for deterministic transformation tests in disposable,
test-owned databases. Keep shared dbt models, portable SQL, adapter-dispatched
macros and representative executed fixtures on both adapters. Compare business
values and physical types without hiding precision or time-zone differences.
Fixture ingestion and bounded synthetic semantic tests may also use DuckDB;
they do not establish a supported serving architecture.

Use Snowflake as the only mart publication and Cube serving backend. Verify
publication, failure preservation, concurrency and reader permission denials in
uniquely named, isolated development schemas with explicit target selection and
a finite authorized compute budget. Offline checks never require credentials.
Keep production access and development access separate.

Remove DuckDB publication, published-file mounts, filesystem access boundaries
and the custom Cube replacement-file driver. Local/CI service smoke checks may
check code loading and Cube metadata without querying a warehouse. They do not
promise an offline business pipeline or operational equivalence to production.
A future offline serving requirement needs concrete user evidence and a new
decision before reintroducing those obligations.

## Consequences

- One publication and serving implementation remains to maintain.
- Fast offline model checks remain available without warehouse credentials.
- Offline model or semantic tests do not prove Snowflake grants, atomicity,
  lifecycle behavior or SQL compatibility. Development acceptance remains required.
- End-to-end serving requires development warehouse access and incurs cost.
- Old local published volumes are no longer mounted or initialized. Existing
  files are not migrated or deleted automatically.
