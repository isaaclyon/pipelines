# Metrics

Owns Cube semantic definitions and service configuration. The pinned Cube image
supplies the service and database drivers; `package.json` and its lockfile pin
TypeScript and Node types for the check image.

```sh
./scripts/platform local check metrics
./scripts/platform local smoke
```

## Current state

There are no marts and no Cube measures over warehouse data yet. The model holds
one placeholder cube, [`model/placeholder.js`](model/placeholder.js), whose SQL
is a constant query that reads no warehouse table. It exists only so the service
compiles and the platform smoke check sees a cube. The first generic marts and
their Cube definitions arrive in Roadmap Phase 4 (see [ROADMAP.md](../../ROADMAP.md)).
Under [ADR 0037](../../docs/adrs/0037-build-marts-in-place-in-the-serving-warehouse.md),
Cube will read marts directly from the client's serving schema.

## Service configuration

[`cube.js`](cube.js) rejects unknown environments and any backend other than
Snowflake (`CUBEJS_DB_TYPE=snowflake`). Ordinary local and CI startup compiles
metadata without warehouse credentials. The stack runs one Cube API service with
an in-memory cache; the refresh worker and Cube Store return with the first
pre-aggregation ([ADR 0014](../../docs/adrs/0014-govern-metrics-with-cube-core.md)).

Queries use the reporting time zone (`ANALYTICS_REPORTING_TIME_ZONE`, default
`America/Los_Angeles`), replacing any time zone the client sends. dbt dates facts
with the same variable and default.

The runtime image runs as UID 10001, like the Python services, because remote
secret files are readable only by that UID; `scripts/platform <target> smoke`
checks it.

## Checks

The package check runs TypeScript over maintained JavaScript configuration,
tests and smoke helpers, then runs the offline tests. It enables `checkJs`,
`strict`, `noUncheckedIndexedAccess` and `exactOptionalPropertyTypes`, with no
emitted code. JSDoc checks configuration against the public Cube types supplied
by the pinned runtime image. `skipLibCheck` skips checking third-party
declaration internals; their exported types still check our code.
Expected-error examples in `tests/typing.cjs` guard against weakening the
checker. TypeScript strict mode rejects implicit `any`, but does not prohibit
explicit `any` annotations.

`tests/model.test.cjs` compiles the real model with the pinned Cube schema
compiler. It fails when the model exposes no cube or view, when a public cube,
view or member has no `description`, or when a member breaks the PII rule below.
It does not validate the meaning of model SQL or run queries against a warehouse.

The platform smoke check fails when Cube metadata lists no cubes or views, so a
release whose model fails to load fails verification.

## Personal data

Each dimension, measure and segment declares `meta: { pii: false }` or
`meta: { pii: true }`, including hidden members. Until the platform has named-user
authentication, approved-field access and minimal auditing, no Cube member may
hold restricted PII. See the
[platform policy](../../engineering-standards.md#personal-data-classification-and-cube-exposure).

See [metric governance](../../docs/adrs/0014-govern-metrics-with-cube-core.md)
and [package isolation](../../docs/adrs/0008-isolate-package-dependencies.md).
