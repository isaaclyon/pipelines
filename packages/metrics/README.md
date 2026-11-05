# Metrics

Owns Cube semantic definitions and service configuration. The pinned Cube image
supplies the service and database drivers. `package.json` and its lockfile pin
the configuration's own runtime dependency, `yaml` (installed into the runtime
image at build time), and TypeScript and Node types for the check image.

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

Startup stops with an error before Cube listens when any check below fails.

### Reporting time zone

Queries use the client's reporting time zone, replacing any time zone the
client sends. [`reporting-timezone.cjs`](reporting-timezone.cjs) reads
`reporting_time_zone` from the client's `client.yaml`, the file every service
reads ([ADR 0030](../../docs/adrs/0030-ship-a-toolkit-with-one-deployment-per-client.md)).
The path is `ANALYTICS_CLIENT_CONFIG`, default `/etc/analytics/client.yaml`.
Cube reads only `version` and `reporting_time_zone`; the client-config package
validates the rest of the file. Startup fails, naming the file, when the file
is missing or is not valid YAML, when `version` is not `1`, or when the time
zone is missing or is not an IANA time zone known to Node.

### Client model folder

Cube loads two folders as one model through Cube's
[`repositoryFactory`](https://cube.dev/docs/reference/configuration/config#repository_factory)
option ([`model-repository.cjs`](model-repository.cjs)):

| Folder | Image path | Contents |
| --- | --- | --- |
| Toolkit | `/cube/conf/model` | This package's [`model/`](model/) |
| Client | `/cube/conf/client/model` | The client's own Cube definitions |

A client's metrics image is built `FROM` the pinned toolkit metrics image and
copies its definitions into the client folder, for example
`COPY --chown=analytics:analytics model /cube/conf/client/model`. Without that
folder the service has no client models.

Both folders follow Cube's own rules: subfolders are allowed, and Cube compiles
`.js`, `.yml`, `.yaml`, `.jinja` and `.py` files. Each file's name is its path
relative to its folder, so a client file can refer to toolkit cubes by name.
The client model follows the same PII and description rules as the toolkit
model. Cube itself keeps the last definition of a repeated name without an
error, so startup fails, naming both files, when:

- a client file has the same relative path as a toolkit file, such as
  `cubes/orders.yml` in both folders; or
- two files define a cube or view with the same name, in either folder.

When a toolkit upgrade adds a name a client already uses, rename the client's
file or definition. The service reads and checks both folders once at startup
and serves exactly the files it checked.

### Environment variables and mounted files

The metrics service reads only these settings. The Cube image may read other
`CUBEJS_*` variables; the deployment sets none beyond this list.

| Setting | Required | Meaning |
| --- | --- | --- |
| `ANALYTICS_ENVIRONMENT` | Yes | `local`, `ci`, `development` or `production` |
| `ANALYTICS_CLIENT_CONFIG` | No | Absolute path of `client.yaml`; default `/etc/analytics/client.yaml` |
| `CUBEJS_DB_TYPE` | Yes | Must be `snowflake` |
| `CUBEJS_API_SECRET` | Yes | Secret that signs Cube API tokens |
| `CUBEJS_DEV_MODE` | Set to `false` | Turns off Cube's development mode |
| `CUBEJS_TELEMETRY` | Set to `false` | Turns off Cube telemetry |
| `CUBEJS_CACHE_AND_QUEUE_DRIVER` | Set to `memory` | In-memory cache and queue, with no Cube Store |
| `CUBEJS_REFRESH_WORKER` | Set to `false` | No refresh worker until the first pre-aggregation |
| `/etc/analytics/client.yaml` | Yes | The client's `client.yaml`, mounted read-only (or the `ANALYTICS_CLIENT_CONFIG` path) |
| `/cube/conf/client/model` | No | The client's Cube definitions, baked into the client image |

Snowflake connection settings arrive with the first mart that Cube queries.

The runtime image runs as UID 10001, like the Python services, because remote
secret files are readable only by that UID; `scripts/platform <target> smoke`
checks it.

## Checks

The package check runs TypeScript over maintained JavaScript configuration,
tests and smoke helpers, then runs the offline tests. The tests read the test
configuration in [`tests/client.yaml`](tests/client.yaml).
`tests/config.test.cjs` covers startup checks and the reporting time zone;
`tests/model-folders.test.cjs` covers loading the client folder and rejecting
repeated file paths and names. It enables `checkJs`,
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
