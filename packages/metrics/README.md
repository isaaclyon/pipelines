# Metrics

Owns Cube semantic definitions and service configuration over published dbt
marts. The pinned Cube image supplies the service and database drivers;
`package.json` and its lockfile pin TypeScript and Node types for the check image.

```sh
./scripts/platform local check metrics
./scripts/platform local smoke
```

## Bounded synthetic local demo

This demo serves **deterministic fixtures, not real sales**, through the same
maintained Cube definitions. It does not implement real-data acquisition or the
per-worktree refresh lifecycle. Run from the repository root:

```sh
# Build the pinned check image with the demo and current models.
./scripts/platform local compose --profile checks build metrics-check
# New disposable directory: never point this at an existing data file.
mkdir -p .scratch
DEMO_DIR=$(mktemp -d "$PWD/.scratch/cube-demo.XXXXXX")
chmod 777 "$DEMO_DIR" # synthetic fixture only; permits the container user to write
export CUBEJS_API_SECRET=$(openssl rand -hex 32)
docker run --rm --network none --entrypoint node \
  -v "$DEMO_DIR:/fixture" analytics-metrics-check:local \
  tests/local-demo.cjs prepare
# The service automatically stops after 180 seconds.
docker run --rm -d --name cube-fixture-demo \
  -p 127.0.0.1:14061:4000 -e CUBEJS_API_SECRET --entrypoint node \
  -v "$DEMO_DIR:/fixture:ro" analytics-metrics-check:local \
  tests/local-demo.cjs serve
# Wait for readiness, bounded to 20 seconds; inspect logs on failure.
for i in $(seq 1 20); do
  curl --fail --silent --max-time 1 http://127.0.0.1:14061/readyz >/dev/null && break
  sleep 1
done
docker exec cube-fixture-demo node tests/local-demo.cjs query
docker stop -t 10 cube-fixture-demo
unset CUBEJS_API_SECRET
```

The authenticated query prints and checks `net_sales_usd: 70`, `aov_usd: 48`
and `asp_usd: 9.5`. Requests without authentication receive HTTP 403. Startup
checks the exact attached catalog/file, read-only state, fixture publication
marker and known order relation before opening HTTP. Both the file mount and
DuckDB attachment are read-only; the stock driver uses an in-memory connection.
An invalid or missing fixture stops startup. No writer runs beside serving.
The temporary directory contains synthetic data only; no credentials are saved
there. Stop the container before removing that directory.

The ordinary local scaffold and remote service remain Snowflake-configured.
Governed AOV/ASP consumers are default off and accepted only with explicit local
DuckDB selection; this demo makes that selection internally. It is not evidence
of shared Snowflake readiness or permission to use private data.

Run commands from the repository root. Configuration checks reject unknown
environments and mismatched backends. The stack runs one Cube API service with
an in-memory cache; the refresh worker and Cube Store return with the first
pre-aggregation ([ADR 0014](../../docs/adrs/0014-govern-metrics-with-cube-core.md)).

The package check runs TypeScript over maintained JavaScript configuration,
tests, and smoke helpers, then runs the offline configuration and read-only
smoke check tests. It enables
`checkJs`, `strict`, `noUncheckedIndexedAccess`, and
`exactOptionalPropertyTypes`, with no emitted code. JSDoc checks configuration
against the public Cube and DuckDB driver types supplied by the pinned runtime
image. `skipLibCheck` skips checking third-party declaration internals; their
exported types still check our code. Expected-error examples in
`tests/typing.cjs` guard against weakening the checker.

The existing CI metrics check runs this same gate. TypeScript tooling stays in
the checks image. Semantic models and SQL need separate Cube validation as they
are introduced; this gate does not validate their meaning. TypeScript strict
mode rejects implicit `any`, but does not prohibit explicit `any` annotations.

## Worktree-local copy and serving
The bounded local command copies an **already built and validated** orders-group
DuckDB database, exports only approved model dependencies, and serves that new
file for up to 180 seconds. It does not fetch sources, run dbt, activate customers,
or authorize private-data access. The default scaffold remains unchanged.

The source owner must checkpoint and close the chosen database first. After
separate approval to read and copy that prepared database:

```sh
scripts/platform local cube build
export CUBEJS_API_SECRET=$(openssl rand -hex 32)
# Stop every client of the source database first. Do not reopen it during copying.
export LOCAL_SOURCE_STOPPED=true
scripts/platform local cube refresh /absolute/approved/source.duckdb local_marts /absolute/approved/privacy.json
scripts/platform local cube query
scripts/platform local cube stop
unset CUBEJS_API_SECRET LOCAL_SOURCE_STOPPED
```

`privacy.json` maps source relation names to column names and authored boolean
PII classifications, for example `{"fct_orders":{"is_aov_eligible":false}}`.
That example is incomplete. Supply every required input column's classification
from evidence applicable to **this source file**. Never generate false defaults
for private or historical inputs. Missing, unknown or restricted classifications
on required columns refuse the export. Additional columns are never copied.
The compiler-derived dependency list is available from
`local-export.cjs:requiredLocalColumns()`; it includes hidden keys, metric filters
and the base `is_test` predicates. Only approved geography columns survive under
the legacy `dim_order_contacts` name. Raw tables and other contact fields do not.

The command owns a path-specific image and container. Set `LOCAL_CUBE_PORT` to a
free port when running several worktrees; the default is 14061, bound only to
127.0.0.1. New private directories under `.scratch/local-cube/` hold independent
working copies, provenance and analytical exports. Copies never overwrite an
existing database. A local operation lock refuses concurrent refresh/stop calls;
a lock left after a killed host command needs operator inspection before removal.
Stop all unmanaged source clients and keep them stopped until refresh finishes.
Serialize copies from the same source. The host must have `lsof`; an application
source/WAL handle, inspection error or missing tool refuses refresh before any
copying. The protected macOS Virtualization service's cached filesystem
transport is not a database client. Instead, any running container mounting the
source or an ancestor directory causes refusal, even with a read-only mount.
Docker Desktop `/host_mnt/` bind paths are checked as host paths; a bind path
that does not exist on the host, such as Docker Desktop's socket proxy, is a VM
path and cannot reach the source. Any other uninspectable bind mount refuses.
This conservative check can reject an unrelated container; the command reports
its ID but never stops another container automatically. Docker Desktop locks are
not reliable between host and container, or between different bind mounts.
The source is mounted read-only and is never opened writable or checkpointed.
A missing source or any WAL recovery file causes refusal with owner preparation
guidance; the command never repairs the source. Source identity, size, modification
time and WAL absence are checked across copying. Only the private working copy
is opened for validation/export. Do not open another client or start a
source-mounting container during refresh.
Provenance records source path, size and modification time, worktree, branch and
copy time.

Refresh stops the previous worktree service first. Every later copy, privacy,
export or startup failure leaves it stopped, with no old-file fallback. Successful
exports have a new publication ID on every relation and marker. Startup verifies
the exact attached file/catalog, read-only state, marker and complete column
allowlist before opening HTTP. The service mounts only that analytical file,
read-only, never the working database or its parent. Private files remain after
stop for inspection; this command does not delete previous experiments.

Run `scripts/test-local-cube.sh` for the executed synthetic copy/export/start/query
and refusal checks, including a held host file descriptor and a different
container holding the host-mounted source. It uses an automatically
assigned loopback port and refuses to interrupt an existing worktree service. Expected
fixture results are net sales 70, AOV 48 and ASP 9.5. These checks do **not** establish
real-data completeness, business acceptance or Snowflake readiness. The remaining
input is the explicitly approved database path, its marts schema, applicable
privacy declarations and evidence that its dbt build/tests passed. Source fetching
and orchestration of a new dbt build remain outside this bounded command.

## Publication readers
Remote services use Snowflake. The accepted
[local-first direction](../../docs/adrs/0028-develop-locally-before-shared-snowflake-release.md)
uses the same definitions for the bounded local command above, with a separate
analytical-only file and stop/validate/restart. The broader
[refresh scope](../../docs/specs/20261101-local-first-development.md)
keeps source refresh and dbt build validation with their owning components.
Snowflake acceptance is deferred until shared activation, not supplied by local
results. Ordinary local/CI scaffold startup compiles metadata without warehouse
credentials; it does not serve a DuckDB publication. No custom snapshot driver
or live replacement-file lifecycle is used.

Every maintained cube reads its table names and refresh key from
`publishedMarts()` in [`publication.cjs`](publication.cjs). Cube compiles model
files in a sandbox without `process`, so model files `require` this module from
the configuration directory through Node. It reads the backend from
`CUBEJS_DB_TYPE` and the published schema from `ANALYTICS_MARTS_SCHEMA`. Local
and CI default it to `<environment>_marts`; deployed targets must set it in
`settings.env`, because the publisher refuses production-like namespaces. The refresh key reads
the one-row `_publication(publication_id, published_at)` table in that schema.
The publisher sets a new UUID and UTC timestamp for each successful publication.
The helper selects `publication_id` from `<schema>._publication` with
`every: '1 second'`. Queries require a published schema and Snowflake access.

This refresh is demand-driven and eventually invalidates Cube's in-memory query
cache. The first request after publication can return cached old results. One
second is a refresh-key check interval, **not a first-request freshness SLA**.
Independent queries or charts can observe different publications during
convergence. There are no pre-aggregations yet.

## Publication-linked availability
The `data_availability` view reads `fct_data_availability`, captured in the same
publication as the order facts. It reports shop-scoped Shopify connected-window
bounds, independent Loop snapshot evidence, environment and known limitations.
It never joins status to order facts. Its SQL requires exactly one publication
marker with a timestamp and an ID matching the captured evidence. An invalid
marker yields no status rows; that is not a successful empty answer.

Availability queries always use UTC, overriding a client-supplied timezone.
Cube serializes these time dimensions without an offset; interpret them as UTC.
Reporting queries still use the reporting time zone
(`ANALYTICS_REPORTING_TIME_ZONE`, default `America/Los_Angeles`). Mixed availability/reporting member
queries are rejected so that source instants cannot become ambiguous local times
at daylight-saving transitions.

### Consumer guarantee: eventual consistency

Use ordinary Cube queries for revenue and availability. Consumers do not need
publication IDs, generation comparisons, or publication-mismatch retries.
Independent queries may temporarily return different publications while caches
converge. Cached answers are allowed; the API does not promise the newest
publication or a shared snapshot across requests.

Availability describes captured pipeline coverage and source cutoffs. It does
**not** certify the age or completeness of separately queried revenue. Do not
label revenue with the availability query's cutoff as if they were one response.
Assess the evidence's age at query time and inspect eligibility coverage
separately; neither a recent cutoff nor matching diagnostic IDs proves complete
revenue. Missing evidence and empty periods must not silently become zero or a
claim of completeness. API errors remain errors.

Publication ID dimensions remain available for diagnostics and compatibility,
but are optional. The former `consistentPublicationPair(load)` helper is retired;
its callers should make ordinary Cube requests rather than replace its retry
loop. No shared client or coordinated endpoint is required.

The morning reporting deadline and 30-minute end-to-end freshness target remain
unchanged. They require measurement of user-visible facts through Cube; cache
refresh intervals and the separate availability query do not establish them.
This change neither proves those targets nor adds historical-generation pinning.

## dbt exposures
[`exposures.cjs`](exposures.cjs) compiles the Cube model offline with the
pinned Cube schema compiler and prints one dbt exposure per public view. It
compiles the model with `SHOPIFY_CUSTOMERS_ENABLED` off and on. A view that
differs between the two gets one exposure per setting, each enabled only under
its own setting. Each exposure refs the marts
the view reads and lists every field's mart columns, in a description table
and in `meta.cube_fields`. The transformations check validates those columns
against the mart declarations.

The generated `cube_model_classifications` entries are validation-only
inventories of all Cube dimensions, including hidden and unused dimensions.
They are not queryable public views. Their dependencies also appear in dbt
lineage so direct aliases can be checked against mart classifications.

The output is checked in to
[`_cube__exposures.yml`](../transformations/dbt/models/exposures/_cube__exposures.yml);
Cube-to-dbt lineage flows only through that file; dbt column metadata flows the
other way through the [mart-contract snapshot](#dbt-column-metadata).
After a Cube model change, or a dbt description change that a Cube field reads,
regenerate it:

```sh
scripts/platform local exposures
```

`tests/exposures.test.cjs` fails when the checked-in file differs from the
generated output. CI runs the metrics check when either the model or the
file changes.

## dbt column metadata
dbt YAML owns each published column's description, type and `pii` flag
([ADR 0014 addendum](../../docs/adrs/0014-govern-metrics-with-cube-core.md#addendum-2026-10-22)).
[`mart-columns.cjs`](mart-columns.cjs) reads them from the transformations
package's reviewed snapshot,
[`customer-mart-contracts.json`](../transformations/dbt/customer-mart-contracts.json),
which the image copies to `/cube/conf/mart-contracts.json`. Model files call
`dbtDimensions(mart, columns)` for dimensions that read a mart column under its
own name: every `customers` dimension and the direct-column dimensions of
`shopify_orders`. The call fails for a column outside the snapshot, an unmapped
dbt type or a restricted mart.

Cube still owns the column allowlist, computed and renamed dimensions (such as
`order_date` and the order geography), measures, joins and views. The
`revenue_coverage` dimensions keep their coverage-specific wording.

To change a field's wording, edit the dbt YAML, regenerate the reviewed
snapshot (see the transformations
[review proposals](../transformations/README.md#generated-review-proposals)) and
the [exposures](#dbt-exposures). `tests/mart-columns.test.cjs` fails when
a direct-column dimension of those cubes differs from the snapshot. CI runs the
metrics check when the snapshot changes.

## Independent publication readers
`ANALYTICS_MARTS_SCHEMA` names the order group, including availability evidence.
Set `ANALYTICS_CUSTOMERS_MARTS_SCHEMA` to a **distinct**, validated customer schema
when migrating to separate publications. The customer model uses its own
`_publication` marker and Cube data source (`customers`). On Snowflake it uses
the same dedicated reader identity/database, not another set of credentials.
No common generation is promised across groups; orders and availability remain
together. Each data source refreshes from its own Snowflake marker.

A blank customer schema setting preserves the legacy combined reader during
migration. Prepublish both new destinations and verify grants before switching
settings. See the [publisher migration procedure](../transformations/README.md#independent-jobs-and-migration).
Synthetic API fixtures exercise combined and independent schema routing; they
do not implement the new local serving lifecycle or establish shared Snowflake readiness.
No inventory Cube model is introduced by this change.

## Semantic model

[`model/orders.js`](model/orders.js) publishes the `orders` view over
`fct_orders` and a geography relation, joined one-to-one on
`(shopify_shop_id, shopify_order_id)`. With `SHOPIFY_CUSTOMERS_ENABLED=true`,
Cube reads city, postal code, country and province from analytical
`dim_order_geography`. It cannot read restricted `dim_order_contacts`.
The order-only configuration reads the same approved geography from contacts.
Customer activation preserves these fields; geography consumers do not migrate.
Public source identities are `orders.shopify_shop_id`,
`orders.shopify_order_id` and `orders.shopify_customer_id`.
These names retain Shopify's source identity.
The view exposes the `fct_orders` channel and order-characteristic flags
(`is_d2c`, `is_commercial`, `is_retail`, `is_specialty`, `is_bulk`, the
characteristic flags and `has_classification_evidence`) as boolean dimensions.
Null means the evidence is unavailable; filter on `true` or `false` explicitly.
Money measures sum shop-currency (USD) amounts;
presentment amounts mix currencies and are not summed.
`tests/orders-model.test.cjs` runs the real model in the pinned Cube server
over a disposable synthetic database, without a publication lifecycle.

[`model/order-lines.js`](model/order-lines.js) adds source-qualified line
reporting and ASP (average selling price). Order-grain AOV (average order value)
stays in `orders`. Both ratios divide sums over metric-specific eligible
populations and return null for a zero denominator. See the
[governed revenue interface](model/README.md#governed-revenue)
for default net units, coverage and shared filters. These new members require
the explicit local-only gate; publication and reader checks remain required
before any later shared Snowflake activation, not for using the synthetic demo.

Every public view, measure and dimension has a `description`, including members
generated in code. Both model tests fail when Cube `/meta` returns a public
member without one (`tests/documentation.cjs`).

[`model/customers.js`](model/customers.js) publishes the `customers` view over
the hidden cube `shopify_customers`, which selects an explicit column allowlist
from `dim_customers`. The view exposes Shopify source identities, profile
status fields, Shopify's own order-count and lifetime-spent summaries (the
amount and currency code as dimensions, never summed because currencies can
differ), and default-address city, postal code, country, province and time zone.
The city and postal dimensions are `customers.default_address_city` and
`customers.default_address_postal_code`. There are no lifetime-value measures
or segments. Customer names, email, phone, notes, tags, metafields, street lines
and coordinates are not modeled: they appear in no cube, view or generated SQL.
Cube has no authorization policy for those restricted fields, so they remain
unavailable through Cube until a policy is implemented
and tested. With the legacy combined reader, `shopify_orders` joins
`shopify_customers` many-to-one on `(shopify_shop_id, shopify_customer_id)`;
the `orders` view does not expose customer fields. Independent customer
publication removes this hidden cross-group join; public views keep their names. `tests/customers-model.test.cjs` proves the metadata allowlist
and the denial of guessed restricted members, including hidden cube and join
paths, on the pinned Cube server.

### PII publication gate
Each authored dimension, measure and segment declares `meta: { pii: false }`
or `meta: { pii: true }`. This includes hidden members and members generated
from an explicit reviewed list. Dimensions from `dbtDimensions()` take the
reviewed dbt `config.meta.pii` flag. Included view members retain their origin's
metadata. Do not supply a catch-all false default for new members.

The ordinary metrics check compiles the real model with customers both disabled
and enabled. `exposures.cjs` rejects missing/non-boolean classifications and any
restricted PII member, including hidden members and members reachable by joins.
It exports explicit classifications and direct-column mappings into dbt exposures;
transformation checks reject contradictory direct-column declarations before
publication. Derived expressions and aggregates require their own authored
classification, with no automatic lineage propagation. Keep the generated file
current with `scripts/platform local exposures`.

This is a build/publication gate, **not per-user authorization**. Existing
customer allowlists, restricted reader identities and runtime denial tests remain
in place. The platform must deliver trusted personal sign-in, reviewed grants,
approved-field-only access and minimal auditing before privileged PII querying
can be enabled. A shared token or `pii: false` label is not an access approval.

Existing customer IDs, city/postal geography and customer-level spending remain
explicitly classified outside the initial restricted category. The data owner
approved the initial classifications and existing exposure. They are not anonymous. See the
[platform policy](../../engineering-standards.md#personal-data-classification-and-cube-exposure).

The platform smoke check fails when Cube metadata lists no cubes or views, so a
release whose model fails to load fails verification.

The ordinary metrics check retains bounded synthetic query fixtures using the
vendor DuckDB driver directly. They check semantic results, joins, metadata,
restricted-member denial and marker interpretation, not serving or file replacement.
They do not prove Snowflake permissions, publication atomicity or cache lifecycle;
those remain deferred shared-release checks requiring isolated development
acceptance. No custom DuckDB driver remains; the local-first design reuses the
vendor driver without live snapshot replacement.

Remote services use Snowflake configuration and runtime key mounts. The runtime
image runs as UID 10001, like the Python services, because remote secret files
are readable only by that UID; `scripts/platform <target> smoke` checks it.
Snowflake semantic behavior has not been exercised by the offline scaffold checks.

See [metric governance](../../docs/adrs/0014-govern-metrics-with-cube-core.md)
and [package isolation](../../docs/adrs/0008-isolate-package-dependencies.md).

Customer metadata and the orders-to-customers join activate only with
`SHOPIFY_CUSTOMERS_ENABLED=true`. The default stays disabled until customer
marts exist. Snowflake activation also requires `ANALYTICS_MARTS_READER_ROLE`
to match `CUBEJS_DB_SNOWFLAKE_ROLE` and differ from
`ANALYTICS_MARTS_WRITER_ROLE`. Set `SNOWFLAKE_READER_USER` and `SNOWFLAKE_READER_PRIVATE_KEY_HOST_PATH`
to a dedicated service user and its own key pair. Compose mounts only that key
in Cube. The guard rejects a shared user, a shared key path and the
writer key mount. A partial reader configuration fails even with customers disabled.
Leave the reader settings unset to retain the legacy order-only credentials.
Omit unused host-path entries for compatibility with previous release validators;
set the user, key path and role together when migrating. The reader user must have only the analytical reader role,
with no inherited restricted, raw or writer access. Verify warehouse denials
and rejection of `USE ROLE <writer>` with that user's credentials before activation;
configuration checks names and paths, not account grants or key contents. When enabled,
`snowflake-reader-driver.cjs` runs `USE SECONDARY ROLES NONE` on every new or
reconnected session before application queries. If the command fails, the
session closes and no application query runs. This adds defense in depth against unintended secondary-role activation.
The pinned-driver tests cover initial connections, reconnection, failure and
connection checks; live session behavior is not exercised through Cube.

### Dedicated reader acceptance

The acceptance run uses a disposable `SERVICE` user and its own RSA key. The
actual Cube driver must read the analytical fixture and reject restricted/raw
reads, writes and `USE ROLE <writer>`, including with secondary roles enabled.
The fixture removes its user, roles, schema and key. This verifies the fixture
credentials, not a persistent deployed reader.

`tests/snowflake-reader-acceptance.test.cjs` stays skipped in ordinary checks.
Its administrator fixture creates a disposable development `SERVICE` user,
a separate RSA key, two roles and synthetic tables. The reader receives only
analytical SELECT access. The actual Cube driver must deny restricted/raw
reads, writes and `USE ROLE <writer>`, even with secondary roles enabled.
The fixture removes its user, roles, schema and key in `finally`.
It never provisions the persistent deployment identity.

Select an individual administrator identity and key explicitly. Prepare an
ignored environment file with these non-secret connection settings:

```dotenv
ANALYTICS_ENVIRONMENT=development
CUBE_READER_ACCEPTANCE=development
CUBEJS_DB_TYPE=snowflake
CUBEJS_DB_NAME=<development database>
CUBEJS_DB_SNOWFLAKE_WAREHOUSE=<development warehouse>
CUBEJS_DB_SNOWFLAKE_ACCOUNT=<development account>
CUBEJS_DB_USER=<individual administrator>
CUBEJS_DB_SNOWFLAKE_AUTHENTICATOR=SNOWFLAKE_JWT
CUBEJS_DB_SNOWFLAKE_PRIVATE_KEY_PATH=/run/secrets/admin_private_key
```

After explicit development authorization, run the bounded fixture in the
current metrics check image. It needs `SYSADMIN` for disposable schemas and
`SECURITYADMIN` for disposable users and roles. Mount only the selected
individual key; the fixture generates the reader key inside the disposable
container. The command needs root inside this test container to create the
private `/run/secrets` file; deployed Cube still runs as UID 10001.

```sh
docker run --rm --user 0 --env-file /path/to/ignored-reader-acceptance.env \
  --mount type=bind,source=/path/to/individual-key,target=/run/secrets/admin_private_key,readonly \
  --entrypoint node analytics-metrics-check:local \
  tests/snowflake-reader-acceptance-fixture.cjs
```

Agree a finite execution budget before running this live check. Ordinary
package checks type-check the fixture without executing it.
