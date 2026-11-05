# Docker Compose deployment

Owns container builds, service wiring, and explicit environment selection.
The platform dispatcher requires host Python 3.10 or newer (standard library only).
Use the repository-root `scripts/platform` dispatcher; `help` lists commands.

| Target | Files | Backend |
| --- | --- | --- |
| local | compose.yaml + local.yaml | Offline fixtures and metadata-only scaffold |
| ci | compose.yaml + local.yaml + ci.yaml | Offline checks; Compose services publish no ports |

The `local` target reads the ignored `deploy/compose/.env.local` when it exists
and otherwise the fixture-only `.env.local.example`; the `ci` target always uses
the example. Set `ANALYTICS_ENV_FILE` to choose another file.
Each checkout gets its own local stack, named `analytics-<checkout folder>`
(for example `analytics-pipelines`), so worktrees do not share volumes or a
network. Stacks started before this default were named `analytics-local`; set
`ANALYTICS_PROJECT=analytics-local` to keep using their volumes. Set
`ANALYTICS_PROJECT` to choose another name, and
`DAGSTER_PORT`/`CUBE_PORT`/`DOCS_PORT` to select different local ports.

## One configuration path for code locations and runs

Declare runtime settings and mounts once on the owning Compose service.
`scripts/platform` resolves Compose, then `scripts/run_context.py` generates
Dagster's container context for services with `DAGSTER_CURRENT_IMAGE`.
Run containers receive the same explicit environment values and volume targets,
including read-only mounts and environment-specific named volumes. JSON values
and literal dollar signs survive serialization. Do not maintain embedded
`DAGSTER_CONTAINER_CONTEXT` JSON or add source settings to the control plane.
The launcher's shared environment values remain launcher-owned; its metadata
password is never copied into the generated context.

Nonempty `*_PATH` and `*_ROOT` settings must point inside a declared mount.
Missing mounts, unsupported mount options and manually supplied contexts fail
configuration rendering before services change. Source-specific required values
and activation checks stay in the owning package. Settings are not credentials:
keep credential contents in mounted files, not environment values, because
Dagster stores the generated context as location metadata.

Use `scripts/platform`, including its `compose` command, rather than invoking
Docker Compose directly. The raw YAML intentionally has no run-container context.
Rendering uses an owner-only temporary file and removes it after the command.
`config` can print resolved settings, as before; use `config --quiet` for checks.

## Run concurrency

Local and CI set `DAGSTER_MAX_CONCURRENT_RUNS=1` in service configuration to
protect shared DuckDB files. A shared Snowflake deployment can select two in
its env file. Without the setting, capacity stays at one. Values above two, zero,
unlimited capacity, and a local/CI value of two fail at Dagster startup.
Increasing the supported capacity requires a measured, reviewed change.

The shared Dagster configuration serializes jobs with the same
`analytics/exclusive-resource` run tag. Independent checks and other datasets
can overlap on Snowflake. Same-dataset ingestion remains exclusive through both
queue admission and the owning package's recovery lock. See the
[control-plane guide](../dagster/README.md) for the execution contract.

## Commands

```sh
./scripts/platform local config --quiet
./scripts/platform local check
./scripts/platform local up
./scripts/platform local smoke
./scripts/platform local down
```

Application images contain installed packages rather than editable source.
Checks use separate images and run offline. The transformation image includes
git for dbt's runtime executable checks. It also includes a separately locked
`/opt/lineage` environment for Colibri; incompatible SQL parser dependencies do
not enter the Dagster/dbt environment. Ingestion does not install that tool.

In the transformation image, dbt writes its artifacts to
`/var/lib/analytics/artifacts/dbt` and its logs to
`/var/lib/analytics/artifacts/dbt-logs`, on the artifacts volume. The docs site
and Cube do not mount that volume. dbt errors can contain data values, so do not
serve these files or copy them into tickets.

The two Python package checks run concurrently, each in its own container with
a 2 GiB memory limit; metrics runs after both finish. Every package's status is
collected, so one failure cannot hide another. Output is grouped by package.
Focused checks still run only the selected package.

GitHub runs only a secret scan (`.github/workflows/secret-scan.yml`); every
other check runs locally. `./scripts/platform ci check --base origin/main`
asks the change router (`scripts/ci_docs.py`) which checks the working tree
needs compared with that commit, including uncommitted and untracked files.
It checks links in the changed documentation and runs only the selected
package checks. When the router also selects integration, it says to run
`./scripts/platform ci verify`. Naming a package still runs that package, using
the router's dbt selection:

| Changed paths | Required lanes |
| --- | --- |
| Recognized documentation only | Documentation and script validation |
| `packages/NAME/tests/` only | Selected package checks |
| Other files under `packages/NAME/` | Selected package checks plus full integration |
| Shared tooling, deployment, unknown paths, or missing history | All packages plus integration |

Within the transformations check, tests that execute the real dbt project run
only when the change can affect them ([ADR 0026](../../docs/adrs/0026-scale-dbt-checks-with-the-change.md)).
The router chooses the dbt tests for that check:

| Changed paths | dbt project tests |
| --- | --- |
| No dbt-affecting path, such as Dagster, ingestion, metrics, or deployment timing | None |
| dbt `models/`, `macros/`, `tests/`, `seeds/`, `snapshots/`, `analyses/`, or package Python | Changed: tests whose dbt nodes or imported test and package code differ from the PR base |
| Other dbt project or package files, dependencies, the check image, or check scripts | All |
| Missing history | All |

For changed dbt tests, the check extracts the base commit's
`packages/transformations` into a temporary folder and compares with it.
Without `--base`, `ci check` and `ci verify` run every package and every dbt
test and check every Markdown link. Run that full check before merging a
dbt-affecting change to `main`; nothing else runs it (ADR 0026).

Mixed changes take the union of required lanes; renames consider both paths.

Dagster code-location and daemon checks run every 60 seconds after each check
finishes, with three consecutive failures marking a service unhealthy. These
CLI checks load Python packages and can consume a full CPU thread for several
seconds even on an idle stack. Readiness and liveness tests, timeouts, and
startup grace periods are unchanged. Failure detection takes roughly three to
four minutes, including check time and timeouts.

Local/CI Cube uses `/livez`; its smoke check separately compiles `/meta`.
Remote Cube retains `/readyz`, including warehouse connection readiness.
This is deliberate: the offline scaffold does not serve business queries.

All health checks probe every second during their start period in every
environment, so `up --wait` finishes as soon as services are ready. They return
to their steady-state intervals after startup. Docker Engine 25 or newer is
required for `start_interval`. Serialized pipeline writes are unchanged.
Local commands build by default.
Only use `--no-build` after building the current checkout; local tags do not prove
freshness. The package command requires selected prebuilt images and disables pulls.

On macOS, Docker's credential helper must be on the shell path. If pulls fail
with `docker-credential-desktop` missing, add Docker Desktop's
`/Applications/Docker.app/Contents/Resources/bin` to the path.

On Linux, set `DOCKER_SOCKET_GID` to the group that owns `/var/run/docker.sock`
(use `stat -c '%g' /var/run/docker.sock`). The control-plane containers use this
supplementary group to launch runs as non-root users.

## Local validation before pushing

Run relevant checks locally and fix failures before pushing; GitHub does not run
them. For package changes, start with
`./scripts/platform ci check --base origin/main`, which runs the checks the
change needs, or name a package: `./scripts/platform ci check ingestion`
(or `transformations` / `metrics`).
For container, dependency, deployment, or orchestration changes, run the full
flow from the repository root:

```sh
./scripts/platform ci verify
```

ShellCheck must be installed on the host.
Shared script lint and regression checks live in `scripts/validate-scripts.sh`,
called by this command. This includes CI routing tests.
The command runs lint, configuration validation, package checks, startup, and
smoke tests. It detects the Linux Docker socket group unless already set, prints
service logs, and removes services on exit while preserving the failing exit
status. Cleanup also removes the project's Dagster run containers. The `ci`
Compose services publish no ports and use the `analytics-ci` project by default,
which retains its named volumes. Set `ANALYTICS_PROJECT` to a distinct name for
simultaneous CI stacks; cleanup removes that project's services and named volumes.

Local checks use the Docker host's native architecture, so an ARM64 Mac does not
verify AMD64 behavior.
Report any checks that could not run. Documentation-only changes need path,
link, and instruction-symlink checks rather than starting the stack.

## Documentation-only changes

Changes confined to recognized
documentation paths check local Markdown link targets and tracked symlinks without
building images or starting services. The allowlist covers Markdown under `docs/`,
canonical agent instructions, package/deployment READMEs and instruction guides,
and the root README and engineering standards. Unknown paths, empty diffs, and
unavailable Git history run the full platform checks. Mixed changes select the
union of lanes described above. Both sides
of renames participate in routing.

Link checks inspect changed Markdown files (every tracked Markdown file when
there is no base), resolve instruction symlinks to their
canonical files, and validate local inline and reference targets. External URLs,
heading anchors, and fenced and inline code examples are excluded. This is an offline path
check, not a complete Markdown renderer or external link crawler.

Run the routing regression tests with `python3 scripts/test_ci_docs.py` and check
a commit range with `python3 scripts/ci_docs.py BASE HEAD` (or `BASE` alone
for the working tree). These lightweight
Git-based checks use the host's Python standard library and Git; package checks
continue to run in their isolated containers.

## Persistent state

| Volume | Owner | Recovery |
| --- | --- | --- |
| postgres | Dagster metadata | Back up and restore PostgreSQL before relying on remote operation |
| artifacts | Dagster artifacts and compute logs | Retain or back up required run evidence |
| ingestion-state | dlt pipeline working state; local raw DuckDB files, one per source | Define destination restoration and interrupted-load recovery per source. Local transformations mount it read-only (ADR 0029) |
| working | Ingestion/dbt writable DuckDB database | Rebuild from declared source checkpoints and transformations |
| docs | dbt documentation page for the docs site | Rerun the `dbt_docs` job |
| docs-state | Caddy's state | Caddy recreates it on start |

Startup initializes volume ownership. dbt creates its working file when needed.
`down` retains named volumes.

Local/CI Cube compiles metadata with Snowflake configuration but no warehouse
credentials. Scaffold smoke checks verify code loading, metadata and scaffold
runs, not business serving. Deterministic DuckDB checks run in package containers.
Snowflake acceptance tests run only against explicitly selected, isolated
development schemas before shared activation. See
[ADR 0028](../../docs/adrs/0028-develop-locally-before-shared-snowflake-release.md).

## Settings each service reads

A deployment reads settings from three places:

- **`client.yaml`** in the client repository (`CLIENT_DIR`): the sources to
  run, their schedules, the warehouse, the reporting time zone and alerts.
  These settings are not secret. `ingestion`, `transformations` and
  `cube-api` mount the file read-only at `/etc/analytics/client.yaml`
  (`ANALYTICS_CLIENT_CONFIG` names another path).
- **The env file** (`ANALYTICS_ENV_FILE`): passwords, API secrets and the host
  paths of secret files. It never enters Git or an image
  ([ADR 0032](../../docs/adrs/0032-read-secrets-from-environment-files.md)).
  The sample client's [`.env.example`](../../examples/sample-client/.env.example)
  lists every value.
- **Secret files** on the host, mounted read-only. Credential contents live
  only in these files, never in environment values.

### Set in your shell

| Setting | Default | Meaning |
| --- | --- | --- |
| `CLIENT_DIR` | `examples/sample-client` | The client repository: its `client.yaml`, dbt project and Cube files |
| `ANALYTICS_ENV_FILE` | See [above](#docker-compose-deployment) | The env file to read |
| `ANALYTICS_PROJECT` | `analytics-<checkout folder>`; `analytics-ci` for `ci` | Compose project, volume and network name |

`scripts/platform` sets `ANALYTICS_ENVIRONMENT` to the target (`local` or
`ci`), `DBT_TARGET=local` and `CUBEJS_DB_TYPE`. On Linux it also sets
`DOCKER_SOCKET_GID`. Do not set these yourself.

### Set in the env file

| Setting | Required | Read by | Meaning |
| --- | --- | --- | --- |
| `DAGSTER_POSTGRES_PASSWORD` | Yes | `postgres`, Dagster services and their runs | Password of Dagster's run-history database |
| `CUBEJS_API_SECRET` | Yes | `cube-api` | Signs Cube API tokens; at least 32 random characters |
| `COMPOSE_PROFILES` | No | Compose | `docs` starts the dbt documentation site |
| `DOCS_SITE_ADDRESS` | With `docs` | `docs` | Address Caddy serves, such as `http://:8080` |
| `DOCS_PASSWORD_HASH` | With `docs` | `docs` | Base64 bcrypt hash of the docs password; see [below](#dbt-documentation-site) |
| `SOURCE_SETTINGS_HOST_DIR` | To load sources | `ingestion` (local) | Host folder of source settings files, one per system; see [live local loads](#live-local-loads-adr-0029) |
| `SLACK_WEBHOOK_HOST_PATH` | With `alerts.slack` | `ingestion` (local) | Host file holding one Slack incoming-webhook URL |
| `DAGSTER_PORT`, `CUBE_PORT`, `DOCS_PORT` | No | Local target | Host ports (defaults 3000, 4000 and 8080) |

### Inside each container

Run containers that Dagster starts for a code location receive that
service's environment and mounts ([one configuration path](#one-configuration-path-for-code-locations-and-runs)).

| Service | Environment | Mounts |
| --- | --- | --- |
| `permissions` | None | Every named volume, to give UID 10001 ownership once |
| `postgres` | `POSTGRES_PASSWORD` from `DAGSTER_POSTGRES_PASSWORD` | `postgres` volume |
| `ingestion` | `ANALYTICS_ENVIRONMENT`, `DAGSTER_POSTGRES_PASSWORD`, `DBT_TARGET`, `DAGSTER_MAX_CONCURRENT_RUNS`, `DAGSTER_CURRENT_IMAGE`. Local adds `SLACK_WEBHOOK_PATH` and `LOAD__WORKERS=1` | `client.yaml`; `ingestion-state` and `artifacts` volumes. Local adds the settings folder at `/run/secrets/sources` and the webhook file at `/run/secrets/slack_webhook` |
| `transformations` | As `ingestion`, without the local additions. Local adds `DBT_RAW_ROOT` | `client.yaml`; `artifacts`, `working` and `docs` volumes. Local adds `ingestion-state`, read-only |
| `webserver`, `daemon` | The shared Dagster values, and `ANALYTICS_NETWORK` | Docker socket; `artifacts` volume |
| `cube-api` | `ANALYTICS_ENVIRONMENT`, `CUBEJS_DB_TYPE`, `CUBEJS_API_SECRET` and fixed Cube values | `client.yaml` |
| `docs` | `DOCS_SITE_ADDRESS`, `DOCS_PASSWORD_HASH` | `Caddyfile`; `docs` volume, read-only; `docs-state` volume |

### Read with defaults

The packages read these settings too. The local and CI targets rely on the
defaults; a shared Snowflake deployment sets them in its own Compose override.

| Setting | Read by | Default | Meaning |
| --- | --- | --- | --- |
| `ANALYTICS_CLIENT_CONFIG` | `ingestion`, `transformations`, `cube-api` | `/etc/analytics/client.yaml` | Path of `client.yaml` |
| `ANALYTICS_SOURCE_SECRETS_ROOT` | `ingestion` | `/run/secrets/sources` | Folder of source settings files |
| `ANALYTICS_INGESTION_STATE_ROOT` | `ingestion` | `/var/lib/analytics/ingestion` | Durable state of every source |
| `SHOPIFY_ORDERS_DATASET`, `SHOPIFY_CUSTOMERS_DATASET`, `LOOP_RETURNS_DATASET` | `ingestion` | `*_RAW` names | Raw Snowflake schema of each source |
| `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_ROLE`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_PRIVATE_KEY_PATH` | `ingestion`, `transformations` | None | Snowflake identity; the key is a mounted file |
| `SNOWFLAKE_SCHEMA`, `DBT_STATEMENT_TIMEOUT_SECONDS` | `transformations` | See the [transformations guide](../../packages/transformations/README.md) | dbt's Snowflake target |
| `DUCKDB_PATH` | `transformations` | `/var/lib/analytics/working/warehouse.duckdb` | dbt's local warehouse file |

The toolkit sets the remaining values itself, such as
`ANALYTICS_REPORTING_TIME_ZONE` and `ANALYTICS_ENABLED_SOURCES`, which it
derives from `client.yaml`. The [ingestion](../../packages/ingestion/README.md)
and [transformations](../../packages/transformations/README.md) guides describe
each package's settings in full.

## Live local loads (ADR 0029)

The local stack can load real source data into each source's raw DuckDB file
in the ingestion-state volume, and local dbt reads those files
([ADR 0029](../../docs/adrs/0029-load-every-source-into-local-duckdb.md)).
Nothing is scheduled. Launch each load by hand in the local Dagster UI.

Every source reads its credentials and settings from one folder of JSON
files, one file per system: `shopify.json` (used by both Shopify orders and
Shopify customers), `loop_returns.json`, `goodday.json`, `google_sheets.json`
and `impact_radius.json`. The
[ingestion guide](../../packages/ingestion/README.md) lists each file's keys.

Keep the folder outside the checkout, readable only by you. Put each file in
place yourself: write it by hand, or render it from your own password manager
outside the checkout. The repository holds no secret templates or secret-manager
commands ([ADR 0032](../../docs/adrs/0032-read-secrets-from-environment-files.md)).
Create the files with `umask 077`:

```sh
mkdir -p ~/.config/analytics-platform/local/sources && chmod 700 ~/.config/analytics-platform/local/sources
(umask 077; ${EDITOR:-vi} ~/.config/analytics-platform/local/sources/shopify.json)
```

Copy `.env.local.example` to the ignored `deploy/compose/.env.local`, add the
folder and the kit sources to enable, then start the stack; the `local`
target reads that file automatically:

```sh
SOURCE_SETTINGS_HOST_DIR=/Users/<you>/.config/analytics-platform/local/sources
ANALYTICS_SOURCES=impact_radius,google_sheets
```

```sh
./scripts/platform local up
```

The ingestion service and its run containers mount the settings folder at
`/run/secrets/sources`, read-only. An unset `SOURCE_SETTINGS_HOST_DIR` mounts
the empty `no-source-settings` folder, and each source's job fails before it
reads anything. A kit source registers its job only when it is in
`ANALYTICS_SOURCES`; a listed kit source without its settings file stops the
ingestion code location from loading. The `ci` target removes this mount, so CI
never receives live credentials. The settings folder and its files
must be readable by the run containers' user (UID 10001) through Docker's file
sharing.

| Source | Settings file | Job | Raw file under the state volume |
| --- | --- | --- | --- |
| Shopify orders | `shopify.json` | `shopify_orders_dlt_load` | `shopify_orders/local/shopify_orders.duckdb` |
| Shopify customers | `shopify.json` | `shopify_customers_dlt_load` | `shopify_customers/local/shopify_customers.duckdb` |
| Loop Returns headers | `loop_returns.json` | `loop_returns_dlt_snapshot` | `loop_returns/local/loop_returns.duckdb` |
| GoodDay inventory | `goodday.json` | `goodday_inventory_capture` | `goodday/local/goodday.duckdb` |
| Google Sheets | `google_sheets.json` | `google_sheets_capture` | `google_sheets/local/google_sheets.duckdb` |
| Impact Radius actions | `impact_radius.json` | `impact_radius_actions_load` | `impact_radius/local/impact_radius.duckdb` |

Adding another kit source needs no change to these Compose files: list it in
`ANALYTICS_SOURCES` and add its settings file.

Shopify orders used to write `shopify_orders/local/raw_shopify_orders.duckdb`.
Local dbt no longer reads that file. To keep its data, move it once while the
stack is stopped, or load a recent window again. Shopify customers now write
`shopify_customers/local/shopify_customers.duckdb` instead of sharing the orders
file, and Loop Returns writes `loop_returns/local/loop_returns.duckdb`.
Orders keep their saved progress in the moved file; a reloaded window starts
from the window you give it.

```sh
./scripts/platform local compose \
  run --rm --no-deps --entrypoint mv ingestion \
  /var/lib/analytics/ingestion/shopify_orders/local/raw_shopify_orders.duckdb \
  /var/lib/analytics/ingestion/shopify_orders/local/shopify_orders.duckdb
```

Shopify orders read only the window you give the run, selected by each order's
last update. One run stops after an hour, so size the window to your store's
order volume. A busy store may need windows of about 12 hours, for example:

```yaml
ops:
  raw__shopify__shopify_orders_raw:
    config: {kind: lookback, start: "2026-11-01T12:00:00+00:00", end: "2026-11-02T00:00:00+00:00"}
```

Each run container has 2 GB of memory. Local ingestion sets `LOAD__WORKERS=1`,
so dlt loads one DuckDB insert job at a time; its parallel default runs out of
memory on a 12-hour window. A failed run leaves its load package, and the next
run loads it before reading its own window.

GoodDay's `goodday.json` holds its BigQuery settings and a read-only
service-account credential, as listed in the
[GoodDay configuration](../../packages/ingestion/README.md#manual-execution-and-configuration).
Each run is one billed BigQuery query with no per-query cost cap. List
`goodday` in the client's `client.yaml` after the first load so dbt builds the
inventory models.

`google_sheets.json` holds a service-account credential and the tabs to
capture, as listed in the
[Google Sheets configuration](../../packages/ingestion/README.md#google-sheets-captures).
The job asks Google only for the read-only Sheets scope. Each run appends one
capture per tab. No dbt models read these tables yet.

Impact Radius reads the last three days of actions for the program in
`impact_radius.json` and has no dbt models yet.

After the loads, build every enabled model from the checkout with
`./scripts/platform local dbt build`.
Local `dbt` commands write the `local_*` schemas unless `DBT_NAMESPACE` is set;
the `ci` target and Snowflake targets require an explicit namespace.

To look at the data, run
`./scripts/platform local browse`.
It needs the DuckDB CLI on the host (`brew install duckdb`). It copies the
warehouse and each raw file into a private temporary folder, opens them
read-only in the DuckDB UI at <http://localhost:4213>, and deletes the copies
when you type `.exit`. Raw files are attached under the aliases dbt uses, such
as `raw_shopify_orders`, so staging views resolve. Raw files
hold private customer data. Keep them in the volume, and remove them when you
no longer need them: `./scripts/platform local down --volumes` deletes every
local volume, including these files and Dagster's run history.

## dbt documentation site

The `docs` Compose service runs Caddy, a web server, from `Caddyfile`. It starts
only where `COMPOSE_PROFILES` includes `docs`, as in the local and CI example
file. It serves the dbt documentation and column lineage at
`http://localhost:8080` behind a shared login, with the fixture password in
`.env.local.example`. `local smoke` checks that login with the fixture password,
so keep the example's `DOCS_PASSWORD_HASH` in a local env file you smoke-test.
Caddy serves the `docs` volume read-only and proxies
nothing, so Dagster and Cube stay on loopback. Its content security policy
allows only the pages' inline code and blocks every network connection, which
also blocks the usage tracking bundled in both pages.
The [transformations `dbt_docs` job](../../packages/transformations/README.md#dbt-documentation-site)
writes both pages. `DOCS_PASSWORD_HASH` is a base64-encoded bcrypt hash; an
empty hash stops Caddy from loading, so the site never runs without a password.
Generate one with:

```sh
docker run --rm caddy:2.11.7-alpine caddy hash-password --plaintext '<password>' | tr -d '\n' | base64
```

`ci smoke` builds the docs and checks the login.

## Shared environments

This repository ships only the `local` and `ci` targets. To run the platform on
a shared host against Snowflake, write your own Compose override for that host.
Give it separate identities, databases, schemas, metadata passwords and API
secrets, and mount credential files read-only for UID 10001. Follow
[ADR 0028](../../docs/adrs/0028-develop-locally-before-shared-snowflake-release.md):
validate changes locally first, then run the opt-in Snowflake acceptance tests
in each package before a shared release.
