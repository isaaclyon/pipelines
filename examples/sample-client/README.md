# Sample client repository

This folder is the template for a client repository. Copy it into its own Git
repository for each brand. A client repository holds only the client's
settings, dbt models and Cube files. It builds on the toolkit's published
images, at the one version it pins, and needs no toolkit checkout
([ADR 0036](../../docs/adrs/0036-versioned-toolkit-releases.md)).

The toolkit's own checks build and check this folder against the current
toolkit source, and its local stack runs it. Links into `../../` point to the
toolkit repository, so they break in a copy.

## Files

| Path | Contents |
| --- | --- |
| `client.yaml` | The client's settings: enabled sources, schedules, reporting time zone, warehouse and alert channels. Not secret; checked in |
| `toolkit.env` | `TOOLKIT_VERSION`, the one toolkit version the client runs |
| `Dockerfile.transformations` | The client's transformations image: the toolkit image plus `dbt/`, with its dbt packages installed at build time |
| `Dockerfile.metrics` | The client's Cube image: the toolkit image plus `cube/model/` |
| `dbt/` | The client's dbt root project, which installs the toolkit's dbt package; see [`dbt/README.md`](dbt/README.md) |
| `cube/model/sample_client/` | The client's Cube definitions, loaded beside the toolkit's |
| `compose.yaml` | Builds both client images and defines the offline check services |
| `check` | Builds the images and runs every offline check |
| `.env.example` | Every secret and host-specific setting a deployment reads. Copy it to `.env`, which Git ignores |
| `.dockerignore` | Keeps `.env`, DuckDB files and dbt output out of the images |

`client.yaml` holds what is true of the client in every environment. Secrets
and host paths go in `.env` instead. See the
[client configuration file](../../packages/client-config/README.md) for every
key, and the [settings each service reads](../../deploy/compose/README.md#settings-each-service-reads).

## Check the client offline

```sh
./check
```

`./check` needs Docker and the two toolkit images that `toolkit.env` names,
`analytics-transformations` and `analytics-metrics`. It needs no network,
credentials or warehouse. It builds both client images, then runs each check
in a container with no network:

- **Transformations:** `analytics-check-client` installs the dbt packages,
  parses the project, applies the toolkit's model checks and SQL lint rules,
  and runs `dbt build` on empty typed tables, with every data test and unit
  test. See
  [`analytics-check-client`](../../packages/transformations/README.md#analytics-check-client).
- **Cube:** reads the reporting time zone from `client.yaml` and compiles the
  toolkit and client models together, as Cube startup does. A repeated file
  path or cube name fails. See the
  [client model folder](../../packages/metrics/README.md#client-model-folder).

`./check` exits nonzero when a build or check fails, and names it. To check
another toolkit version without editing `toolkit.env`, run
`TOOLKIT_VERSION=<version> ./check`.

## Pin and upgrade the toolkit

`toolkit.env` holds the only pin. Each toolkit release lists its breaking
changes and upgrade steps in the toolkit's `CHANGELOG.md`. To upgrade, follow
ADR 0036:

1. Load the new toolkit images and set `TOOLKIT_VERSION` in `toolkit.env`.
2. Fix anything the release notes list, then run `./check`. Then check against
   the client's development target.
3. Stop scheduled runs, deploy the new version, and run any data change the
   release needs.
4. Resume schedules and confirm that the next run succeeds.

Upgrade one client at a time. To roll back, redeploy the previous version.
When a release notes that it changes stored data, fix forward instead.

## Add dbt models

Add models under `dbt/models/` in the client's own project, `sample_client`.
The toolkit's sources, staging models and macros come from the
`analytics_toolkit` package:

- Select toolkit models with `ref('stg_shopify_orders__orders')`. Call toolkit
  macros with the package prefix, such as `analytics_toolkit.source_enabled()`.
- Keep `macros/generate_schema_name.sql`. It applies the toolkit's schema rule,
  `<DBT_NAMESPACE>_<layer>`, to client models.
- Follow the toolkit's rules for client models: layer names (ADR 0022), model
  and column declarations, and a PII decision for every column. The
  transformations check enforces them.
- Change a toolkit model's settings from `dbt/dbt_project.yml` under
  `models: analytics_toolkit:`. The root project's settings win over the
  package's own.

Run dbt through `analytics-dbt`, not `dbt`. It passes the sources and reporting
time zone from `client.yaml` to dbt.

## Add Cube files

Add Cube model files under `cube/model/sample_client/`. Name the folder after
the client, so no file path repeats one of the toolkit's. Cube names must also
differ from the toolkit's. Cube files cannot read the environment, so a
`sql_table` names the production schema, such as `production_marts`.

## Run the client on the toolkit's local stack

From a toolkit checkout, point the local stack at a client repository and its
`.env`:

```sh
CLIENT_DIR=/path/to/client ANALYTICS_ENV_FILE=/path/to/client/.env \
    ./scripts/platform local up
```

The stack builds the client's images from toolkit images built from that
checkout, not from the pinned version. Deployment on a client VM is
[Roadmap](../../ROADMAP.md) Phase 4.
