# Analytics platform

A container-first analytics platform using dlt for ingestion, Dagster for
orchestration, dbt Core for transformations, Snowflake as the warehouse, and
Cube Core for metrics. Single-user development loads and transforms data in
local DuckDB files; Snowflake remains the shared release target.

The repository includes source connectors (Shopify orders and customers, Loop
Returns, Impact Radius, GoodDay inventory and Google Sheets), dbt staging
models, a placeholder Cube model, a local Docker Compose stack and independent
package checks. Marts and their Cube metrics arrive in Roadmap Phase 4. It ships
only the local and CI targets; running it on a shared host needs your own
deployment.

The platform is becoming a reusable toolkit for small D2C brands, with one VM
and one MotherDuck or Snowflake warehouse per client. The [roadmap](ROADMAP.md)
describes the phases; some content below still describes the earlier
single-brand setup until Phase 1 lands.

- [Roadmap](ROADMAP.md)
- [Architecture decisions](docs/adrs/README.md)
- [Engineering standards](engineering-standards.md)
- [Delivery policy](docs/delivery-policy.md) — outcomes, acceptance, focus, and tracking
- [Shared terminology](GLOSSARY.md)
- [Shopify order raw field contract](docs/specs/20261009-shopify-raw-ingestion.md)

Accepted ADRs and engineering standards define the current direction.

## Repository structure

```text
packages/
  ingestion/         # dlt + ingestion Dagster code location
  transformations/   # dbt project + transformation code location
  metrics/           # Cube definitions
deploy/
  dagster/           # Control-plane configuration
  compose/           # Local and CI Compose configuration
docs/
  adrs/              # Accepted architecture decisions
  specs/             # Implementation specifications
.agents/             # Canonical shared agent configuration
```

Python packages use `src/` and `tests/`, with independent dependency declarations,
uv lockfiles, and build environments. Shared packages are added when a concrete
shared contract exists.

## Quickstart

Install Python 3.10 or newer and Docker with Compose 2.24.4 or newer, then run
from the checkout:

```sh
./scripts/platform local check
```

Enable the secret-scanning commit hook once per clone (needs
[gitleaks](https://github.com/gitleaks/gitleaks)):

```sh
git config core.hooksPath .githooks
```

For optional service-scaffold checks, run `local up` and `local smoke`.
These do not serve fixture data. Dagster is available at
[localhost:3000](http://localhost:3000) and the Cube API at
[localhost:4000](http://localhost:4000). Initial builds download dependencies;
the ordinary package checks then run with networking disabled. Host Python renders
run-container settings using only its standard library. Host Node is optional.

The smoke check first runs read-only checks on every target: each Dagster code
location loads, and Cube returns its model metadata to a token signed with
`CUBEJS_API_SECRET`. Local and CI targets then launch `scaffold_check`
in both code locations. Cube uses Snowflake configuration for metadata compilation
but has no warehouse credentials in the offline scaffold. It does not serve
fixture data. Snowflake behavior requires explicit development Snowflake
acceptance; local smoke checks do not prove an end-to-end business pipeline.

```sh
./scripts/platform local check ingestion  # Or transformations / metrics
./scripts/platform local logs --tail 100
./scripts/platform local down             # Retains named volumes
./scripts/platform help
```

Run `local up` again after source or dependency changes to rebuild the installed
packages.

See [deployment and state](deploy/compose/README.md) for environment
configuration.

## Configure for your business

Business settings come from environment variables in your Compose env file
(`ANALYTICS_ENV_FILE`; `local` defaults to `deploy/compose/.env.local` when it
exists, otherwise `deploy/compose/.env.local.example`).
`ANALYTICS_REPORTING_TIME_ZONE` sets the calendar zone for order dates in dbt
and Cube (default `America/Los_Angeles`). Source connectors read their own
settings files; see the [ingestion README](packages/ingestion/README.md).

## Worktrunk worktrees

Install [Worktrunk](https://worktrunk.dev/), `uv`, and Node with `npm` for
automatic host development setup. Container checks remain the validation path
described above. Enable directory switching with `wt config shell install`.

```sh
wt switch --create my-change
```

The shared [Worktrunk config](.config/wt.toml) runs
[`scripts/worktree-setup.sh`](scripts/worktree-setup.sh) synchronously when a
worktree is created. Worktrunk asks you to approve the hook on first use.
Your personal Worktrunk `worktree-path` setting controls its location;
`.worktrees/` is ignored for nested worktrees.

Setup copies existing `.env` files from the primary checkout at the repository
root and in `packages/ingestion`, `packages/transformations`, and
`packages/metrics`, plus `deploy/compose/.env.local`. Keep these files local-only.
Missing files are skipped, existing destination files are preserved, and copied
files receive owner-only permissions. Symlinked source env files are rejected.

It runs `uv sync --frozen` independently in both Python packages, the isolated
`packages/transformations/lineage` CLI environment, and `deploy/dagster`, including
declared development dependencies, then `npm ci` in
`packages/metrics`. Each Python project gets its own `.venv`; lockfiles stay fixed.
To retry setup or prepare an existing checkout, run from its root:

```sh
sh scripts/worktree-setup.sh /absolute/path/to/primary-checkout
```

Each checkout's local stack is named after its folder (`analytics-<folder>`),
so worktrees keep separate volumes. To run two stacks at once, select distinct
ports:

```sh
DAGSTER_PORT=3100 CUBE_PORT=4100 DOCS_PORT=8180 ./scripts/platform local up
```

The `local` target reads `deploy/compose/.env.local` when it exists and the
fixture example otherwise. Use the same ports for subsequent commands. See
[deployment configuration](deploy/compose/README.md) for details.

## Agent MCP configuration

`.agents/mcp.json` is the canonical project MCP configuration. `.pi/mcp.json`
and Claude Code's root `.mcp.json` symlink to it, matching the shared skills setup.

The Dagster MCP server runs through `uv` in an isolated tool environment, using
a pinned upstream commit and MCP SDK version. Install `uv` and restart the agent
to load the configuration; Pi requires project trust and Claude Code may prompt
for project MCP approval.

The server starts and exposes its tools before a Dagster project exists. Tool
calls require a running Dagster instance at `http://localhost:3000/graphql`.
Once the project is ready, start its local Dagster webserver on port 3000.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
