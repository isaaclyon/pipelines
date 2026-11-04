---
title: Integration libraries index for this repository's tools (dbt, dlt, Docker, DuckDB, Postgres, Snowflake).
type: index
triggers:
  - "integration, external tool, dagster-\\*"
  - "dbt, dlt, docker, duckdb, postgres, snowflake"
---

<!-- Modified locally: adapted for open-source Dagster on 2026-10-07. -->
<!-- Modified locally: pruned to this repository's integrations on 2026-10-13. -->

# Integrations Reference

Dagster provides integration libraries for a range of tools and technologies. This reference directory keeps only the integrations this repository uses or is likely to use. For any other tool, check the [Dagster integrations documentation](https://docs.dagster.io/integrations/libraries) before building a custom component.

Integration libraries are typically named `dagster-<technology>`, where `<technology>` is the name of the tool or technology being integrated. Integrations marked as _(community)_ are maintained by the community rather than the Dagster team.

All integration reference files contain a link to the official documentation for the integration library, which can be referenced in cases where the local documentation does not provide sufficient information.

## Component Selection Hierarchy

Before writing ANY code, classify every external system in the request against the Reference Files Index below. Then for each system, use the FIRST option that applies:

1. **Use existing component as-is** — library exists, no customization needed. Scaffold with `dg scaffold defs`.
2. **Subclass existing component** — library exists, need custom behavior (mock execution, custom specs, etc.). Override `get_asset_spec()`, `execute()`, or `build_defs()` (`build_defs_from_state()` for `StateBackedComponent`). See [Subclassing Components](../components/subclassing-components.md).
3. **Custom `Component`** — no library exists, no external state needed. See [Creating Components](../components/creating-components.md).
4. **Custom `StateBackedComponent`** — no library exists, definitions require external state. See [Creating State-Backed Components](../components/state-backed/creating.md). Parent class may need to be inspected to determine the specific state format for `write_state_to_path()` / `build_defs_from_state()`.

**NEVER build a custom component when a library integration exists, even for mocks or testing usecases.**

## Component vs. Pythonic Integrations

**IMPORTANT**: If an integration library offers a Component, **ALWAYS** prefer this over the Pythonic integration unless there is an explicit reason to prefer the Pythonic integration. Components have a simpler, more deterministic interface and are easier to understand and manage.

### Discovery Workflow (library exists)

```bash
# Install the integration library
uv add dagster-<technology>

# List the available components
uv run dg list components --json

# [Optional] If a component exists, inspect its scaffolding or defs yaml schema
uv run dg utils inspect-component <ComponentType> --scaffold-params-schema
uv run dg utils inspect-component <ComponentType> --defs-yaml-schema

# Scaffold an instance of the component
uv run dg scaffold defs <ComponentType> <instance_name>
```

**NEVER skip this workflow.** Do not write Pythonic integration code (e.g. `FivetranWorkspace`, `@dbt_assets`, `CensusResource`) when a component is available. The `dg list components` command is the authoritative source for what components exist — do not guess or assume from memory.

### Custom Integration Workflow (no library exists)

If the desired external tool has **no published `dagster-*` library** (e.g. custom REST API), do NOT fall back to raw `@dg.asset` or `@dg.sensor` definitions. Instead, **create a custom component**:

```bash
# Scaffold a new component type
uv run dg scaffold component MyToolComponent

# Verify registration and get the full type path
uv run dg list components

# Scaffold an instance of your new component
uv run dg scaffold defs my_project.components.my_tool_component.MyToolComponent my_instance
```

See [Designing Component Integrations](../components/designing-component-integrations.md) for patterns on structuring the component (definition-only, observing via sensors, or orchestrating via execution). Components are the standard unit of integration in Dagster — even for tools without a published library.

## Reference Files Index

<!-- BEGIN GENERATED INDEX -->

- [dagster-dbt](./dagster-dbt/INDEX.md) — integrating dbt Core with Dagster
- [dagster-dlt](./dagster-dlt/INDEX.md) — dlt, data load tool, declarative pipelines
- [dagster-docker](./dagster-docker/INDEX.md) — containerized execution with Docker
- [dagster-duckdb](./dagster-duckdb/INDEX.md) — in-process analytical queries with DuckDB
- [dagster-postgres](./dagster-postgres/INDEX.md) — PostgreSQL as a Dagster storage backend
- [dagster-snowflake](./dagster-snowflake/INDEX.md) — interacting with Snowflake from Dagster
<!-- END GENERATED INDEX -->
