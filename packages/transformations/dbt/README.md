# analytics_toolkit dbt package

This dbt project is the toolkit's dbt package. A client's dbt project, the
dbt root project, installs it at the toolkit version the client pins
([ADR 0030](../../../docs/adrs/0030-ship-a-toolkit-with-one-deployment-per-client.md)).
The toolkit's own checks run it as a root project.

It provides:

- the raw sources and one staging model per source entity, gated by the
  client's `client.yaml` sources;
- macros: schema naming (`generate_schema_name`), UTC and reporting-date
  conversion, `source_enabled()`, raw-file attachment and `sparse_source()`;
- the `on-run-start` hook that attaches local raw files, which dbt also runs
  for an installed package;
- generic tests `unique_combination` and `composite_relationship`.

## Using it from a client project

```yaml
# packages.yml
packages:
  - local: "{{ env_var('ANALYTICS_TOOLKIT_DBT') }}"
```

```sql
-- macros/generate_schema_name.sql: dbt applies a package's schema macro to
-- the package's own nodes only, so the client's models delegate to it.
{% macro generate_schema_name(custom_schema_name, node) %}{{ return(analytics_toolkit.generate_schema_name(custom_schema_name, node)) }}{% endmacro %}
```

Use the `analytics` profile, which the toolkit owns and the transformations
image provides through `DBT_PROFILES_DIR`. Run dbt through `analytics-dbt`,
never plain `dbt`: the project reads `ANALYTICS_ENABLED_SOURCES` and
`ANALYTICS_REPORTING_TIME_ZONE`, which only that wrapper sets, from
`client.yaml`. A client project can disable or reconfigure toolkit models
under `models: analytics_toolkit:` in its own `dbt_project.yml`.

`examples/sample-client/` in the toolkit repository is a working client
project. See the [transformations README](../README.md) for the settings,
the model checks and `analytics-check-client`.
