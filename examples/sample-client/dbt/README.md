# Sample client dbt project

This is the dbt root project of a sample client. It installs the toolkit's dbt
project, `analytics_toolkit`, as a dbt package from the path the toolkit's
transformations image sets in `ANALYTICS_TOOLKIT_DBT` (`packages.yml`). The
toolkit supplies the raw sources, staging models, macros and dbt targets
(`profiles.yml` through `DBT_PROFILES_DIR`); this project adds the client's own
models on top.

| Path | Contents |
| --- | --- |
| `../client.yaml` | The client's settings: enabled sources, reporting time zone, warehouse |
| `macros/generate_schema_name.sql` | Delegates schema naming to the toolkit, so client models build in `<DBT_NAMESPACE>_<layer>` |
| `models/marts/fct_orders__daily.sql` | Non-test Shopify orders per reporting day, from `stg_shopify_orders__orders` |
| `models/marts/_marts__models.yml` | Its documentation, column declarations, tests and a dbt unit test |

Client models follow the toolkit's rules: ADR 0022 layers and names, model
and column declarations, and a PII decision for every column. Run dbt through
`analytics-dbt`, which passes the `client.yaml` settings to dbt.

In the client's transformations image, with this project at `DBT_PROJECT_DIR`
and `client.yaml` at `ANALYTICS_CLIENT_CONFIG`, check it offline with:

```sh
analytics-check-client
```

The toolkit's transformations tests run the same check on this project. See
the [transformations README](../../../packages/transformations/README.md#client-projects).
