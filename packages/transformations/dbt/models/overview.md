{% docs __overview__ %}
# Analytics Platform dbt project

This site documents the raw sources and the staging models built from them.
Each model page lists its columns, tests and SQL. Marts, the tables that Cube
reads, are added as each business question needs them.

## Lineage

- **Table lineage:** open any model and click the round button in the
  bottom-right corner. The graph shows the models and sources it depends on
  and the models that use it.
- **Column lineage:** add `lineage/` to this site's address, for example
  `https://<site>/lineage/`. That page traces each column from the raw
  tables through the models.
{% enddocs %}
