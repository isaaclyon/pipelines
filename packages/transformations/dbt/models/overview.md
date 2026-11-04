{% docs __overview__ %}
# Analytics Platform dbt project

This site documents the Shopify sources, the staging and intermediate models
built from them, and the marts that Cube reads. Each model page lists its
columns, tests and SQL.

## Lineage

- **Table lineage:** open any model and click the round button in the
  bottom-right corner. The graph shows the models and sources it depends on
  and the models that use it.
- **Column lineage:** add `lineage/` to this site's address, for example
  `https://<site>/lineage/`. That page traces each column from the raw
  Shopify tables to the marts.

Staging and intermediate relations exist only while a release is published,
so their column types come from their declarations rather than the warehouse.
{% enddocs %}
