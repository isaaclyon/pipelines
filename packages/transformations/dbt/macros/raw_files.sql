{#-
  ADR 0029: on the local DuckDB target, attach each source's raw file read-only.
  A source names its file in meta.raw_file, relative to DBT_RAW_ROOT, and its
  database alias in `database`. A missing file is never created; the run fails
  only when a selected node reads that source.
-#}
{% macro attach_raw_files() %}
  {%- set root = env_var('DBT_RAW_ROOT', '') -%}
  {%- if not execute or target.type != 'duckdb' or not root -%}{{ return('') }}{%- endif -%}
  {%- set files = {} -%}
  {%- for source in graph.sources.values() if source.database != target.database -%}
    {%- if not source.meta.get('raw_file') -%}
      {{ exceptions.raise_compiler_error("Source " ~ source.source_name ~ " reads database " ~ source.database ~ " but names no meta.raw_file") }}
    {%- endif -%}
    {%- do files.update({source.database: root ~ '/' ~ source.meta.raw_file}) -%}
  {%- endfor -%}
  {%- set missing = {} -%}
  {%- for alias, path in files.items() -%}
    {%- set quoted = "'" ~ path | replace("'", "''") ~ "'" -%}
    {%- if run_query("select count(*) from glob(" ~ quoted ~ ")").columns[0].values()[0] > 0 -%}
      {%- do run_query("attach if not exists " ~ quoted ~ " as " ~ adapter.quote(alias) ~ " (read_only)") -%}
    {%- else -%}
      {%- do missing.update({alias: path}) -%}
    {%- endif -%}
  {%- endfor -%}
  {%- if missing -%}
    {#- Walk upstream from the selection; appended ids extend the loop. -#}
    {%- set queue = selected_resources | list -%}
    {%- set seen = {} -%}
    {%- for unique_id in queue if unique_id not in seen -%}
      {%- do seen.update({unique_id: true}) -%}
      {%- set source = graph.sources.get(unique_id) -%}
      {%- if source and source.database in missing -%}
        {{ exceptions.raise_compiler_error("Raw file for source " ~ source.source_name ~ " is missing: " ~ missing[source.database] ~ ". Load it on the local stack first, or deselect the models that read it.") }}
      {%- endif -%}
      {%- set node = graph.nodes.get(unique_id) -%}
      {%- if node -%}{%- do queue.extend(node.depends_on.nodes) -%}{%- endif -%}
    {%- endfor -%}
  {%- endif -%}
{% endmacro %}

{#-
  ADR 0029: dlt creates a nested table only once a loaded row needs it, so a
  short live window can lack one (for example refund duties). On the local
  raw-file target, read a missing table as no rows with its declared columns.
  Every other target reads the source as before and fails if it is absent.
-#}
{% macro sparse_source(source_name, table_name) %}
  {%- set relation = source(source_name, table_name) -%}
  {%- if execute and target.type == 'duckdb' and env_var('DBT_RAW_ROOT', '')
      and adapter.get_relation(database=relation.database, schema=relation.schema, identifier=relation.identifier) is none -%}
    {%- set declared = graph.sources.values() | selectattr('source_name', 'equalto', source_name) | selectattr('name', 'equalto', table_name) | first -%}
    {%- if not declared.columns -%}
      {{ exceptions.raise_compiler_error("Source " ~ source_name ~ "." ~ table_name ~ " is missing and declares no columns") }}
    {%- endif -%}
    (select
      {%- for column in declared.columns.values() %}
      cast(null as {{ column.data_type }}) as {{ adapter.quote(column.name) }}{{ ',' if not loop.last }}
      {%- endfor %}
    where false)
  {%- else -%}
    {{ relation }}
  {%- endif -%}
{% endmacro %}
