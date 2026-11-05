{#-
  Client settings from client.yaml, which the analytics-dbt wrapper passes to
  dbt as generated variables. Neither has a default: plain dbt without the
  wrapper fails instead of guessing a client's sources.
-#}

{#-
  Whether client.yaml enables a source, by its client.yaml name. For model SQL,
  such as a client model that reads an optional source:
  {{ config(enabled=analytics_toolkit.source_enabled('goodday')) }}
  YAML files cannot call macros, so dbt_project.yml and the sources repeat the
  same expression inline.
-#}
{% macro source_enabled(source) %}
  {%- set known = ['goodday', 'google_sheets', 'impact_radius', 'loop_returns', 'shopify_customers', 'shopify_orders'] -%}
  {%- if source not in known -%}
    {{ exceptions.raise_compiler_error("Unknown client.yaml source " ~ source ~ "; expected one of " ~ known | join(', ')) }}
  {%- endif -%}
  {{- return(source in env_var('ANALYTICS_ENABLED_SOURCES').split(',')) -}}
{% endmacro %}
