{#- The toolkit names every schema <DBT_NAMESPACE>_<layer>. A package's macro applies only to its own nodes, so the client's models delegate to it. -#}
{% macro generate_schema_name(custom_schema_name, node) %}{{ return(analytics_toolkit.generate_schema_name(custom_schema_name, node)) }}{% endmacro %}
