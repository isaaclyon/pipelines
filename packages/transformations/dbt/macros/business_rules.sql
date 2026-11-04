{#- Brand business rules read from the project vars in dbt_project.yml. -#}

{#- True when the column matches any SQL LIKE pattern; false for no patterns.
    Patterns are lowercased because the compared values are normalized. -#}
{% macro matches_any(column, patterns) %}
    {%- if patterns | length == 0 -%}
        false
    {%- else -%}
        ({% for pattern in patterns -%}
            {{ column }} like '{{ pattern | lower | replace("'", "''") }}'
            {%- if not loop.last %} or {% endif -%}
        {%- endfor %})
    {%- endif -%}
{% endmacro %}

{#- Reporting dates use one zone, shared with Cube through the same variable. -#}
{% macro reporting_time_zone() %}
    {{- return(env_var('ANALYTICS_REPORTING_TIME_ZONE', 'America/Los_Angeles')) -}}
{% endmacro %}

{#- A separately reported shipping country is its own market; every other
    country, including a missing one, reports in the home market. -#}
{% macro reporting_market(country_code_column) %}
    {%- set separate = var('separate_reporting_markets') -%}
    {%- if separate | length == 0 -%}
        '{{ var('home_reporting_market') }}'
    {%- else -%}
        case
            when {{ country_code_column }} in (
                {%- for market in separate -%}
                    '{{ market }}'{% if not loop.last %}, {% endif %}
                {%- endfor -%}
            ) then {{ country_code_column }}
            else '{{ var('home_reporting_market') }}'
        end
    {%- endif -%}
{% endmacro %}

{#- matches_any for the patterns in one project var. -#}
{% macro matches_var(column, var_name) %}
    {{- matches_any(column, var(var_name)) -}}
{% endmacro %}
