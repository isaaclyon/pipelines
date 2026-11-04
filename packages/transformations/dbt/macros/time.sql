{#- Instants are stored as UTC timestamps without a zone; dates are local. -#}

{% macro utc_timestamp(column) %}
    {{ return(adapter.dispatch('utc_timestamp')(column)) }}
{% endmacro %}

{% macro duckdb__utc_timestamp(column) %}
    timezone('UTC', {{ column }})
{% endmacro %}

{% macro snowflake__utc_timestamp(column) %}
    cast(convert_timezone('UTC', {{ column }}) as timestamp_ntz)
{% endmacro %}

{% macro default__utc_timestamp(column) %}
    {{ exceptions.raise_compiler_error('utc_timestamp has no implementation for ' ~ adapter.type()) }}
{% endmacro %}

{% macro local_date(utc_column, time_zone) %}
    {{ return(adapter.dispatch('local_date')(utc_column, time_zone)) }}
{% endmacro %}

{% macro duckdb__local_date(utc_column, time_zone) %}
    cast(timezone('{{ time_zone }}', timezone('UTC', {{ utc_column }})) as date)
{% endmacro %}

{% macro snowflake__local_date(utc_column, time_zone) %}
    cast(convert_timezone('UTC', '{{ time_zone }}', {{ utc_column }}) as date)
{% endmacro %}

{% macro default__local_date(utc_column, time_zone) %}
    {{ exceptions.raise_compiler_error('local_date has no implementation for ' ~ adapter.type()) }}
{% endmacro %}

{#- Reporting dates use one zone, shared with Cube through the same variable. -#}
{% macro reporting_time_zone() %}
    {{- return(env_var('ANALYTICS_REPORTING_TIME_ZONE', 'America/Los_Angeles')) -}}
{% endmacro %}
