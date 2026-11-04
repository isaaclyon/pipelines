{% macro generate_schema_name(custom_schema_name, node) %}
    {% set namespace = env_var('DBT_NAMESPACE') %}
    {% if not modules.re.fullmatch('[a-z][a-z0-9]*(?:_[a-z0-9]+)*', namespace) or namespace | length > 90 %}
        {{ exceptions.raise_compiler_error('Explicit lowercase snake_case namespace required (maximum 90 characters)') }}
    {% endif %}
    {% set auxiliary = {'seed': 'seeds', 'snapshot': 'snapshots', 'test': 'test_failures'} %}
    {% if node.resource_type in auxiliary %}
        {% set suffix = auxiliary[node.resource_type] %}
        {% set allowed = [suffix, 'dbt_test__audit'] if node.resource_type == 'test' else [suffix] %}
        {% if custom_schema_name is not none and custom_schema_name | trim not in allowed %}
            {{ exceptions.raise_compiler_error('Auxiliary resources require their dedicated namespace schema') }}
        {% endif %}
    {% elif node.resource_type == 'model' %}
        {% set suffix = custom_schema_name | trim if custom_schema_name is not none else '' %}
        {% if suffix not in ['staging', 'intermediate', 'marts'] %}
            {{ exceptions.raise_compiler_error('Model requires an explicit staging, intermediate or marts schema') }}
        {% endif %}
    {% else %}
        {{ return(target.schema) }}
    {% endif %}
    {{ namespace }}_{{ suffix }}
{% endmacro %}
