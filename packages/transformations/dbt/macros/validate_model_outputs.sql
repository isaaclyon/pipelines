{% macro model_output_types(sql, declarations) %}
    {% set columns = [] %}
    {% if target.type == 'snowflake' %}
        {# dbt-snowflake's query schema API reports FIXED without precision/scale.
           Inspect the zero-row result on this same connection instead; DESCRIBE
           preserves NUMBER(p,s), timestamp precision and explicit string lengths. #}
        {% do run_query(get_empty_subquery_sql(sql)) %}
        {% set metadata = run_query('describe result last_query_id()') %}
        {% for row in metadata.rows %}
            {% set name = row[0] | lower %}
            {% set data_type = row[1] | lower %}
            {# Unbounded VARCHAR declarations do not constrain string length.
               Snowflake resolves literal/view and NULL casts to different
               default lengths. Explicit VARCHAR(n) still compares exactly. #}
            {% set declaration = declarations.get(name, {}) %}
            {% if data_type.startswith('varchar(') and (declaration.get('data_type') or '') | lower | trim in ['varchar', 'text', 'string'] %}
                {% set data_type = 'varchar' %}
            {% endif %}
            {% do columns.append((name, data_type)) %}
        {% endfor %}
    {% else %}
        {% for column in get_column_schema_from_query(sql) %}
            {% do columns.append((column.name | lower, column.data_type | lower)) %}
        {% endfor %}
    {% endif %}
    {{ return(columns) }}
{% endmacro %}

{% macro validate_model_outputs(outputs) %}
    {# Private candidates only: compare adapter-resolved types, including decimal
       precision/scale, without turning internal models into public contracts. #}
    {% if execute %}
        {% for output in outputs %}
            {% set observed = model_output_types(output.sql, output.columns) %}
            {% set names = observed | map(attribute=0) | list %}
            {% if names | sort != output.columns.keys() | list | sort %}
                {{ exceptions.raise_compiler_error(output.model_id ~ ': actual output columns differ from declarations: ' ~ names) }}
            {% endif %}
            {% set typed = {} %}
            {% for name, column in output.columns.items() if column.get('data_type') %}
                {% do typed.update({name: column}) %}
            {% endfor %}
            {% set declared = model_output_types(get_empty_schema_sql(typed), typed) if typed else [] %}
            {% set observed_typed = [] %}
            {% for column in observed if column[0] in typed %}
                {% do observed_typed.append(column) %}
            {% endfor %}
            {% if observed_typed | sort != declared | sort %}
                {{ exceptions.raise_compiler_error(output.model_id ~ ': actual output columns/types differ from declarations: ' ~ observed ~ ' versus ' ~ declared) }}
            {% endif %}
        {% endfor %}
    {% endif %}
{% endmacro %}
