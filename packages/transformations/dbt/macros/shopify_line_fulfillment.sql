{% macro shopify_line_fulfillment_columns(relation) %}
    {# Revision 4 adds nullable raw columns. A code release must also run before
       the first revision-4 load creates them. Never turn absent evidence to zero. #}
    {% set names = [] %}
    {% if execute %}
        {% for column in adapter.get_columns_in_relation(relation) %}
            {% do names.append(column.name | lower) %}
        {% endfor %}
    {% endif %}
    {% for name, kind in [('unfulfilled_quantity', 'bigint'),
                         ('non_fulfillable_quantity', 'bigint'),
                         ('requires_shipping', 'boolean')] %}
        cast({{ name if name in names else 'null' }} as {{ kind }}) as {{ "is_shipping_required" if name == "requires_shipping" else name }}{% if not loop.last %},{% endif %}
    {% endfor %}
{% endmacro %}
