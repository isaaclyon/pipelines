{% test composite_relationship(model, columns, to, to_columns) %}
{#- Composite foreign key: every row whose listed columns are all present must
    match one parent row on the same columns. A row with any listed column null
    has no reference to check. -#}
select child.*
from (
    select {{ columns | join(', ') }}
    from {{ model }}
    where
        {%- for column in columns %}
        {{ column }} is not null
        {%- if not loop.last %} and{% endif %}
        {%- endfor %}
) as child
left join {{ to }} as parent
    on
        {%- for column in columns %}
        child.{{ column }} = parent.{{ to_columns[loop.index0] }}
        {%- if not loop.last %} and{% endif %}
        {%- endfor %}
where parent.{{ to_columns[0] }} is null
{% endtest %}
