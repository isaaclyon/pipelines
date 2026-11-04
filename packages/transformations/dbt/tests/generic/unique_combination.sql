{% test unique_combination(model, columns) %}
{#- Composite key uniqueness, matched to the declared key by its columns. -#}
select {{ columns | join(', ') }}
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}
