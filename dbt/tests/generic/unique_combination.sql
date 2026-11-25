{#- Fails with one row per combination of `columns` that occurs more than once.
    GROUP BY treats nulls as equal, so a null key part still counts. -#}
{% test unique_combination(model, columns) %}
select {{ columns | join(', ') }}, count(*) as n_rows
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}
