{#- Valid GTFS boolean flag: 1 or 0. Compared as varchar so integer sources work too. -#}
{% macro is_valid_gtfs_bool(column) -%}
    cast({{ column }} as varchar) in ('1', '0')
{%- endmacro %}

{#- Convert a GTFS boolean flag to boolean; raise an error when it is malformed. -#}
{% macro gtfs_bool(column) -%}
case
    when {{ column }} is null then null
    when not {{ is_valid_gtfs_bool(column) }} then error('malformed GTFS flag: ' || {{ column }})
    else cast({{ column }} as varchar) = '1'
end
{%- endmacro %}
