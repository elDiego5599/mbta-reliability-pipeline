{% macro gtfs_time_to_seconds(column) -%}
case
    when {{ column }} is null then null
    else
        cast(split_part({{ column }}, ':', 1) as integer) * 3600
        + cast(split_part({{ column }}, ':', 2) as integer) * 60
        + cast(split_part({{ column }}, ':', 3) as integer)
end
{%- endmacro %}
