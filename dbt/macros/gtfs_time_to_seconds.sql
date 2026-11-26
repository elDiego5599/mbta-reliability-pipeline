{#- Valid GTFS time: H:MM:SS or HH:MM:SS. Hours may exceed 23 for service after midnight. -#}
{% macro is_valid_gtfs_time(column) -%}
    regexp_full_match({{ column }}, '^[0-9]+:[0-5][0-9]:[0-5][0-9]$')
{%- endmacro %}

{#- Convert a GTFS time to seconds; raise an error when it is malformed. -#}
{% macro gtfs_time_to_seconds(column) -%}
case
    when {{ column }} is null then null
    when not {{ is_valid_gtfs_time(column) }} then error('malformed GTFS time: ' || {{ column }})
    else
        cast(split_part({{ column }}, ':', 1) as integer) * 3600
        + cast(split_part({{ column }}, ':', 2) as integer) * 60
        + cast(split_part({{ column }}, ':', 3) as integer)
end
{%- endmacro %}
