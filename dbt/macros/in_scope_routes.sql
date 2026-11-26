{#- Routes of one static version in scope: allowed route types and not an
    excluded description. Use as a join to feed_version, route_id. -#}
{% macro in_scope_routes() -%}
(
    select
        feed_version,
        route_id,
        route_type
    from {{ ref('stg_static__routes') }}
    where
        route_type in (select unnest({{ var('intermediate_route_types') }}))
        and coalesce(route_desc, '') not in (select unnest({{ var('excluded_route_descs') }}))
)
{%- endmacro %}
