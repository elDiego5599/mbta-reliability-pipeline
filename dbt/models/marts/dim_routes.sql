with ranked_routes as (
    select
        routes.feed_version,
        routes.route_id,
        routes.route_short_name,
        routes.route_long_name,
        routes.route_desc,
        routes.route_type,
        routes.route_color,
        routes.route_text_color,
        routes.route_sort_order,
        routes.line_id,
        row_number() over (
            partition by routes.route_id
            order by versions.first_seen_at desc nulls last, routes.feed_version desc
        ) as route_rank
    from {{ ref('stg_static__routes') }} as routes
    left join {{ ref('stg_static__versions') }} as versions
        on versions.feed_version = routes.feed_version
)

select
    route_id,
    feed_version,
    route_short_name,
    route_long_name,
    route_desc,
    route_type,
    case route_type
        when 0 then 'light_rail'
        when 1 then 'subway'
        when 2 then 'commuter_rail'
        when 3 then 'bus'
        when 4 then 'ferry'
        else 'other'
    end as mode,
    route_color,
    route_text_color,
    route_sort_order,
    line_id,
    exists (
        select 1
        from {{ in_scope_routes() }} as scoped_routes
        where scoped_routes.feed_version = ranked_routes.feed_version
          and scoped_routes.route_id = ranked_routes.route_id
    ) as is_in_scope
from ranked_routes
where route_rank = 1
