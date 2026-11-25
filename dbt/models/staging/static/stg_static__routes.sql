select
    feed_version,
    route_id,
    agency_id,
    route_short_name,
    route_long_name,
    route_desc,
    cast(route_type as integer) as route_type,
    route_url,
    route_color,
    route_text_color,
    cast(route_sort_order as integer) as route_sort_order,
    route_fare_class,
    line_id,
    cast(listed_route as integer) as listed_route,
    network_id
from {{ source('static', 'routes') }}
