select
    feed_version,
    route_pattern_id,
    route_id,
    cast(direction_id as integer) as direction_id,
    route_pattern_name,
    route_pattern_time_desc,
    cast(route_pattern_typicality as integer) as route_pattern_typicality,
    cast(route_pattern_sort_order as integer) as route_pattern_sort_order,
    representative_trip_id,
    cast(canonical_route_pattern as integer) as canonical_route_pattern
from {{ source('static', 'route_patterns') }}
