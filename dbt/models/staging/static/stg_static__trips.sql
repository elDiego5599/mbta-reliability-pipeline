select
    feed_version,
    route_id,
    service_id,
    trip_id,
    trip_headsign,
    trip_short_name,
    cast(direction_id as integer) as direction_id,
    block_id,
    shape_id,
    cast(wheelchair_accessible as integer) as wheelchair_accessible,
    cast(trip_route_type as integer) as trip_route_type,
    route_pattern_id,
    cast(bikes_allowed as integer) as bikes_allowed
from {{ source('static', 'trips') }}
