with ranked_stops as (
    select
        stops.feed_version,
        stops.stop_id,
        stops.stop_name,
        stops.parent_station,
        stops.location_type,
        stops.platform_name,
        stops.stop_lat,
        stops.stop_lon,
        stops.municipality,
        row_number() over (
            partition by stops.stop_id
            order by versions.first_seen_at desc, stops.feed_version desc
        ) as stop_rank
    from {{ ref('stg_static__stops') }} as stops
    inner join {{ ref('stg_static__versions') }} as versions
        on versions.feed_version = stops.feed_version
)

select
    stop_id,
    feed_version,
    stop_name,
    coalesce(parent_station, stop_id) as station_id,
    parent_station,
    location_type,
    platform_name,
    stop_lat,
    stop_lon,
    municipality
from ranked_stops
where stop_rank = 1
