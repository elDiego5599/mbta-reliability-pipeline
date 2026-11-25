select
    feed_version,
    trip_id,
    arrival_time,
    {{ gtfs_time_to_seconds('arrival_time') }} as arrival_time_s,
    departure_time,
    {{ gtfs_time_to_seconds('departure_time') }} as departure_time_s,
    stop_id,
    cast(stop_sequence as integer) as stop_sequence,
    stop_headsign,
    cast(pickup_type as integer) as pickup_type,
    cast(drop_off_type as integer) as drop_off_type,
    cast(timepoint as integer) as timepoint,
    checkpoint_id,
    cast(continuous_pickup as integer) as continuous_pickup,
    cast(continuous_drop_off as integer) as continuous_drop_off
from {{ source('static', 'stop_times') }}
