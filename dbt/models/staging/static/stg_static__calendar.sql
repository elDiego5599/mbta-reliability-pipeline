select
    feed_version,
    service_id,
    {{ gtfs_bool('monday') }} as monday,
    {{ gtfs_bool('tuesday') }} as tuesday,
    {{ gtfs_bool('wednesday') }} as wednesday,
    {{ gtfs_bool('thursday') }} as thursday,
    {{ gtfs_bool('friday') }} as friday,
    {{ gtfs_bool('saturday') }} as saturday,
    {{ gtfs_bool('sunday') }} as sunday,
    strptime(start_date, '%Y%m%d')::date as start_date,
    strptime(end_date, '%Y%m%d')::date as end_date
from {{ source('static', 'calendar') }}
