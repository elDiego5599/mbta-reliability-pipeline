select
    feed_version,
    service_id,
    cast(monday as integer) = 1 as monday,
    cast(tuesday as integer) = 1 as tuesday,
    cast(wednesday as integer) = 1 as wednesday,
    cast(thursday as integer) = 1 as thursday,
    cast(friday as integer) = 1 as friday,
    cast(saturday as integer) = 1 as saturday,
    cast(sunday as integer) = 1 as sunday,
    strptime(start_date, '%Y%m%d')::date as start_date,
    strptime(end_date, '%Y%m%d')::date as end_date
from {{ source('static', 'calendar') }}
