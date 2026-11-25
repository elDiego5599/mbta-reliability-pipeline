select
    feed_version,
    service_id,
    strptime(date, '%Y%m%d')::date as date,
    cast(exception_type as integer) as exception_type,
    holiday_name
from {{ source('static', 'calendar_dates') }}
