with mart_dates as (
    select distinct service_date
    from {{ ref('fct_headways') }}
    union
    select distinct service_date
    from {{ ref('fct_schedule_adherence') }}
    union
    select distinct service_date
    from {{ ref('agg_route_hour_reliability') }}
)

select mart_dates.service_date
from mart_dates
where not exists (
    select 1
    from {{ ref('int_static_active_version') }} as active
    where active.service_date = mart_dates.service_date
)
