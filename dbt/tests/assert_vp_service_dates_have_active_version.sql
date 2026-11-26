-- Warn when a vehicle-position service date has no active static version at
-- all. A route counts as in scope when it is in scope in any static version;
-- whether the active version holds the route is not checked here (such trips
-- simply get no path in int_trip_paths).
{{ config(severity = 'warn') }}

with vp_dates as (
    select distinct
        strptime(trip__start_date, '%Y%m%d')::date as service_date,
        trip__route_id as route_id
    from {{ ref('stg_vehicle_positions') }}
    where trip__start_date is not null
),

in_scope as (
    select distinct route_id
    from {{ in_scope_routes() }}
)

select distinct
    vp_dates.service_date
from vp_dates
inner join in_scope
    on in_scope.route_id = vp_dates.route_id
where not exists (
    select 1
    from {{ ref('int_static_active_version') }} as active
    where active.service_date = vp_dates.service_date
)
