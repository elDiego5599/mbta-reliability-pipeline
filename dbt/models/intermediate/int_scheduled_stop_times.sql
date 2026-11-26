-- One scheduled timepoint stop time on a service date: grain and key
-- (service_date, trip_id, stop_sequence).

with observed_dates as (
    select distinct
        strptime(trip__start_date, '%Y%m%d')::date as service_date
    from {{ ref('stg_vehicle_positions') }}
    where trip__start_date is not null
),

date_bounds as (
    select
        min(service_date) as first_date,
        max(service_date) as last_date
    from observed_dates
),

service_dates as (
    select
        cast(unnest(generate_series(first_date, last_date, interval 1 day)) as date) as service_date
    from date_bounds
),

active_dates as (
    select
        service_dates.service_date,
        active.feed_version
    from service_dates
    inner join {{ ref('int_static_active_version') }} as active
        on active.service_date = service_dates.service_date
),

trip_dates as (
    select
        active_dates.service_date,
        active_dates.feed_version,
        trips.trip_id,
        trips.route_id,
        trips.direction_id,
        trips.route_pattern_id,
        trips.service_id
    from active_dates
    inner join {{ ref('stg_static__trips') }} as trips
        on trips.feed_version = active_dates.feed_version
    inner join {{ in_scope_routes() }} as routes
        on routes.feed_version = trips.feed_version
        and routes.route_id = trips.route_id
),

calendar_active as (
    select
        trip_dates.*,
        calendar.start_date,
        calendar.end_date,
        calendar.monday,
        calendar.tuesday,
        calendar.wednesday,
        calendar.thursday,
        calendar.friday,
        calendar.saturday,
        calendar.sunday
    from trip_dates
    inner join {{ ref('stg_static__calendar') }} as calendar
        on calendar.feed_version = trip_dates.feed_version
        and calendar.service_id = trip_dates.service_id
),

calendar_candidates as (
    select
        *
    from calendar_active
    where service_date between start_date and end_date
        and (
            (extract(dow from service_date) = 1 and monday)
            or (extract(dow from service_date) = 2 and tuesday)
            or (extract(dow from service_date) = 3 and wednesday)
            or (extract(dow from service_date) = 4 and thursday)
            or (extract(dow from service_date) = 5 and friday)
            or (extract(dow from service_date) = 6 and saturday)
            or (extract(dow from service_date) = 0 and sunday)
        )
),

removed_dates as (
    select
        exceptions.feed_version,
        exceptions.service_id,
        exceptions.date as service_date
    from {{ ref('stg_static__calendar_dates') }} as exceptions
    where exceptions.exception_type = 2
),

added_dates as (
    select
        exceptions.feed_version,
        exceptions.service_id,
        exceptions.date as service_date
    from {{ ref('stg_static__calendar_dates') }} as exceptions
    where exceptions.exception_type = 1
),

runs as (
    select
        calendar_candidates.service_date,
        calendar_candidates.feed_version,
        calendar_candidates.trip_id,
        calendar_candidates.route_id,
        calendar_candidates.direction_id,
        calendar_candidates.route_pattern_id,
        calendar_candidates.service_id
    from calendar_candidates
    where not exists (
        select 1
        from removed_dates
        where removed_dates.feed_version = calendar_candidates.feed_version
            and removed_dates.service_id = calendar_candidates.service_id
            and removed_dates.service_date = calendar_candidates.service_date
    )

    union

    select
        trip_dates.service_date,
        trip_dates.feed_version,
        trip_dates.trip_id,
        trip_dates.route_id,
        trip_dates.direction_id,
        trip_dates.route_pattern_id,
        trip_dates.service_id
    from trip_dates
    inner join added_dates
        on added_dates.feed_version = trip_dates.feed_version
        and added_dates.service_id = trip_dates.service_id
        and added_dates.service_date = trip_dates.service_date
),

scheduled as (
    select
        runs.service_date,
        runs.feed_version,
        runs.trip_id,
        runs.route_id,
        runs.direction_id,
        runs.route_pattern_id,
        runs.service_id,
        stop_times.stop_sequence,
        stop_times.stop_id,
        stops.parent_station,
        stop_times.checkpoint_id,
        stop_times.arrival_time_s,
        stop_times.departure_time_s
    from runs
    inner join {{ ref('stg_static__stop_times') }} as stop_times
        on stop_times.feed_version = runs.feed_version
        and stop_times.trip_id = runs.trip_id
        and stop_times.checkpoint_id is not null
    inner join {{ ref('stg_static__stops') }} as stops
        on stops.feed_version = runs.feed_version
        and stops.stop_id = stop_times.stop_id
)

select
    scheduled.service_date,
    scheduled.feed_version,
    scheduled.trip_id,
    scheduled.route_id,
    scheduled.direction_id,
    scheduled.route_pattern_id,
    scheduled.service_id,
    scheduled.stop_sequence,
    scheduled.stop_id,
    coalesce(scheduled.parent_station, scheduled.stop_id) as station_id,
    scheduled.checkpoint_id,
    scheduled.arrival_time_s,
    scheduled.departure_time_s,
    epoch(
        timezone(
            'America/New_York',
            scheduled.service_date + interval 12 hour
        )
    ) - 43200 + scheduled.arrival_time_s as scheduled_arrival_ts,
    epoch(
        timezone(
            'America/New_York',
            scheduled.service_date + interval 12 hour
        )
    ) - 43200 + scheduled.departure_time_s as scheduled_departure_ts
from scheduled
