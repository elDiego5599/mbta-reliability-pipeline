with measure_crossings as (
    select
        crossings.service_date,
        crossings.trip_id,
        crossings.stop_sequence,
        crossings.stop_id,
        crossings.station_id,
        active.feed_version,
        crossings.route_id,
        crossings.direction_id,
        crossings.route_pattern_id,
        crossings.trip_source,
        crossings.crossing_ts,
        crossings.crossing_method,
        crossings.crossing_uncertainty_s,
        crossings.uses_clamped_report
    from {{ ref('int_stop_crossings') }} as crossings
    inner join {{ ref('int_static_active_version') }} as active
        on active.service_date = crossings.service_date
    inner join {{ ref('int_measure_points') }} as measure
        on measure.feed_version = active.feed_version
        and measure.route_id = crossings.route_id
        and measure.direction_id = crossings.direction_id
        and measure.station_id = crossings.station_id
),

ordered as (
    select
        measure_crossings.*,
        lag(trip_id) over (
            partition by service_date, route_id, direction_id
            order by crossing_ts, trip_id
        ) as prev_trip_id,
        lag(route_pattern_id) over (
            partition by service_date, route_id, direction_id
            order by crossing_ts, trip_id
        ) as prev_route_pattern_id,
        lag(crossing_ts) over (
            partition by service_date, route_id, direction_id
            order by crossing_ts, trip_id
        ) as prev_crossing_ts,
        lag(trip_source) over (
            partition by service_date, route_id, direction_id
            order by crossing_ts, trip_id
        ) as prev_trip_source,
        lag(crossing_method) over (
            partition by service_date, route_id, direction_id
            order by crossing_ts, trip_id
        ) as prev_crossing_method,
        lag(crossing_uncertainty_s) over (
            partition by service_date, route_id, direction_id
            order by crossing_ts, trip_id
        ) as prev_crossing_uncertainty_s,
        lag(uses_clamped_report) over (
            partition by service_date, route_id, direction_id
            order by crossing_ts, trip_id
        ) as prev_uses_clamped_report
    from measure_crossings
),

headways as (
    select
        service_date,
        route_id,
        direction_id,
        feed_version,
        station_id,
        stop_id,
        stop_sequence,
        trip_id,
        prev_trip_id,
        route_pattern_id,
        prev_route_pattern_id,
        trip_source,
        crossing_ts,
        prev_crossing_ts,
        to_timestamp(crossing_ts) at time zone 'America/New_York' as crossing_local,
        (floor(crossing_ts / 3600) * 3600)::bigint as hour_start_ts,
        crossing_ts - prev_crossing_ts as headway_s,
        crossing_method,
        prev_crossing_method,
        crossing_uncertainty_s,
        prev_crossing_uncertainty_s,
        crossing_uncertainty_s + prev_crossing_uncertainty_s as headway_uncertainty_s,
        case
            when coalesce(uses_clamped_report, false)
              or coalesce(prev_uses_clamped_report, false)
                then 'clamped_report'
            when crossing_uncertainty_s > {{ var('max_crossing_uncertainty_s') }}
              or prev_crossing_uncertainty_s > {{ var('max_crossing_uncertainty_s') }}
                then 'high_uncertainty'
            else 'valid'
        end as headway_status
    from ordered
    where prev_crossing_ts is not null
)

select *
from headways
