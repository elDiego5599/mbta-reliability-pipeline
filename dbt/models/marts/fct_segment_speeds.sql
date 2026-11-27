with timepoints as (
    select
        service_date,
        trip_id,
        stop_sequence,
        stop_id,
        station_id,
        feed_version,
        route_id,
        direction_id,
        route_pattern_id,
        trip_source,
        lead(stop_sequence) over (
            partition by service_date, trip_id
            order by stop_sequence
        ) as to_stop_sequence,
        lead(stop_id) over (
            partition by service_date, trip_id
            order by stop_sequence
        ) as to_stop_id,
        lead(station_id) over (
            partition by service_date, trip_id
            order by stop_sequence
        ) as to_station_id
    from {{ ref('int_trip_paths') }}
    where checkpoint_id is not null
),

segments as (
    select
        timepoints.*,
        from_crossing.crossing_ts as from_crossing_ts,
        from_crossing.crossing_method as from_crossing_method,
        from_crossing.crossing_uncertainty_s as from_crossing_uncertainty_s,
        from_crossing.uses_clamped_report as from_uses_clamped_report,
        to_crossing.crossing_ts as to_crossing_ts,
        to_crossing.crossing_method as to_crossing_method,
        to_crossing.crossing_uncertainty_s as to_crossing_uncertainty_s,
        to_crossing.uses_clamped_report as to_uses_clamped_report
    from timepoints
    inner join {{ ref('int_stop_crossings') }} as from_crossing
        on from_crossing.service_date = timepoints.service_date
        and from_crossing.trip_id = timepoints.trip_id
        and from_crossing.stop_sequence = timepoints.stop_sequence
    inner join {{ ref('int_stop_crossings') }} as to_crossing
        on to_crossing.service_date = timepoints.service_date
        and to_crossing.trip_id = timepoints.trip_id
        and to_crossing.stop_sequence = timepoints.to_stop_sequence
    where timepoints.to_stop_sequence is not null
),

shapes as (
    select
        segments.*,
        coalesce(scheduled_trip.shape_id, representative_trip.shape_id) as shape_id
    from segments
    left join {{ ref('stg_static__trips') }} as scheduled_trip
        on scheduled_trip.feed_version = segments.feed_version
        and scheduled_trip.trip_id = segments.trip_id
        and segments.trip_source = 'scheduled'
    left join {{ ref('stg_static__route_patterns') }} as patterns
        on patterns.feed_version = segments.feed_version
        and patterns.route_pattern_id = segments.route_pattern_id
        and segments.trip_source = 'pattern_match'
    left join {{ ref('stg_static__trips') }} as representative_trip
        on representative_trip.feed_version = patterns.feed_version
        and representative_trip.trip_id = patterns.representative_trip_id
),

projected as (
    select
        shapes.*,
        from_distance.shape_dist_m as from_shape_dist_m,
        from_distance.offset_m as from_offset_m,
        to_distance.shape_dist_m as to_shape_dist_m,
        to_distance.offset_m as to_offset_m
    from shapes
    left join {{ ref('int_shape_stop_distances') }} as from_distance
        on from_distance.feed_version = shapes.feed_version
        and from_distance.shape_id = shapes.shape_id
        and from_distance.stop_id = shapes.stop_id
    left join {{ ref('int_shape_stop_distances') }} as to_distance
        on to_distance.feed_version = shapes.feed_version
        and to_distance.shape_id = shapes.shape_id
        and to_distance.stop_id = shapes.to_stop_id
),

measures as (
    select
        projected.*,
        projected.to_crossing_ts - projected.from_crossing_ts as travel_time_s,
        case
            when projected.from_shape_dist_m is null
              or projected.to_shape_dist_m is null
              or projected.from_offset_m is null
              or projected.to_offset_m is null
              or projected.from_offset_m > {{ var('max_stop_shape_offset_m') }}
              or projected.to_offset_m > {{ var('max_stop_shape_offset_m') }}
                then null
            else round(
                projected.to_shape_dist_m - projected.from_shape_dist_m,
                3
            )
        end as distance_m
    from projected
),

speeds as (
    select
        measures.*,
        case
            when measures.distance_m is null
              or measures.distance_m <= 0
              or measures.travel_time_s <= 0
                then null
            else round(
                measures.distance_m * 3.6 / measures.travel_time_s,
                3
            )
        end as speed_kmh
    from measures
),

statuses as (
    select
        speeds.*,
        case
            when coalesce(speeds.from_uses_clamped_report, false)
              or coalesce(speeds.to_uses_clamped_report, false)
                then 'clamped_report'
            when speeds.from_crossing_uncertainty_s > {{ var('max_crossing_uncertainty_s') }}
              or speeds.to_crossing_uncertainty_s > {{ var('max_crossing_uncertainty_s') }}
                then 'high_uncertainty'
            when speeds.distance_m is null
              or speeds.distance_m <= 0
                then 'no_distance'
            when speeds.travel_time_s <= 0
                then 'non_positive_time'
            when speeds.speed_kmh > {{ var('max_segment_speed_kmh') }}
                then 'implausible_speed'
            else 'valid'
        end as segment_status
    from speeds
)

select
    service_date,
    trip_id,
    route_id,
    direction_id,
    route_pattern_id,
    trip_source,
    feed_version,
    shape_id,
    stop_sequence as from_stop_sequence,
    to_stop_sequence,
    stop_id as from_stop_id,
    to_stop_id,
    station_id as from_station_id,
    to_station_id,
    from_crossing_ts,
    to_crossing_ts,
    to_timestamp(from_crossing_ts) at time zone 'America/New_York' as from_crossing_local,
    (floor(from_crossing_ts / 3600) * 3600)::bigint as hour_start_ts,
    travel_time_s,
    distance_m,
    speed_kmh,
    from_crossing_method,
    to_crossing_method,
    from_crossing_uncertainty_s,
    to_crossing_uncertainty_s,
    segment_status
from statuses
